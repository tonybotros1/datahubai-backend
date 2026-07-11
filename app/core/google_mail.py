import base64
import hashlib
import json
import os
from typing import Any, Optional
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from cryptography.fernet import Fernet, InvalidToken


GOOGLE_AUTHORIZATION_URL = "https://accounts.google.com/o/oauth2/v2/auth"
GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
GOOGLE_USERINFO_URL = "https://openidconnect.googleapis.com/v1/userinfo"
GOOGLE_GMAIL_SEND_URL = "https://gmail.googleapis.com/gmail/v1/users/me/messages/send"
GOOGLE_REVOKE_URL = "https://oauth2.googleapis.com/revoke"
GOOGLE_MAIL_SCOPES = (
    "openid email https://www.googleapis.com/auth/gmail.send"
)


class GoogleMailError(Exception):
    def __init__(self, message: str, status_code: int = 502):
        super().__init__(message)
        self.message = message
        self.status_code = status_code


def oauth_configuration() -> dict:
    client_id = (os.getenv("GOOGLE_OAUTH_CLIENT_ID") or "").strip()
    client_secret = (os.getenv("GOOGLE_OAUTH_CLIENT_SECRET") or "").strip()
    redirect_uri = (os.getenv("GOOGLE_OAUTH_REDIRECT_URI") or "").strip()
    if not client_id or not client_secret or not redirect_uri:
        raise GoogleMailError(
            "Google Mail OAuth is not configured. Set GOOGLE_OAUTH_CLIENT_ID, "
            "GOOGLE_OAUTH_CLIENT_SECRET and GOOGLE_OAUTH_REDIRECT_URI.",
            503,
        )
    return {
        "client_id": client_id,
        "client_secret": client_secret,
        "redirect_uri": redirect_uri,
    }


def _token_cipher() -> Fernet:
    secret = (
        os.getenv("MAIL_TOKEN_ENCRYPTION_KEY")
        or os.getenv("ACCESS_SECRET_KEY")
        or ""
    ).strip()
    if not secret:
        raise GoogleMailError(
            "Mail token encryption is not configured. "
            "Set MAIL_TOKEN_ENCRYPTION_KEY.",
            503,
        )
    derived_key = base64.urlsafe_b64encode(
        hashlib.sha256(secret.encode("utf-8")).digest()
    )
    return Fernet(derived_key)


def encrypt_refresh_token(refresh_token: str) -> str:
    return _token_cipher().encrypt(refresh_token.encode("utf-8")).decode("utf-8")


def decrypt_refresh_token(encrypted_token: str) -> str:
    try:
        return _token_cipher().decrypt(
            encrypted_token.encode("utf-8")
        ).decode("utf-8")
    except (InvalidToken, ValueError, TypeError):
        raise GoogleMailError(
            "The connected Google Mail token cannot be decrypted. "
            "Please reconnect Google Mail.",
            409,
        )


def authorization_url(state: str) -> str:
    config = oauth_configuration()
    params = {
        "client_id": config["client_id"],
        "redirect_uri": config["redirect_uri"],
        "response_type": "code",
        "scope": GOOGLE_MAIL_SCOPES,
        "access_type": "offline",
        "prompt": "consent",
        "include_granted_scopes": "true",
        "state": state,
    }
    return f"{GOOGLE_AUTHORIZATION_URL}?{urlencode(params)}"


def _request_json(
        url: str,
        *,
        method: str = "GET",
        headers: Optional[dict] = None,
        form: Optional[dict] = None,
        payload: Optional[dict] = None,
) -> dict:
    request_headers = dict(headers or {})
    data = None
    if form is not None:
        data = urlencode(form).encode("utf-8")
        request_headers["Content-Type"] = "application/x-www-form-urlencoded"
    elif payload is not None:
        data = json.dumps(payload).encode("utf-8")
        request_headers["Content-Type"] = "application/json"

    request = Request(
        url,
        data=data,
        headers=request_headers,
        method=method,
    )
    try:
        with urlopen(request, timeout=30) as response:
            response_body = response.read().decode("utf-8")
            return json.loads(response_body) if response_body else {}
    except HTTPError as error:
        raw_error = error.read().decode("utf-8", errors="replace")
        message = raw_error
        try:
            decoded_error: Any = json.loads(raw_error)
            message = (
                decoded_error.get("error_description")
                or (decoded_error.get("error") or {}).get("message")
                or decoded_error.get("error")
                or raw_error
            )
        except (json.JSONDecodeError, AttributeError):
            pass
        raise GoogleMailError(str(message)[:300], error.code)
    except URLError as error:
        raise GoogleMailError(f"Could not reach Google: {error.reason}")


def exchange_authorization_code(code: str) -> dict:
    config = oauth_configuration()
    return _request_json(
        GOOGLE_TOKEN_URL,
        method="POST",
        form={
            "code": code,
            "client_id": config["client_id"],
            "client_secret": config["client_secret"],
            "redirect_uri": config["redirect_uri"],
            "grant_type": "authorization_code",
        },
    )


def refresh_access_token(refresh_token: str) -> str:
    config = oauth_configuration()
    token_response = _request_json(
        GOOGLE_TOKEN_URL,
        method="POST",
        form={
            "refresh_token": refresh_token,
            "client_id": config["client_id"],
            "client_secret": config["client_secret"],
            "grant_type": "refresh_token",
        },
    )
    access_token = token_response.get("access_token")
    if not access_token:
        raise GoogleMailError(
            "Google did not return an access token. Please reconnect Google Mail."
        )
    return str(access_token)


def connected_google_email(access_token: str) -> str:
    response = _request_json(
        GOOGLE_USERINFO_URL,
        headers={"Authorization": f"Bearer {access_token}"},
    )
    email = str(response.get("email") or "").strip().lower()
    if not email or response.get("email_verified") is False:
        raise GoogleMailError("Google did not return a verified email address.")
    return email


def send_raw_message(access_token: str, raw_message: str) -> dict:
    return _request_json(
        GOOGLE_GMAIL_SEND_URL,
        method="POST",
        headers={"Authorization": f"Bearer {access_token}"},
        payload={"raw": raw_message},
    )


def revoke_refresh_token(refresh_token: str) -> None:
    _request_json(
        GOOGLE_REVOKE_URL,
        method="POST",
        form={"token": refresh_token},
    )

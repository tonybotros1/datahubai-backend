import os, hashlib, uuid
from datetime import datetime, timedelta, timezone
from bson import ObjectId
from fastapi import Header, HTTPException, status
from jose import jwt, JWTError
from passlib.context import CryptContext

from app.database import get_collection

ACCESS_SECRET = str(os.getenv("ACCESS_SECRET_KEY"))
REFRESH_SECRET = str(os.getenv("REFRESH_SECRET_KEY"))
ALGORITHM = "HS256"
ACCESS_TTL_MIN = int(os.getenv("ACCESS_TTL_MIN", "60"))
REFRESH_TTL_DAYS = int(os.getenv("REFRESH_TTL_DAYS", "60"))
pwd_ctx = CryptContext(schemes=["argon2"], deprecated="auto")
users_collection = get_collection("sys-users")


def now_utc():
    return datetime.now(timezone.utc) + timedelta(hours=4)


def one_month_from_now_utc():
    return datetime.now(timezone.utc) + timedelta(days=30)


def new_jti():
    return str(uuid.uuid4())


def hash_sha256(raw: str) -> str:
    return hashlib.sha256(raw.encode()).hexdigest()


# token creators (include company_id + role)
def create_access_token(
        user_id: str,
        company_id: str,
        role: list[str],
        session_version: int = 0,
        session_id: str = "",
):
    jti = new_jti()
    iat = int(now_utc().timestamp())
    exp = int((now_utc() + timedelta(minutes=ACCESS_TTL_MIN)).timestamp())
    payload = {
        "sub": str(user_id),
        "jti": jti,
        "type": "access",
        "company_id": str(company_id),
        "role": role,
        "session_version": session_version,
        "session_id": session_id,
        "iat": iat,
        "exp": exp
    }
    token = jwt.encode(payload, ACCESS_SECRET, algorithm=ALGORITHM)
    return token, jti, exp - iat


def create_refresh_token(user_id: str, company_id: str):
    jti = new_jti()
    exp_dt = now_utc() + timedelta(days=REFRESH_TTL_DAYS)
    payload = {
        "sub": str(user_id),
        "jti": jti,
        "type": "refresh",
        "company_id": str(company_id),
        "iat": int(now_utc().timestamp()),
        "exp": int(exp_dt.timestamp())
    }
    raw = jwt.encode(payload, REFRESH_SECRET, algorithm=ALGORITHM)
    token_hash = hash_sha256(raw)
    return raw, token_hash, exp_dt, jti


def decode_refresh_token(raw_token: str):
    try:
        return jwt.decode(raw_token, REFRESH_SECRET, algorithms=[ALGORITHM])
    except JWTError:
        return None


def decode_access_token(raw: str):
    try:
        return jwt.decode(raw, ACCESS_SECRET, algorithms=[ALGORITHM])
    except JWTError:
        return None


def verify_password(plain, hashed):
    return pwd_ctx.verify(plain, hashed)


def get_password_hash(password):
    return pwd_ctx.hash(password)


async def get_current_user(authorization: str = Header(...)):
    if not authorization.startswith("Bearer "):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid authorization header")

    token = authorization.split(" ")[1]

    # Use your existing decode method
    payload = decode_access_token(token)

    if payload is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid or expired token")

    if payload.get("type") != "access":
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token type")

    try:
        user_id = ObjectId(str(payload.get("sub", "")))
        company_id = ObjectId(str(payload.get("company_id", "")))
    except Exception:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token identity")

    user = await users_collection.find_one(
        {"_id": user_id, "company_id": company_id},
        {
            "status": 1,
            "session_version": 1,
            "revoked_session_ids": 1,
            "revoked_access_jtis": 1,
        },
    )
    if not user or not user.get("status", False):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User session is inactive")

    token_version = int(payload.get("session_version", 0) or 0)
    current_version = int(user.get("session_version", 0) or 0)
    if token_version != current_version:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User session was revoked")

    session_id = str(payload.get("session_id", "") or "")
    if session_id and session_id in user.get("revoked_session_ids", []):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Device session was revoked")

    access_jti = str(payload.get("jti", "") or "")
    if access_jti and access_jti in user.get("revoked_access_jtis", []):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Device access was revoked")

    return payload

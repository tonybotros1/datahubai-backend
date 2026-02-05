import os, hashlib, uuid
from datetime import datetime, timedelta, timezone
from fastapi import Header, HTTPException, status
from jose import jwt, JWTError
from passlib.context import CryptContext

ACCESS_SECRET = str(os.getenv("ACCESS_SECRET_KEY"))
REFRESH_SECRET = str(os.getenv("REFRESH_SECRET_KEY"))
ALGORITHM = "HS256"
ACCESS_TTL_MIN = int(os.getenv("ACCESS_TTL_MIN", "60"))
REFRESH_TTL_DAYS = int(os.getenv("REFRESH_TTL_DAYS", "60"))
pwd_ctx = CryptContext(schemes=["argon2"], deprecated="auto")


def now_utc():
    # return datetime.now(timezone.utc)
    return datetime.now().astimezone()

def one_month_from_now_utc():
    return datetime.now(timezone.utc) + timedelta(days=30)


def new_jti():
    return str(uuid.uuid4())


def hash_sha256(raw: str) -> str:
    return hashlib.sha256(raw.encode()).hexdigest()


# token creators (include company_id + role)
def create_access_token(user_id: str, company_id: str, role: list[str]):
    jti = new_jti()
    iat = int(now_utc().timestamp())
    exp = int((now_utc() + timedelta(minutes=ACCESS_TTL_MIN)).timestamp())
    payload = {
        "sub": str(user_id),
        "jti": jti,
        "type": "access",
        "company_id": str(company_id),
        "role": role,
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

    return payload

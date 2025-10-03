from bson import ObjectId
from fastapi import APIRouter, HTTPException, Form
from app.database import get_collection
from app.core import security
from app.widgets.check_date import is_date_equals_today_or_older
import jwt

router = APIRouter()

users = get_collection("sys-users")
companies = get_collection("companies")
refresh_tokens = get_collection("refresh_tokens")



@router.post("/login")
async def login(
        email: str = Form(...),
        password: str = Form(...)
):
    user = await users.find_one({"email": email})
    if not user:
        raise HTTPException(status_code=401, detail="Invalid email or password")

    if not security.verify_password(password, user["password_hash"]):
        raise HTTPException(status_code=401, detail="Invalid email or password")

    company_id = user.get("company_id")
    company = await companies.find_one({"_id": company_id})
    if not company:
        raise HTTPException(status_code=404, detail="Company not found")

    company_status = company.get("status", False)
    if not company_status:
        raise HTTPException(status_code=403, detail="Your session has been expired")

    user_expiry_date = user.get("expiry_date", security.now_utc())
    user_active = user.get("status", False)
    if not user_active or is_date_equals_today_or_older(user_expiry_date):
        raise HTTPException(status_code=403, detail="Your session has been expired")

    roles = user.get("roles", [])

    roles = [str(r) for r in roles]

    access_token, access_jti, access_expires_in = security.create_access_token(
        str(user["_id"]), str(company_id), roles
    )
    refresh_token, refresh_token_hash, refresh_exp, refresh_jti = security.create_refresh_token(
        str(user["_id"]), str(company_id)
    )

    await refresh_tokens.insert_one({
        "user_id": ObjectId(user["_id"]),
        "jti": refresh_jti,
        "token_hash": refresh_token_hash,
        "expires_at": refresh_exp
    })

    return {
        "user_id": str(user["_id"]),
        "email": user["email"],
        "company_id": str(company_id),
        "role": roles,
        "access_token": access_token,
        "expires_in": access_expires_in,
        "refresh_token": refresh_token,
        "token_type": "bearer"
    }


@router.post("/logout")
async def logout(refresh_token: str = Form(...)):
    token_hash = security.hash_sha256(refresh_token)
    await refresh_tokens.delete_one({"token_hash": token_hash})
    return {"message": "Logged out successfully from this device"}


@router.get("/is_user_valid/{user_id}")
async def is_user_valid(user_id: str):
    try:
        user = await users.find_one({"_id": ObjectId(user_id)})
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        expiry_date = user.get("expiry_date", security.now_utc())
        return {"valid": not is_date_equals_today_or_older(expiry_date)}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Network/Database error: {str(e)}")


@router.post("/refresh_token")
async def refresh_token_method(token: str = Form(...)):
    try:
        payload = security.decode_refresh_token(token)
        if payload.get("type") != "refresh":
            raise HTTPException(status_code=401, detail="Invalid token type")

        token_doc = await refresh_tokens.find_one({
            "jti": payload["jti"],
            "token_hash": security.hash_sha256(token),
            "user_id": ObjectId(payload["sub"])
        })
        if not token_doc:
            raise HTTPException(status_code=401, detail="Refresh token invalid or revoked")

        # Create new access token
        user_id = payload["sub"]
        company_id = payload["company_id"]
        roles = payload.get("role", [])  # optional, may store in DB for more security
        access_token, access_jti, expires_in = security.create_access_token(user_id, company_id, roles)

        return {
            "access_token": access_token,
            "expires_in": expires_in,
        }

    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Refresh token expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid refresh token")

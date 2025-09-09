from typing import List

from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException, Request, status, Form, UploadFile, File, Body
from pymongo.errors import DuplicateKeyError

from app.database import get_collection
from datetime import timezone
from app import database
from app.core import security
from app.widgets import upload_images

router = APIRouter()

users = get_collection("sys-users")
companies = get_collection("companies")
refresh_tokens = get_collection("refresh_tokens")


@router.post("/register_company")
async def register_company(
        company_name: str = Form(...),
        admin_email: str = Form(...),
        admin_password: str = Form(...),
        industry: str = Form(...),
        company_logo: UploadFile = File(None),
        roles_ids: List[str] = Form(...),
        admin_name: str = Form(...),
        phone_number: str = Form(...),
        address: str = Form(...),
        country: str = Form(...),
        city: str = Form(...)
):
    async with database.client.start_session() as s:
        try:
            # 👇 ابدأ الترانزاكشن
            await s.start_transaction()

            # معالجة صورة الشركة
            company_logo_url = ""
            company_logo_public_id = ""
            if company_logo:
                result = await upload_images.upload_image(company_logo, 'companies')
                company_logo_url = result["url"]
                company_logo_public_id = result["public_id"]

            role_ids_list = [ObjectId(r.strip()) for r in roles_ids]

            # 1. إنشاء الشركة
            company_doc = {
                "company_name": company_name,
                "owner_id": None,
                "status": True,
                "createdAt": security.now_utc(),
                "updatedAt": security.now_utc(),
                "industry": ObjectId(industry),
                "company_logo_url": company_logo_url,
                "company_logo_public_id": company_logo_public_id,
            }
            res_company = await companies.insert_one(company_doc, session=s)

            # 2. إنشاء المستخدم (الـ owner)
            owner_doc = {
                "company_id": res_company.inserted_id,
                "email": admin_email,
                "user_name": admin_name,
                "password_hash": security.pwd_ctx.hash(admin_password),
                "roles": role_ids_list,
                "status": True,
                "expiryDate": security.one_month_from_now_utc(),
                "createdAt": security.now_utc(),
                "updatedAt": security.now_utc(),
                "phone_number": phone_number,
                "address": address,
                "country": ObjectId(country),
                "city": ObjectId(city),
            }
            res_owner = await users.insert_one(owner_doc, session=s)

            # 3. تحديث الشركة وربطها بالـ owner
            await companies.update_one(
                {"_id": res_company.inserted_id},
                {"$set": {"owner_id": res_owner.inserted_id}},
                session=s
            )

            # 👇 إذا كلشي مشي تمام
            await s.commit_transaction()

            return {
                "company_id": str(res_company.inserted_id),
                "owner_id": str(res_owner.inserted_id),
                "message": "Company and owner registered successfully"
            }

        except DuplicateKeyError as e:
            await s.abort_transaction()
            if "company_name" in str(e):
                raise HTTPException(status_code=400, detail="Company name already exists")
            elif "email" in str(e):
                raise HTTPException(status_code=400, detail="Email already exists")
            else:
                raise HTTPException(status_code=400, detail="Duplicate entry")

        except Exception as e:
            # 👇 rollback إذا صار خطأ
            await s.abort_transaction()
            raise HTTPException(status_code=500, detail=f"Registration failed: {str(e)}")


@router.post("/login")
async def login(
        email: str = Form(...),
        password: str = Form(...)
):
    user = await users.find_one({"email": email})
    if not user:
        raise HTTPException(status_code=401, detail="Invalid email or password")

    if not security.pwd_ctx.verify(password, user["password_hash"]):
        raise HTTPException(status_code=401, detail="Invalid email or password")

    company_id = user.get("company_id")
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
    """
    Logout endpoint: يحذف refresh token الحالي فقط
    """
    # 1. احسب hash للـ refresh token
    token_hash = security.hash_sha256(refresh_token)

    # 2. حذف الـ refresh token من DB
    result = await refresh_tokens.delete_one({"token_hash": token_hash})

    if result.deleted_count == 0:
        # إذا ما لاقى token → ممكن يكون انتهى أو غير صالح
        raise HTTPException(status_code=400, detail="Invalid refresh token")

    return {"message": "Logged out successfully from this device"}

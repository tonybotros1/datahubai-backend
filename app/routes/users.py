from typing import Optional, List
from bson import ObjectId
from fastapi import APIRouter, Body, HTTPException, Depends
from pydantic import BaseModel, EmailStr
from pymongo.errors import DuplicateKeyError

from app import database
from app.core import security
from app.database import get_collection
from datetime import datetime, timezone
from app.websocket_config import manager

router = APIRouter()
users_collection = get_collection("sys-users")
refresh_tokens_collection = get_collection("refresh_tokens")


def serializer(user: dict) -> dict:
    user["_id"] = str(user["_id"])
    user["roles"] = [str(r) for r in user.get("roles", [])]
    for key, value in user.items():
        if isinstance(value, datetime):
            user[key] = value.isoformat()
    return user


class UserCreate(BaseModel):
    user_name: Optional[str]
    email: EmailStr
    password: Optional[str]
    roles_ids: Optional[List[str]]
    expiry_date: Optional[datetime]


@router.get("/get_all_users")
async def get_all_users(data: dict = Depends(security.get_current_user)):
    try:
        all_users = await users_collection.find({"company_id": ObjectId(data.get("company_id"))}, {
            "_id": 1,
            "user_name": 1,
            "email": 1,
            "roles": 1,
            "status": 1,
            "expiry_date": 1,
            "createdAt": 1,
            "updatedAt": 1}).to_list(None)

        return {"users": [serializer(u) for u in all_users]}

    except Exception as e:
        return {"message": str(e)}


@router.post("/add_new_user")
async def add_new_user(user: UserCreate, data: dict = Depends(security.get_current_user)):
    company_id = ObjectId(data.get("company_id"))

    try:
        existing_user = await users_collection.find_one(
            {"company_id": company_id, "email": user.email},
        )
        if existing_user:
            raise HTTPException(status_code=400, detail="Email already exists")

        new_user = {
            "company_id": company_id,
            "user_name": user.user_name,
            "email": user.email,
            "password_hash": security.pwd_ctx.hash(user.password),
            "roles": user.roles_ids or [],
            "expiry_date": user.expiry_date,
            "status": True,
            "createdAt": security.now_utc(),
            "updatedAt": security.now_utc(),
        }

        result = await users_collection.insert_one(new_user)

        new_user.pop("password_hash", None)
        new_user.pop("company_id", None)
        new_user["_id"] = str(result.inserted_id)
        new_user = serializer(new_user)
        await manager.broadcast({
            "type": "user_added",
            "data": new_user
        })
        return {
            "success": True,
            "message": "User created successfully",
            "user": new_user
        }


    except HTTPException as e:  # let FastAPI handle HTTP errors
        raise e

    except Exception as error:  # other unexpected errors
        raise HTTPException(status_code=500, detail="Internal server error")


@router.delete("/remove_user/{user_id}")
async def remove_user(user_id: str, _: dict = Depends(security.get_current_user)):
    try:
        result = await users_collection.delete_one({"_id": ObjectId(user_id)})

        if result.deleted_count == 0:
            raise HTTPException(status_code=404, detail="Role not found")
        await refresh_tokens_collection.delete_many({"user_id": ObjectId(user_id)})

        await manager.broadcast({
            "type": "user_deleted",
            "data": {"_id": user_id}
        })
        return {"message": "Role deleted successfully", "role_id": user_id}

    except Exception as e:
        raise e

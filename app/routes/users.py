from typing import Optional, List
from bson import ObjectId
from fastapi import APIRouter, Body, HTTPException, Depends
from pydantic import BaseModel, EmailStr
from pymongo import ReturnDocument
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
    email: Optional[EmailStr]
    password: Optional[str]
    roles: Optional[List[str]]
    expiry_date: Optional[datetime]


# For updating a user (all fields optional)
class UserUpdate(BaseModel):
    user_name: Optional[str] = None
    email: Optional[EmailStr] = None
    password: Optional[str] = None
    roles: Optional[List[str]] = None
    expiry_date: Optional[datetime] = None


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
            "roles": user.roles or [],
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


@router.patch("/update_user/{user_id}")
async def update_user(user_id: str, user: UserUpdate, _: dict = Depends(security.get_current_user)):
    try:
        user_data = user.model_dump(exclude_unset=True)

        if "password" in user_data:
            hashed = security.pwd_ctx.hash(user_data.pop("password"))
            user_data["password_hash"] = hashed

        user_data["updatedAt"] = security.now_utc()

        result = await users_collection.find_one_and_update(
            {"_id": ObjectId(user_id)},
            {"$set": user_data},
            projection={
                "_id": 1,
                "user_name": 1,
                "email": 1,
                "roles": 1,
                "status": 1,
                "expiry_date": 1,
                "createdAt": 1,
                "updatedAt": 1,
            },
            return_document=ReturnDocument.AFTER
        )

        if not result:
            raise HTTPException(status_code=404, detail="User not found")

        updated_user = serializer(result)

        await manager.broadcast({
            "type": "user_updated",
            "data": updated_user
        })

        return updated_user

    except Exception as e:
        raise e


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


@router.patch("/change_user_status/{user_id}")
async def change_user_status(user_id: str, status: bool = Body(None), _: dict = Depends(security.get_current_user)):
    try:
        result = await users_collection.find_one_and_update(
            {"_id": ObjectId(user_id)}, {"$set": {"status": status, "updatedAt": datetime.now(timezone.utc), }},
            return_document=ReturnDocument.AFTER
        )
        if not result:
            raise HTTPException(status_code=404, detail="User not found")
        await manager.broadcast({
            "type": "user_status_updated",
            "data": {"status": status, "_id": user_id}
        })
    except Exception as error:
        return {"message": str(error)}

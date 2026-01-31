from typing import Optional, List
from fastapi import APIRouter, Body, Depends
from pydantic import BaseModel, EmailStr
from pymongo import ReturnDocument

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
    user["branches"] = [str(r) for r in user.get("branches", [])]
    if "primary_branch" in user: user["primary_branch"] = str(user['primary_branch'])
    for key, value in user.items():
        if isinstance(value, datetime):
            user[key] = value.isoformat()
    return user


class UserCreate(BaseModel):
    user_name: Optional[str]
    email: Optional[EmailStr]
    password: Optional[str]
    roles: Optional[List[str]]
    branches: Optional[List[str]]
    primary_branch: Optional[str] = None
    expiry_date: Optional[datetime]


# For updating a user (all fields optional)
class UserUpdate(BaseModel):
    user_name: Optional[str] = None
    email: Optional[EmailStr] = None
    password: Optional[str] = None
    roles: Optional[List[str]] = None
    branches: Optional[List[str]] = None
    primary_branch: Optional[str] = None
    expiry_date: Optional[datetime] = None


@router.get("/get_all_users")
async def get_all_users(data: dict = Depends(security.get_current_user)):
    try:
        all_users = await users_collection.find({"company_id": ObjectId(data.get("company_id"))}, {
            "_id": 1,
            "user_name": 1,
            "email": 1,
            "roles": 1,
            "branches": 1,
            "primary_branch": 1,
            "status": 1,
            "expiry_date": 1,
            "createdAt": 1,
            "updatedAt": 1}).to_list(None)

        return {"users": [serializer(u) for u in all_users]}

    except Exception as e:
        return {"message": str(e)}


from bson import ObjectId, errors
from fastapi import Depends, HTTPException


@router.post("/add_new_user")
async def add_new_user(user: UserCreate, data: dict = Depends(security.get_current_user)):
    try:
        # Validate company_id
        try:
            company_id = ObjectId(data.get("company_id"))
            print(company_id)
        except errors.InvalidId:
            raise HTTPException(status_code=400, detail="Invalid company ID")

        # Check for existing email
        existing_user = await users_collection.find_one(
            {"company_id": company_id, "email": user.email},
        )
        if existing_user:
            raise HTTPException(status_code=400, detail="Email already exists")

        # Convert roles to ObjectId
        roles_list = [ObjectId(role) for role in user.roles] if user.roles else []
        branches_list = [ObjectId(branch) for branch in user.branches] if user.branches else []

        # Hash password
        password_hash = security.pwd_ctx.hash(user.password) if user.password else None

        new_user = {
            "company_id": company_id,
            "user_name": user.user_name,
            "email": user.email,
            "password_hash": password_hash,
            "roles": roles_list,
            "branches": branches_list,
            "primary_branch": ObjectId(user.primary_branch) if user.primary_branch else None,
            "expiry_date": user.expiry_date,
            "status": True,
            "createdAt": security.now_utc(),
            "updatedAt": security.now_utc(),
        }

        # Insert new user
        result = await users_collection.insert_one(new_user)

        # Prepare response
        new_user["_id"] = str(result.inserted_id)
        new_user.pop("password_hash", None)
        new_user.pop("company_id", None)
        new_user = serializer(new_user)

        # Broadcast event
        await manager.broadcast({
            "type": "user_added",
            "data": new_user
        })

        return {
            "success": True,
            "message": "User created successfully",
            "user": new_user
        }

    except HTTPException:
        raise
    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.patch("/update_user/{user_id}")
async def update_user(
        user_id: str, user: UserUpdate, _: dict = Depends(security.get_current_user)
):
    try:
        # Convert user_id safely
        try:
            user_obj_id = ObjectId(user_id)
        except errors.InvalidId:
            raise HTTPException(status_code=400, detail="Invalid user ID")

        # Extract only provided fields
        user_data = user.model_dump(exclude_unset=True)

        # Hash password if provided
        if "password" in user_data:
            hashed = security.pwd_ctx.hash(user_data.pop("password"))
            user_data["password_hash"] = hashed

        # Convert roles to ObjectId
        if "roles" in user_data:
            user_data["roles"] = [ObjectId(role) for role in user_data["roles"]]
        if "branches" in user_data:
            user_data["branches"] = [ObjectId(branch) for branch in user_data["branches"]]
        if "primary_branch" in user_data and user_data["primary_branch"]:
            user_data["primary_branch"] = ObjectId(user_data["primary_branch"])

        user_data["updatedAt"] = security.now_utc()

        # Update user in MongoDB
        result = await users_collection.find_one_and_update(
            {"_id": user_obj_id},
            {"$set": user_data},
            projection={
                "_id": 1,
                "user_name": 1,
                "email": 1,
                "roles": 1,
                "branches": 1,
                "primary_branch": 1,
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

        # Broadcast update
        await manager.broadcast({
            "type": "user_updated",
            "data": updated_user
        })

        return updated_user

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


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

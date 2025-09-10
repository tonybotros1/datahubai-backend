from bson import ObjectId
from fastapi import APIRouter, Body, HTTPException, Depends
from pymongo import ReturnDocument

from app.core import security
from app.database import get_collection
from datetime import datetime, timezone
from app.websocket_config import manager

router = APIRouter()
roles_collection = get_collection("sys-roles")


def serialize_role(role: dict) -> dict:
    role["_id"] = str(role["_id"])
    role["menu_id"] = str(role.get("menu_id", "")) if role.get("menu_id") else ""
    for key, value in role.items():
        if isinstance(value, datetime):
            role[key] = value.isoformat()
    return role


async def get_role_details(role_id: str):
    pipeline = [
        {"$match": {"_id": ObjectId(role_id)}},
        {
            "$lookup": {
                "from": "menus",
                "localField": "menu_id",
                "foreignField": "_id",
                "as": "menus",
            }
        },
        {
            "$unwind": {
                "path": "$menus",
                "preserveNullAndEmptyArrays": True
            }
        },
        {
            "$project": {
                "_id": 1,
                "role_name": 1,
                "menu_name": {"$ifNull": ["$menus.name", ""]},
                "menu_id": {"$ifNull": ["$menus._id", ""]},
                "code": {"$ifNull": ["$menus.code", ""]},
                "is_shown_for_users": 1,
                "createdAt": 1,
                "updatedAt": 1,
            }
        }
    ]
    cursor = await roles_collection.aggregate(pipeline)
    role = await cursor.to_list(length=1)
    if not role:
        raise HTTPException(status_code=404, detail="Role not found")
    return serialize_role(role[0])


# =====================================================================================

# this function is to get all roles
@router.get("/get_all_roles")
async def get_all_roles(_: dict = Depends(security.get_current_user)):
    try:
        pipeline = [
            {
                "$lookup": {
                    "from": "menus",
                    "localField": "menu_id",
                    "foreignField": "_id",
                    "as": "menus",
                },

            },
            {
                "$unwind": {
                    "path": "$menus",
                    "preserveNullAndEmptyArrays": True  # 🔹 keep roles even if no menu match
                }
            },
            {
                "$project": {
                    "_id": 1,
                    "role_name": 1,
                    "menu_name": {"$ifNull": ["$menus.name", ""]},  # empty if no menu
                    "menu_id": {"$ifNull": ["$menus._id", ""]},  # empty if no menu
                    "code": {"$ifNull": ["$menus.code", ""]},
                    "is_shown_for_users": 1,
                    "createdAt": 1,
                    "updatedAt": 1,

                }
            }
        ]
        cursor = await roles_collection.aggregate(pipeline)
        results = await cursor.to_list()
        if not results:
            raise HTTPException(status_code=404, detail="Role not found")
        return {"roles": [serialize_role(result) for result in results]}

    except Exception as e:
        return {"error": str(e)}


@router.post("/add_new_role")
async def add_new_role(role_name: str | None = Body(None), menu_id: str | None = Body(None),
                       _: dict = Depends(security.get_current_user)):
    try:
        role_dict = {
            "role_name": role_name,
            "menu_id": ObjectId(menu_id),
            "is_shown_for_users": True,
            "createdAt": datetime.now(timezone.utc),
            "updatedAt": datetime.now(timezone.utc),
        }
        result = await roles_collection.insert_one(role_dict)
        serialized = await get_role_details(result.inserted_id)
        await manager.broadcast({
            "type": "role_created",
            "data": serialized
        })


    except Exception as e:
        return {"error": str(e)}


@router.patch("/update_role_status/{role_id}")
async def update_role_status(role_id: str, status: bool = Body(..., embed=True),
                             _: dict = Depends(security.get_current_user)):
    try:
        await roles_collection.find_one_and_update(
            {"_id": ObjectId(role_id)}, {"$set": {"is_shown_for_users": status,
                                                  "updatedAt": datetime.now(timezone.utc),
                                                  }},
            return_document=ReturnDocument.AFTER
        )
        serialized = await get_role_details(role_id)
        await manager.broadcast({
            "type": "role_updated",
            "data": serialized
        })
        return {"message": "Role updated successfully!", "Role": serialized}


    except Exception as e:
        return {"error": str(e)}


@router.patch("/update_role/{role_id}")
async def update_role(role_id: str, role_name: str | None = Body(None), menu_id: str | None = Body(None),
                      _: dict = Depends(security.get_current_user)):
    try:
        await  roles_collection.find_one_and_update({"_id": ObjectId(role_id)}, {
            "$set": {
                "role_name": role_name,
                "menu_id": ObjectId(menu_id),
                "updatedAt": datetime.now(timezone.utc),
            }
        })

        serialized = await get_role_details(role_id)
        await manager.broadcast({
            "type": "role_updated",
            "data": serialized
        })
        return {"message": "Role updated successfully!", "Role": serialized}


    except Exception as e:
        return {"error": str(e)}


@router.delete("/delete_role/{role_id}")
async def delete_role(role_id: str, _: dict = Depends(security.get_current_user)):
    try:
        result = await roles_collection.delete_one({"_id": ObjectId(role_id)})

        if result.deleted_count == 0:
            raise HTTPException(status_code=404, detail="Role not found")

        await manager.broadcast({
            "type": "role_deleted",
            "data": {"_id": role_id}
        })
        return {"message": "Role deleted successfully", "role_id": role_id}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

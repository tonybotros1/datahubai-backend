from typing import Optional
from bson import ObjectId
from fastapi import APIRouter, Body, Depends, Query, Path, HTTPException
from app.core import security
from app.database import get_collection
from datetime import datetime, timezone
from app.websocket_config import manager

router = APIRouter()
list_collection = get_collection("all_lists")
value_collection = get_collection("all_lists_values")


def serializer(data: dict) -> dict:
    data["_id"] = str(data["_id"])
    if "mastered_by_id" in data:
        data["mastered_by_id"] = str(data["mastered_by_id"])
    for key, value in data.items():
        if isinstance(value, datetime):
            data[key] = value.isoformat()
    return data


# ====================================== Lists Section ============================================

async def get_list_details(list_id: ObjectId) -> dict:
    pipeline = [
        {
            "$match": {
                "_id": ObjectId(list_id)
            }
        },
        {
            "$lookup": {
                "from": "all_lists",
                "localField": "mastered_by",
                "foreignField": "_id",
                "as": "new_list",
            }
        },
        {
            "$unwind": {
                "path": "$new_list",
                "preserveNullAndEmptyArrays": True

            }
        },
        {
            "$project": {
                "_id": 1,
                "code": {"$ifNull": ["$code", '']},
                "name": {"$ifNull": ["$name", '']},
                "mastered_by": {"$ifNull": ["$new_list.name", '']},
                "mastered_by_id": {"$ifNull": ["$mastered_by", '']},
                "status": 1,
                "createdAt": 1,
                "updatedAt": 1,
            }
        }
    ]
    all_lists = await list_collection.aggregate(pipeline)
    result = await all_lists.to_list(length=None)
    return result[0]


@router.get("/get_all_lists")
async def get_all_lists(_: dict = Depends(security.get_current_user)):
    try:
        pipeline = [
            {
                "$lookup": {
                    "from": "all_lists",
                    "localField": "mastered_by",
                    "foreignField": "_id",
                    "as": "final_list"
                }
            },
            {
                "$unwind": {
                    "path": "$final_list",
                    "preserveNullAndEmptyArrays": True
                }
            },
            {
                "$project": {
                    "_id": 1,
                    "code": {"$ifNull": ["$code", '']},
                    "name": {"$ifNull": ["$name", '']},
                    "mastered_by": {"$ifNull": ["$final_list.name", '']},
                    "mastered_by_id": {"$ifNull": ["$mastered_by", '']},
                    "status": 1,
                    "createdAt": 1,
                    "updatedAt": 1,
                }
            }
        ]
        all_lists = await list_collection.aggregate(pipeline)
        result = await all_lists.to_list(length=None)
        return {"all_lists": [serializer(r) for r in result]}

    except Exception as e:
        return {"message": str(e)}


@router.post("/add_new_list")
async def add_new_list(
        _: dict = Depends(security.get_current_user),
        name: str = Body(None), code: str = Body(None),
        mastered_by: str = Body(None),
):
    mastered_by_id = ObjectId(mastered_by) if mastered_by else ''

    try:
        list_dict = {
            "name": name,
            "code": code,
            "mastered_by": mastered_by_id,
            "status": True,
            "createdAt": datetime.now(timezone.utc),
            "updatedAt": datetime.now(timezone.utc),
        }
        result = await list_collection.insert_one(list_dict)
        new_list = await get_list_details(result.inserted_id)
        serialized = serializer(new_list)
        await manager.broadcast({
            "type": "list_added",
            "data": serialized
        })
        return {"message": "List added successfully!", "list": serialized}


    except Exception as e:
        return {"message": str(e)}


@router.delete("/remove_list/{list_id}")
async def remove_list(list_id: str, _: dict = Depends(security.get_current_user)):
    try:
        result = await list_collection.delete_one({"_id": ObjectId(list_id)})

        if result.deleted_count == 1:
            await manager.broadcast({
                "type": "list_deleted",
                "data": {"_id": list_id}
            })
            return {"message": "List deleted successfully!"}
        else:
            return {"message": "List not found"}

    except Exception as e:
        return {"message": str(e)}


@router.patch("/update_list/{list_id}")
async def update_list(list_id: str, name: str = Body(None), code: str = Body(None),
                      mastered_by: str = Body(None), _: dict = Depends(security.get_current_user)):
    try:
        mastered_by_id = ObjectId(mastered_by) if mastered_by else ''

        await  list_collection.find_one_and_update({"_id": ObjectId(list_id)}, {
            "$set": {
                "name": name,
                "code": code,
                "mastered_by": mastered_by_id,
                "updatedAt": datetime.now(timezone.utc),
            }
        })
        updated_list = await get_list_details(ObjectId(list_id))
        serialized = serializer(updated_list)
        await manager.broadcast({
            "type": "list_updated",
            "data": serialized
        })
        return {"message": "List updated successfully!", "list": serialized}

    except Exception as e:
        return {"message": str(e)}


# ====================================== Values Section ============================================

async def get_value_details(value_id: ObjectId):
    pipeline = [
        {
            "$match": {
                "_id": value_id
            }
        },
        {
            "$lookup": {
                "from": "all_lists_values",
                "localField": "mastered_by",
                "foreignField": "_id",
                "as": "final_value",
            }
        },
        {"$unwind": {"path": "$final_value", "preserveNullAndEmptyArrays": True}},
        {
            "$project": {
                "_id": 1,
                "name": {"$ifNull": ["$name", '']},
                "mastered_by": {"$ifNull": ["$final_value.name", '']},
                "mastered_by_id": {"$ifNull": ["$mastered_by", '']},
                "status": 1,
                "createdAt": 1,
                "updatedAt": 1,
            }
        }
    ]
    agg_cursor = await value_collection.aggregate(pipeline)
    current_value = await agg_cursor.to_list(length=None)
    return current_value[0]


@router.get("/get_list_values/{list_id}")
async def get_list_values(list_id: str, mastered_by_list_id: Optional[str] = Query(None),
                          _: dict = Depends(security.get_current_user)):
    try:
        if mastered_by_list_id:
            mastered_by_list_id = ObjectId(mastered_by_list_id)

        # Pipeline لإحضار قيم القائمة نفسها
        pipeline_values = [
            {"$match": {"list_id": ObjectId(list_id)}},
            {
                "$lookup": {
                    "from": "all_lists_values",
                    "localField": "mastered_by",
                    "foreignField": "_id",
                    "as": "final_values",
                }
            },
            {"$unwind": {"path": "$final_values", "preserveNullAndEmptyArrays": True}},
            {
                "$project": {
                    "_id": 1,
                    "name": {"$ifNull": ["$name", ""]},
                    "mastered_by": {"$ifNull": ["$final_values.name", ""]},
                    "status": 1,
                    "createdAt": 1,
                    "updatedAt": 1,
                }
            },
        ]

        agg_cursor = await value_collection.aggregate(pipeline_values)
        current_list_values = await agg_cursor.to_list(length=None)

        # إذا القائمة لها master، نجيب قيم الـ master list أيضاً
        master_list_values = []
        if mastered_by_list_id:
            pipeline_master = [
                {"$match": {"list_id": mastered_by_list_id}},
                {"$project": {"_id": 1, "name": 1}},
            ]
            agg_master = await value_collection.aggregate(pipeline_master)
            master_list_values = await agg_master.to_list(length=None)

        return {
            "list_values": [serializer(v) for v in current_list_values],
            "master_values": [serializer(v) for v in master_list_values],
        }

    except Exception as e:
        return {"message": str(e)}


@router.post("/add_new_value/{list_id}")
async def add_new_value(list_id: str = Path(...),
                        name: str = Body(None),
                        mastered_by_id: str = Body(None),
                        _: dict = Depends(security.get_current_user)
                        ):
    try:
        mastered_by_id = ObjectId(mastered_by_id) if mastered_by_id else ''
        list_id = ObjectId(list_id) if list_id else ''
        value_dict = {
            "name": name,
            "mastered_by": mastered_by_id,
            "list_id": list_id,
            "status": True,
            "createdAt": datetime.now(timezone.utc),
            "updatedAt": datetime.now(timezone.utc),
        }

        result = await value_collection.insert_one(value_dict)
        new_value = await get_value_details(result.inserted_id)
        serialized = serializer(new_value)
        await manager.broadcast({
            "type": "list_value_added",
            "data": serialized
        })
        return {"message": "Value added successfully!", "list": serialized}


    except Exception as e:
        return {"message": str(e)}


@router.delete("/delete_value/{value_id}")
async def delete_value(value_id: str, _: dict = Depends(security.get_current_user)):
    try:
        result = await value_collection.delete_one({"_id": ObjectId(value_id)})
        if result.deleted_count == 1:
            await manager.broadcast({
                "type": "list_value_deleted",
                "data": {"_id": value_id}
            })
            return {"message": "Value deleted successfully!"}

    except Exception as e:
        return {"message": str(e)}


#
@router.patch("/update_value/{value_id}")
async def update_value(value_id: str, name: str = Body(None),
                       mastered_by_id: str = Body(None),
                       _: dict = Depends(security.get_current_user)
                       ):
    try:
        mastered_by_id = ObjectId(mastered_by_id) if mastered_by_id else ''
        result = await value_collection.find_one_and_update(
            {"_id": ObjectId(value_id)},
            {"$set": {"name": name, "mastered_by": mastered_by_id,
                      "updatedAt": datetime.now(timezone.utc), }},
        )
        if not result:
            raise HTTPException(status_code=404, detail="Model not found")

        edited_value = await get_value_details(ObjectId(value_id))
        serialized = serializer(edited_value)

        await manager.broadcast({
            "type": "list_value_updated",
            "data": serialized
        })

        return {"message": "Value updated successfully!", "value": serialized}


    except Exception as e:
        return {"message": str(e)}


@router.get("/get_list_values_by_code")
async def get_list_values_by_code(code: str, _: dict = Depends(security.get_current_user)):
    try:
        pipeline = [
            {
                "$match": {"code": code.upper(), "status": True},
            },
            {"$lookup": {
                "from": "all_lists_values",
                "let": {"listId": "$_id"},
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$and": [
                                    {"$eq": ["$list_id", "$$listId"]},
                                    {"$eq": ["$status", True]}
                                ]
                            }
                        }
                    }
                ],
                "as": "value"
            }
            },
            {
                "$unwind": {
                    "path": "$value",
                    "preserveNullAndEmptyArrays": True
                }
            },
            {
                "$project": {
                    "_id": "$value._id",
                    "name": {"$ifNull": ["$value.name", ""]},
                    "status": {"$ifNull": ["$value.status", ""]},
                    "mastered_by": {"$ifNull": ["$value.mastered_by", ""]},

                }
            }
        ]

        cursor = await list_collection.aggregate(pipeline)
        results = await cursor.to_list()
        if not results:
            raise HTTPException(status_code=404, detail="Values not found")
        return {"values": [serializer(result) for result in results]}


    except Exception as error:
        raise error

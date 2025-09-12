from bson import ObjectId
from fastapi import APIRouter, Body, Depends
from app.core import security
from app.database import get_collection
from datetime import datetime, timezone
from app.websocket_config import manager

router = APIRouter()
list_collection = get_collection("all_lists")
value_collection = get_collection("all_lists_values")


def serializer(data: dict) -> dict:
    data["_id"] = str(data["_id"])
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
@router.get("/get_list_values/{list_id}/{mastered_by_list_id}")
async def get_list_values(list_id: str, mastered_by_list_id: str):
    try:
        print(f"mastered_by_list_id for list {list_id}: {mastered_by_list_id}")
        mastered_by_list_id = ObjectId(mastered_by_list_id)

        # Pipeline لإحضار قيم القائمة نفسها
        pipeline_values = [
            {"$match": {"list_id": ObjectId(list_id)}},
            {
                "$lookup": {
                    "from": "all_lists_values",  # self-join على القيم إذا بدك اسم master للقيم
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

from bson import ObjectId
from fastapi import APIRouter, Body, HTTPException, Depends
from pymongo import ReturnDocument
from app.core import security
from app.database import get_collection
from datetime import datetime, timezone
from app.websocket_config import manager

router = APIRouter()
counters_collection = get_collection("counters")


def serializer(counter: dict) -> dict:
    counter["_id"] = str(counter["_id"])
    counter['company_id'] = str(counter['company_id'])
    for key, value in counter.items():
        if isinstance(value, datetime):
            counter[key] = value.isoformat()
    return counter


@router.get("/get_all_counters")
async def get_all_counters(data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))

        counters = await counters_collection.find({"company_id": company_id}) \
            .sort("code", 1) \
            .to_list(length=None)

        return {"counters": [serializer(c) for c in counters]}

    except Exception as error:
        return {"message": str(error)}


@router.post("/add_new_counter")
async def add_new_counter(code: str = Body(None), description: str = Body(None), prefix: str = Body(None),
                          value: int = Body(None), length: int = Body(None), separator: str = Body(None),
                          data: dict = Depends(security.get_current_user)
                          ):
    try:
        company_id = ObjectId(data.get("company_id"))
        counter_dict = {
            "code": code,
            "description": description,
            "prefix": prefix,
            "value": value,
            "length": length,
            "separator": separator,
            "company_id": company_id,
            "status": True,
            "createdAt": datetime.now(timezone.utc),
            "updatedAt": datetime.now(timezone.utc),
        }
        result = await counters_collection.insert_one(counter_dict)
        counter_dict["_id"] = str(result.inserted_id)
        serialized = serializer(counter_dict)
        await manager.broadcast({
            "type": "counter_added",
            "data": serialized
        })

    except Exception as error:
        return {"message": str(error)}


@router.delete("/remove_counter/{counter_id}")
async def remove_counter(counter_id: str, _: dict = Depends(security.get_current_user)):
    try:
        result = await counters_collection.delete_one({"_id": ObjectId(counter_id)})
        if result.deleted_count == 1:
            await manager.broadcast({
                "type": "counter_deleted",
                "data": {"_id": counter_id}
            })
            return {"message": "Counter removed successfully!"}
        else:
            raise HTTPException(status_code=404, detail="Counter not found")

    except Exception as error:
        return {"message": str(error)}


@router.patch("/update_counter/{counter_id}")
async def update_counter(counter_id: str,
                         code: str = Body(None), description: str = Body(None), prefix: str = Body(None),
                         value: int = Body(None), length: int = Body(None), separator: str = Body(None)
                         , _: dict = Depends(security.get_current_user)
                         ):
    try:
        result = await counters_collection.find_one_and_update(
            {"_id": ObjectId(counter_id)},
            {"$set": {"code": code, "description": description, "prefix": prefix, "value": value, "length": length,
                      "separator": separator,
                      "updatedAt": datetime.now(timezone.utc), }},
            return_document=ReturnDocument.AFTER
        )
        if not result:
            raise HTTPException(status_code=404, detail="Model not found")

        serialized = serializer(result)

        await manager.broadcast({
            "type": "counter_updated",
            "data": serialized
        })
        return {"message": "Counter updated successfully!", "counter": serialized}

    except Exception as error:
        return {"message": str(error)}


@router.patch("/change_counter_status/{counter_id}")
async def change_counter_status(counter_id: str, status: bool = Body(None),
                                _: dict = Depends(security.get_current_user)
                                ):
    try:
        result = await counters_collection.find_one_and_update(
            {"_id": ObjectId(counter_id)}, {"$set": {"status": status, "updatedAt": datetime.now(timezone.utc), }},
            return_document=ReturnDocument.AFTER
        )
        if not result:
            raise HTTPException(status_code=404, detail="Counter not found")
        serialized = serializer(result)
        await manager.broadcast({
            "type": "counter_updated",
            "data": serialized
        })
        return {"message": "Counter updated successfully!", "counter": serialized}

    except Exception as error:
        return {"message": str(error)}

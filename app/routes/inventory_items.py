from typing import Optional

from bson import ObjectId
from fastapi import APIRouter, Body, HTTPException, Depends
from pydantic import BaseModel
from pymongo import ReturnDocument
from app.core import security
from app.database import get_collection
from datetime import datetime, timezone
from app.websocket_config import manager

router = APIRouter()
inventory_items_collection = get_collection("inventory_items")


def serializer(item: dict) -> dict:
    item["_id"] = str(item["_id"])
    if item['company_id']:
        item['company_id'] = str(item['company_id'])
    for key, value in item.items():
        if isinstance(value, datetime):
            item[key] = value.isoformat()
    return item


class InventoryItem(BaseModel):
    name: Optional[str] = None
    code: Optional[str] = None
    min_quantity: Optional[float] = None


@router.get("/get_all_inventory_items")
async def get_all_inventory_items(data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        results = await inventory_items_collection.find({"company_id": company_id}).to_list(None)
        return {"inventory_items": [serializer(r) for r in results]}

    except Exception as e:
        return {"message": str(e)}


@router.post("/add_new_inventory_item")
async def add_new_inventory_item(inventory_item: InventoryItem, data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        item = inventory_item.model_dump(exclude_unset=True)
        item_dict = {
            "company_id": company_id,
            "name": item["name"],
            "code": item["code"],
            "min_quantity": item["min_quantity"],
            "createdAt": security.now_utc(),
            "updatedAt": security.now_utc(),
        }
        result = await inventory_items_collection.insert_one(item_dict)
        if not result.inserted_id:
            raise HTTPException(status_code=500, detail="Failed to insert item")
        item_dict["_id"] = result.inserted_id
        serialized = serializer(item_dict)
        await manager.broadcast({
            "type": "inventory_item_added",
            "data": serialized
        })
        return {"item": serialized}

    except HTTPException:
        raise
    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=str(e))


@router.patch("/update_inventory_item/{item_id}")
async def update_inventory_item(item_id: str, inventory_item: InventoryItem,
                                _: dict = Depends(security.get_current_user)):
    try:
        item_id = ObjectId(item_id)
        items = inventory_item.model_dump(exclude_unset=True)
        items["updatedAt"] = security.now_utc()
        result = await inventory_items_collection.find_one_and_update(
            {"_id": item_id},
            {"$set": items},
            return_document=ReturnDocument.AFTER
        )
        if not result:
            raise HTTPException(status_code=404, detail="Item not found")

        serialized = serializer(result)
        print(serialized)

        await manager.broadcast({
            "type": "inventory_item_updated",
            "data": serialized
        })

    except Exception as e:
        return {"message": str(e)}


@router.delete("/delete_item/{item_id}")
async def delete_branch(item_id: str, _: dict = Depends(security.get_current_user)):
    try:
        result = await inventory_items_collection.delete_one({"_id": ObjectId(item_id)})
        if result.deleted_count == 1:
            await manager.broadcast({
                "type": "inventory_item_deleted",
                "data": {"_id": item_id}
            })
            return {"message": "Inventory Item removed successfully!"}
        else:
            raise HTTPException(status_code=404, detail="Item not found")

    except Exception as error:
        return {"message": str(error)}


@router.post("/search_engine_for_inventory_items")
async def search_engine_for_inventory_items(items: InventoryItem, data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        match_stage = {}
        pipeline = []
        if company_id:
            match_stage["company_id"] = company_id
        if items.code:
            match_stage["code"] = {"$regex": items.code, "$options": "i"}
        if items.name:
            match_stage["name"] = {"$regex": items.name, "$options": "i"}
        if items.min_quantity:
            match_stage["min_quantity"] = items.min_quantity

        pipeline.append({"$match": match_stage})
        pipeline.append({
            '$addFields': {
                '_id': {
                    '$toString': '$_id'
                },
                'company_id': {
                    '$toString': '$company_id'
                }
            }
        })
        pipeline.append({
            '$limit': 200
        })
        cursor = await inventory_items_collection.aggregate(pipeline)
        results = await cursor.to_list(None)
        return {"inventory_items": results}

    except Exception as error:
        return {"message": str(error)}

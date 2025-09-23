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
salesman_collection = get_collection("sales_man")


def serializer(salesman: dict) -> dict:
    salesman["_id"] = str(salesman["_id"])
    salesman['company_id'] = str(salesman['company_id'])
    for key, value in salesman.items():
        if isinstance(value, datetime):
            salesman[key] = value.isoformat()
    return salesman


class SaleManModel(BaseModel):
    name: Optional[str] = None
    target: Optional[float] = None


@router.get("/get_all_salesman")
async def get_all_salesman(data: dict = Depends(security.get_current_user)):
    company_id = ObjectId(data.get("company_id"))
    result = await salesman_collection.find({"company_id": company_id}).sort("name", 1).to_list(length=None)
    return {"salesman": [serializer(s) for s in result]}


@router.post("/add_new_salesman")
async def add_new_salesman(sale_man: SaleManModel, data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))

        sale_man_dict = {
            "name": sale_man.name,
            "target": sale_man.target,
            "company_id": company_id,
            "createdAt": datetime.now(timezone.utc),
            "updatedAt": datetime.now(timezone.utc),
        }

        result = await  salesman_collection.insert_one(sale_man_dict)
        sale_man_dict["_id"] = result.inserted_id
        serialized = serializer(sale_man_dict)
        await manager.broadcast({
            "type": "salesman_added",
            "data": serialized
        })

    except Exception as e:
        raise e


@router.delete("/delete_salesman/{salesman_id}")
async def delete_salesman(salesman_id: str, _: dict = Depends(security.get_current_user)):
    try:
        result = await salesman_collection.delete_one({"_id": ObjectId(salesman_id)})
        if result.deleted_count == 0:
            raise HTTPException(status_code=404, detail="Salesman not found")

        await manager.broadcast({
            "type": "salesman_deleted",
            "data": {"_id": str(salesman_id)}
        })
    except HTTPException as e:  # Let FastAPI handle HTTP errors
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal server error")


@router.patch("/update_salesman/{salesman_id}")
async def update_salesman(salesman_id: str, sale_man: SaleManModel, _: dict = Depends(security.get_current_user)):
    try:
        update_data = sale_man.model_dump(exclude_unset=True)

        result = await salesman_collection.find_one_and_update({"_id": ObjectId(salesman_id)}, {"$set": update_data},
                                                               return_document=ReturnDocument.AFTER)
        if not result:
            raise HTTPException(status_code=404, detail="Salesman not found")
        serialized = serializer(result)
        await manager.broadcast({
            "type": "salesman_updated",
            "data": serialized
        })

    except HTTPException as e:  # Let FastAPI handle HTTP errors
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal server error")

from datetime import datetime
from typing import Optional

from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException, status, Form, UploadFile, File, Body
from pydantic import BaseModel
from pymongo import ReturnDocument
from pymongo.errors import DuplicateKeyError, PyMongoError
from app import database
from app.core import security
from app.database import get_collection
from app.websocket_config import manager
from app.widgets import upload_images

router = APIRouter()
ap_payment_types_collection = get_collection("ap_payment_types")


def serializer(doc):
    doc["_id"] = str(doc["_id"])
    doc["company_id"] = str(doc["company_id"])
    for key, value in doc.items():
        if isinstance(value, datetime):
            doc[key] = value.isoformat()
    return doc


class APPaymentTypes(BaseModel):
    type: Optional[str] = None


@router.get("/get_all_ap_payment_types")
async def get_all_ap_payment_types(data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        types = await ap_payment_types_collection.find({"company_id": company_id}).sort("type", 1).to_list(None)
        return {"types": [serializer(t) for t in types]}

    except Exception as e:
        raise e


@router.post("/add_new_ap_payment_type")
async def add_new_ap_payment_type(types: APPaymentTypes, data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        types = types.model_dump(exclude_unset=True)
        type_dict = {
            "type": types.get('type', None),
            "company_id": company_id,
            "createdAt": security.now_utc(),
            "updatedAt": security.now_utc(),
        }
        result = await ap_payment_types_collection.insert_one(type_dict)
        type_dict["_id"] = str(result.inserted_id)
        serialized = serializer(type_dict)
        await manager.broadcast({
            "type": "ap_payment_type_added",
            "data": serialized
        })
    except Exception as e:
        raise e


@router.patch("/update_new_ap_payment_type/{type_id}")
async def update_new_ap_payment_type(type_id: str, types: APPaymentTypes,
                                     _: dict = Depends(security.get_current_user)):
    try:
        types = types.model_dump(exclude_unset=True)
        type_dict = {
            "type": types.get('type', None),
            "updatedAt": security.now_utc(),
        }
        result = await ap_payment_types_collection.find_one_and_update({"_id": ObjectId(type_id)}, {"$set": type_dict},
                                                                       return_document=ReturnDocument.AFTER)
        serialized = serializer(result)
        await manager.broadcast({
            "type": "ap_payment_type_updated",
            "data": serialized
        })
    except Exception as e:
        raise e


@router.delete("/delete_ap_payment_type/{type_id}")
async def delete_ap_payment_type(type_id: str, _: dict = Depends(security.get_current_user)):
    try:
        result = await ap_payment_types_collection.delete_one({"_id": ObjectId(type_id)})
        if result.deleted_count == 0:
            raise HTTPException(status_code=404, detail="Payment type not found")
        await manager.broadcast({
            "type": "ap_payment_type_deleted",
            "data": {"_id": type_id}
        })

    except Exception as e:
        raise e

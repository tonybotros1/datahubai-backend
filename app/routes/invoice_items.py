from typing import Optional
from bson import ObjectId
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from pymongo import ReturnDocument
from app.core import security
from app.database import get_collection
from datetime import datetime
from app.websocket_config import manager

router = APIRouter()
invoice_items_collection = get_collection("invoice_items")


def serializer(doc):
    doc["_id"] = str(doc["_id"])
    doc["company_id"] = str(doc["company_id"])
    for key, value in doc.items():
        if isinstance(value, datetime):
            doc[key] = value.isoformat()
    return doc


class InvoiceItem(BaseModel):
    name: Optional[str] = None
    price: Optional[float] = None
    description: Optional[str] = None


@router.get("/get_all_invoice_items")
async def get_all_invoice_items(data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        results = await invoice_items_collection.find({"company_id": company_id}).sort("name", 1).to_list()
        return {"invoice_items": [serializer(i) for i in results]}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/add_new_invoice_item")
async def add_new_invoice_item(invoice: InvoiceItem, data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        invoice = invoice.model_dump(exclude_unset=True)
        invoice_dict = {
            "company_id": company_id,
            "name": invoice['name'],
            "price": invoice['price'],
            "description": invoice['description'],
            "createdAt": security.now_utc(),
            "updatedAt": security.now_utc(),
        }
        new_invoice = await invoice_items_collection.insert_one(invoice_dict)
        invoice_dict["_id"] = new_invoice.inserted_id
        serialized = serializer(invoice_dict)
        await manager.broadcast({
            "type": "invoice_item_added",
            "data": serialized
        })
        return {"message": "invoice added successfully!", "invoice": serialized}


    except HTTPException:
        raise
    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=str(e))


@router.patch("/update_invoice_item/{invoice_id}")
async def update_invoice_item(invoice_id: str, invoice: InvoiceItem, _: dict = Depends(security.get_current_user)):
    try:
        invoice = invoice.model_dump(exclude_unset=True)
        invoice["updatedAt"] = security.now_utc()
        updated_invoice = await invoice_items_collection.find_one_and_update({"_id": ObjectId(invoice_id)},
                                                                             {"$set": invoice},
                                                                             return_document=ReturnDocument.AFTER)
        serialized = serializer(updated_invoice)
        await manager.broadcast({
            "type": "invoice_item_updated",
            "data": serialized
        })

    except HTTPException:
        raise
    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/delete_invoice_item/{invoice_id}")
async def delete_technician(invoice_id: str, _: dict = Depends(security.get_current_user)):
    try:
        result = await invoice_items_collection.delete_one({"_id": ObjectId(invoice_id)})
        if result.deleted_count == 0:
            raise HTTPException(status_code=404, detail="Invoice not found")

        await manager.broadcast({
            "type": "invoice_item_deleted",
            "data": {"_id": invoice_id}
        })

    except HTTPException:
        raise
    except Exception as error:
        raise HTTPException(status_code=500, detail=str(error))

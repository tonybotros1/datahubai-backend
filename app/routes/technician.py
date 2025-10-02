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
all_technicians_collection = get_collection("all_technicians")


def serializer(doc):
    doc['_id'] = str(doc['_id'])
    doc['company_id'] = str(doc['company_id'])
    for key, value in doc.items():
        if isinstance(value, datetime):
            doc[key] = value.isoformat()
    return doc


class Technician(BaseModel):
    name: Optional[str] = None
    job: Optional[str] = None


@router.get("/get_all_technicians")
async def get_all_technicians(data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        results = await all_technicians_collection.find({"company_id": company_id}).sort("name", 1).to_list(None)
        return {"technicians": [serializer(t) for t in results]}

    except Exception as error:
        raise error


@router.post("/add_new_technician")
async def add_new_technician(tech: Technician, data: dict = Depends(security.get_current_user)):
    try:
        tech = tech.model_dump(exclude_unset=True)
        company_id = data.get('company_id')
        tech_dict = {
            "company_id": ObjectId(company_id),
            "name": tech['name'],
            "job": tech['job'],
            "createdAt": security.now_utc(),
            "updatedAt": security.now_utc(),
        }
        result = await all_technicians_collection.insert_one(tech_dict)
        tech_dict["_id"] = result.inserted_id
        serialized = serializer(tech_dict)
        await manager.broadcast({
            "type": "tech_added",
            "data": serialized
        })

    except HTTPException:
        raise
    except Exception as error:
        raise HTTPException(status_code=500, detail=str(error))


@router.patch("/update_technician/{tech_id}")
async def update_technician(tech_id: str, tech: Technician, _: dict = Depends(security.get_current_user)):
    try:
        tech = tech.model_dump(exclude_unset=True)
        tech["updatedAt"] = security.now_utc()

        result = await all_technicians_collection.find_one_and_update({"_id": ObjectId(tech_id)}, {"$set": tech},
                                                                      return_document=ReturnDocument.AFTER)
        print(result)
        serialized = serializer(result)
        await manager.broadcast({
            "type": "tech_updated",
            "data": serialized
        })

    except HTTPException:
        raise
    except Exception as error:
        raise HTTPException(status_code=500, detail=str(error))


@router.delete("/delete_technicians/{tech_id}")
async def delete_technician(tech_id: str, _: dict = Depends(security.get_current_user)):
    try:
        result = await all_technicians_collection.delete_one({"_id": ObjectId(tech_id)})
        if result.deleted_count == 0:
            raise HTTPException(status_code=404, detail="Technician not found")

        await manager.broadcast({
            "type": "tech_deleted",
            "data": {"_id": tech_id}
        })

    except HTTPException:
        raise
    except Exception as error:
        raise HTTPException(status_code=500, detail=str(error))

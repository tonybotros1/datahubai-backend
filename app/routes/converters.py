import copy
from typing import Optional, List, Any
from bson import ObjectId
from fastapi import APIRouter, HTTPException, Depends, UploadFile, Form, File
from pydantic import BaseModel
from app import database
from app.core import security
from app.database import get_collection
from datetime import datetime, timezone, timedelta
from app.routes.car_trading import PyObjectId
from app.routes.counters import create_custom_counter
from app.routes.quotation_cards import get_quotation_card_details
from app.widgets.check_date import is_date_equals_today_or_older
from app.widgets.upload_files import upload_file, delete_file_from_server
from app.widgets.upload_images import upload_image, delete_image_from_server

router = APIRouter()
converters_collection = get_collection("converters")


class Converter(BaseModel):
    date: Optional[datetime] = None
    name: Optional[str] = None
    description: Optional[str] = None
    status: Optional[str] = None



def serializer(doc: dict) -> dict:
    def convert(value):
        if isinstance(value, ObjectId):
            return str(value)
        elif isinstance(value, datetime):
            return value.isoformat()
        elif isinstance(value, list):
            return [convert(v) for v in value]
        elif isinstance(value, dict):
            return {k: convert(v) for k, v in value.items()}
        return value

    return {k: convert(v) for k, v in doc.items()}

@router.post("/add_new_converter")
async def add_new_converter(converter: Converter, data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        converter = converter.model_dump(exclude_unset=True)
        new_converter_counter = await create_custom_counter("CN", "C", description='Converter Number', data=data)

        converter['company_id'] = company_id
        converter['status'] = 'New'
        converter['converter_number'] = new_converter_counter['final_counter'] if new_converter_counter[
            'success'] else None
        converter['createdAt'] = security.now_utc()
        converter['updatedAt'] = security.now_utc()

        result = await converters_collection.insert_one(converter)
        if not result.inserted_id:
            raise HTTPException(status_code=500, detail="Failed to insert converter")

        return {"converter_id": str(result.inserted_id),"converter_number": new_converter_counter['final_counter']}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.patch("/update_converter/{converter_id}")
async def update_converter(converter_id:str,converter: Converter, _: dict = Depends(security.get_current_user)):
    try:
        converter_id = ObjectId(converter_id)
        converter = converter.model_dump(exclude_unset=True)
        converter['updatedAt'] = security.now_utc()

        result = await converters_collection.update_one({"_id": converter_id}, {"$set": converter})
        if  result.modified_count == 0:
            raise HTTPException(status_code=404, detail="Converter not found")


    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/get_converter_status/{converter_id}")
async def get_converter_status(converter_id: str, _: dict = Depends(security.get_current_user)):
    try:
        if not ObjectId.is_valid(converter_id):
            raise HTTPException(status_code=400, detail="Invalid job_id format")

        converter_id = ObjectId(converter_id)

        result = await converters_collection.find_one(
            {"_id": converter_id},
            {"_id": 0, "status": 1}
        )

        if not result:
            raise HTTPException(status_code=404, detail="Converter not found")

        return {"status": "success", "data": result}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")



@router.get("/get_all_converter")
async def get_all_converters(data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        results = await converters_collection.find({"company_id": company_id}).to_list(None)
        return {"converters": [serializer(r) for r in results]}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
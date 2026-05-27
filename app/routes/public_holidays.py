from typing import Optional
from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException
from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel
from app.core import security
from app.database import get_collection
from datetime import datetime

router = APIRouter()
public_holidays_collection = get_collection("public_holidays")


class PublicHolidaysModel(BaseModel):
    name: Optional[str] = None
    date: Optional[datetime] = None
    legislation: Optional[str] = None


class PublicHolidaysFilterModel(BaseModel):
    legislation: Optional[str] = None
    year: Optional[int] = None


def optional_object_id(value: Optional[str], field_name: str) -> Optional[ObjectId]:
    if not value:
        return None
    if not ObjectId.is_valid(value):
        raise HTTPException(status_code=400, detail=f"Invalid {field_name}")
    return ObjectId(value)


def encode_object_ids(doc: dict) -> dict:
    encoded_doc = dict(doc)
    for field in ("_id", "company_id", "legislation"):
        if isinstance(encoded_doc.get(field), ObjectId):
            encoded_doc[field] = str(encoded_doc[field])
    return encoded_doc


@router.post("/get_all_holidays")
async def get_public_holidays(holidays_filter: PublicHolidaysFilterModel,
                              data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        match_filters = {"company_id": company_id}

        if holidays_filter.legislation:
            match_filters["legislation"] = optional_object_id(
                holidays_filter.legislation,
                "legislation",
            )

        if holidays_filter.year:
            match_filters["date"] = {
                "$gte": datetime(holidays_filter.year, 1, 1),
                "$lt": datetime(holidays_filter.year + 1, 1, 1),
            }

        results = await public_holidays_collection.find(match_filters, {
            "_id": 1,
            "name": 1,
            "date": 1,
            "legislation": 1,
        }).sort("date", 1).to_list(None)
        results = [encode_object_ids(item) for item in results]

        return {"holidays": results}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/add_new_holiday")
async def add_new_holiday(holiday: PublicHolidaysModel, data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        holiday = holiday.model_dump(exclude_unset=True)
        legislation = optional_object_id(holiday.get("legislation"), "legislation")
        if legislation:
            holiday["legislation"] = legislation
        else:
            holiday.pop("legislation", None)
        holiday['company_id'] = company_id
        holiday['createdAt'] = security.now_utc()
        holiday['updatedAt'] = security.now_utc()
        new_element = await public_holidays_collection.insert_one(holiday)
        holiday['_id'] = str(new_element.inserted_id)
        added_element = jsonable_encoder(encode_object_ids(holiday))

        return {"added_holiday": added_element}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.patch("/update_holiday/{holiday_id}")
async def update_holiday(holiday_id: str, holiday: PublicHolidaysModel,
                         _: dict = Depends(security.get_current_user)):
    try:
        holiday = holiday.model_dump(exclude_unset=True)
        legislation = optional_object_id(holiday.get("legislation"), "legislation")
        if legislation:
            holiday["legislation"] = legislation
        else:
            holiday.pop("legislation", None)
        holiday['updatedAt'] = security.now_utc()
        updated_element = await public_holidays_collection.update_one({"_id": ObjectId(holiday_id)}, {"$set": holiday})
        if updated_element.matched_count == 0:
            raise HTTPException(status_code=500, detail=f"Holiday ID not found")
        holiday['_id'] = str(holiday_id)
        updated_holiday = jsonable_encoder(encode_object_ids(holiday))

        return {"updated_holiday": updated_holiday}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/delete_holiday/{holiday_id}")
async def add_new_holiday(holiday_id: str, _: dict = Depends(security.get_current_user)):
    try:
        deleted_element = await public_holidays_collection.delete_one({"_id": ObjectId(holiday_id)})
        if deleted_element.deleted_count == 0:
            raise HTTPException(status_code=404, detail="holiday doc not found")

        return {"deleted_holiday_id": holiday_id}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

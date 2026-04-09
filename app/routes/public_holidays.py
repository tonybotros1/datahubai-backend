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


@router.get("/get_all_holidays")
async def get_public_holidays(data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        results = await public_holidays_collection.find({"company_id": company_id}, {
            "_id": 1,
            "name": 1,
            "date": 1,
        }).to_list(None)
        for item in results:
            item["_id"] = str(item["_id"])

        return {"holidays": results}

    except Exception as e:
        print(e)

        raise HTTPException(status_code=500, detail=str(e))


@router.post("/add_new_holiday")
async def add_new_holiday(holiday: PublicHolidaysModel, data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        holiday = holiday.model_dump(exclude_unset=True)
        holiday['company_id'] = company_id
        holiday['createdAt'] = security.now_utc()
        holiday['updatedAt'] = security.now_utc()
        new_element = await public_holidays_collection.insert_one(holiday)
        holiday['_id'] = str(new_element.inserted_id)
        holiday['company_id'] = str(company_id)
        added_element = jsonable_encoder(holiday)

        return {"added_holiday": added_element}

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

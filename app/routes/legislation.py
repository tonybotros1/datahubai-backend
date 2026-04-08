from typing import Optional
from bson import ObjectId
from fastapi import APIRouter, HTTPException, Depends
from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel
from app.core import security
from app.database import get_collection
from app.websocket_config import manager

router = APIRouter()
legislations_collection = get_collection("legislations")


class LegislationModel(BaseModel):
    name: Optional[str] = None


@router.get("/get_all_legislations")
async def get_all_legislations(data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        results = await legislations_collection.find({"company_id": company_id}, {
            "name": 1,
        }).to_list(None)
        return {
            "all_legislations": jsonable_encoder(
                results,
                custom_encoder={ObjectId: str}
            )
        }


    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=f"str{e}")


@router.post("/add_new_legislation")
async def add_new_legislation(leg: LegislationModel, data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        leg = leg.model_dump(exclude_unset=True)
        leg['company_id'] = company_id
        leg['createdAt'] = security.now_utc()
        leg['updatedAt'] = security.now_utc()
        new_leg = await legislations_collection.insert_one(leg)

        leg['_id'] = str(new_leg.inserted_id)
        leg['company_id'] = str(company_id)
        leg = jsonable_encoder(leg)  # 🔥 this fixes datetime + ObjectId

        await manager.send_to_company(str(company_id), {
            "type": "leg_added",
            "data": leg
        })
        return {"message": "added successfully!", "new_leg": leg}

    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=str(e))

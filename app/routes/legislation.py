from typing import Optional, Any, List
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
    weekend: Optional[List[str]] = None


class SearchModel(BaseModel):
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


@router.patch("/update_legislation/{leg_id}")
async def update_legislation(leg_id: str, leg: LegislationModel, data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        leg = leg.model_dump(exclude_unset=True)
        leg['updatedAt'] = security.now_utc()
        updated_leg = await legislations_collection.update_one({"_id": ObjectId(leg_id)}, {"$set": leg})

        if updated_leg.matched_count == 0:
            raise HTTPException(status_code=404, detail="legislation not found")
        leg['_id'] = str(leg_id)
        leg = jsonable_encoder(leg)

        await manager.send_to_company(str(company_id), {
            "type": "leg_updated",
            "data": leg
        })
        return {"message": "updated successfully!", "updated_leg": leg}

    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/delete_legislation/{leg_id}")
async def delete_legislation(leg_id: str, data: dict = Depends(security.get_current_user)):
    try:
        company_id = data.get("company_id")
        result = await legislations_collection.delete_one({"_id": ObjectId(leg_id)})
        if result.deleted_count == 1:
            await manager.send_to_company(str(company_id), {
                "type": "leg_deleted",
                "data": {"_id": leg_id}
            })
            return {"message": "Element removed successfully!"}
        else:
            raise HTTPException(status_code=404, detail="Branch not found")

    except Exception as error:
        return {"message": str(error)}


@router.post("/search_engine_for_legislations")
async def search_engine_for_legislations(
        filters: SearchModel,
        data: dict = Depends(security.get_current_user)
):
    try:
        company_id = ObjectId(data.get("company_id"))
        match_stage: Any = {}
        if company_id:
            match_stage["company_id"] = company_id
        if filters.name:
            match_stage["name"] = {"$regex": filters.name, "$options": "i"}

        legislations_elements_pipeline = [
            {"$match": match_stage},
            {"$set": {
                "_id": {
                    "$toString": "$_id"
                }
            }},
            {"$project": {
                "company_id": 0,

            }}
        ]
        cursor = await legislations_collection.aggregate(legislations_elements_pipeline)
        legislations_elements = await cursor.to_list(None)
        print(legislations_elements)
        return {"legislations_elements": legislations_elements if legislations_elements else []}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

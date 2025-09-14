from bson import ObjectId
from fastapi import APIRouter, Body, HTTPException, Depends
from pymongo import ReturnDocument
from app.core import security
from app.database import get_collection
from datetime import datetime, timezone
from app.websocket_config import manager

router = APIRouter()
branches_collection = get_collection("branches")


def serializer(branch: dict) -> dict:
    branch["_id"] = str(branch["_id"])
    branch['country_id'] = str(branch['country_id'])
    branch['city_id'] = str(branch['city_id'])
    for key, value in branch.items():
        if isinstance(value, datetime):
            branch[key] = value.isoformat()
    return branch


@router.get("/get_all_branches")
async def get_all_branches(data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        pipeline = [
            {"$match": {"company_id": company_id}},
            {
                "$lookup": {
                    "from": "all_countries",
                    "localField": "country_id",
                    "foreignField": "_id",
                    "as": "country",
                }
            },
            {
                "$unwind": {
                    "path": "$country",
                    "preserveNullAndEmptyArrays": True
                }
            },
            {
                "$lookup": {
                    "from": "all_countries_cities",
                    "localField": "city_id",
                    "foreignField": "_id",
                    "as": "city",
                }
            },
            {
                "$unwind": {
                    "path": "$city",
                    "preserveNullAndEmptyArrays": True
                }
            },
            {
                "$group": {
                    "_id": "$_id",
                    "name": {"$first": "$name"},
                    "code": {"$first": "$code"},
                    "line": {"$first": "$line"},
                    "status": {"$first": "$status"},
                    "country": {"$first": "$country.name"},
                    "country_id": {"$first": "$country_id"},
                    "city": {"$first": "$city.name"},
                    "city_id": {"$first": "$city_id"},
                    "createdAt": {"$first": "$createdAt"},
                    "updatedAt": {"$first": "$updatedAt"},
                }
            }
        ]
        cursor = await branches_collection.aggregate(pipeline)
        results = await cursor.to_list()
        if not results:
            raise HTTPException(status_code=404, detail="Branch not found")
        return {"branches": [serializer(result) for result in results]}


    except Exception as e:
        return {"message": str(e)}


@router.post("/add_new_branch")
async def add_new_branch(name: str = Body(None), code: str = Body(None), line: str = Body(None),
                         country_id: str = Body(None), city_id: str = Body(None),
                         data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        branch_dic = {
            "name": name,
            "code": code,
            "line": line,
            "status": True,
            "company_id": company_id,
            "country_id": ObjectId(country_id),
            "city_id": ObjectId(city_id),
            "createdAt": datetime.now(timezone.utc),
            "updatedAt": datetime.now(timezone.utc),
        }
        result = await branches_collection.insert_one(branch_dic)
        branch_dic["_id"] = str(result.inserted_id)
        serialized = serializer(branch_dic)
        await manager.broadcast({
            "type": "branch_added",
            "data": serialized
        })

    except Exception as e:
        return {"message": str(e)}

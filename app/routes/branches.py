from typing import Optional

from bson import ObjectId
from fastapi import APIRouter, Body, HTTPException, Depends
from pymongo import ReturnDocument
from app.core import security
from app.database import get_collection
from datetime import datetime, timezone
from app.websocket_config import manager

router = APIRouter()
branches_collection = get_collection("branches")
users_collection = get_collection("sys-users")


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


async def get_branch_details(branch_id: ObjectId):
    pipeline = [
        {"$match": {"_id": branch_id}},
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
    results = await cursor.next()
    if not results:
        raise HTTPException(status_code=404, detail="Branch not found")

    return results


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
        serialized = [serializer(result) for result in results]
        return {"branches": serialized}


    except Exception as e:
        return {"message": str(e)}


@router.post("/add_new_branch")
async def add_new_branch(name: str = Body(None), code: Optional[str] = Body(None), line: Optional[str] = Body(None),
                         country_id: Optional[str] = Body(None), city_id: Optional[str] = Body(None),
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
        new_branch = await get_branch_details(result.inserted_id)
        serialized = serializer(new_branch)
        await manager.broadcast({
            "type": "branch_added",
            "data": serialized
        })
        return {"message": "Branch added successfully!", "branch": serialized}


    except Exception as e:
        return {"message": str(e)}


@router.patch("/update_branch/{branch_id}")
async def update_branch(branch_id: str, name: str = Body(None), code: str = Body(None), line: str = Body(None),
                        country_id: str = Body(None), city_id: str = Body(None),
                        _: dict = Depends(security.get_current_user)):
    try:
        branch_id = ObjectId(branch_id)
        result = await branches_collection.update_one(
            {"_id": branch_id},
            {"$set": {"name": name, "code": code, "line": line, "country_id": ObjectId(country_id),
                      "city_id": ObjectId(city_id),
                      "updatedAt": datetime.now(timezone.utc), }},
        )
        if not result:
            raise HTTPException(status_code=404, detail="Branch not found")

        updated_branch = await get_branch_details(branch_id)
        serialized = serializer(updated_branch)

        await manager.broadcast({
            "type": "branch_updated",
            "data": serialized
        })

    except Exception as e:
        return {"message": str(e)}


@router.patch("/change_branch_status/{branch_id}")
async def change_branch_status(branch_id: str, status: bool = Body(None), _: dict = Depends(security.get_current_user)):
    try:
        result = await branches_collection.find_one_and_update(
            {"_id": ObjectId(branch_id)}, {"$set": {"status": status, "updatedAt": datetime.now(timezone.utc), }},
            return_document=ReturnDocument.AFTER
        )
        if not result:
            raise HTTPException(status_code=404, detail="Branch not found")
        await manager.broadcast({
            "type": "branch_status_updated",
            "data": {"status": status, "_id": branch_id}
        })
    except Exception as error:
        return {"message": str(error)}


@router.delete("/delete_branch/{branch_id}")
async def delete_branch(branch_id: str, _: dict = Depends(security.get_current_user)):
    try:
        result = await branches_collection.delete_one({"_id": ObjectId(branch_id)})
        if result.deleted_count == 1:
            await manager.broadcast({
                "type": "branch_deleted",
                "data": {"_id": branch_id}
            })
            return {"message": "Branch removed successfully!"}
        else:
            raise HTTPException(status_code=404, detail="Branch not found")

    except Exception as error:
        return {"message": str(error)}


@router.get("/get_all_branches_by_status")
async def get_all_branches_by_status(data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        results = await branches_collection.find({"status": True, "company_id": company_id}).sort("name", 1).to_list(
            None)
        return {"branches": [serializer(result) for result in results]}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/get_all_customer_branches")
async def get_all_customer_branches(data: dict = Depends(security.get_current_user)):
    try:
        user_id = ObjectId(data.get("sub"))
        customer_branches = [
            {
                '$match': {
                    '_id': user_id
                }
            }, {
                '$lookup': {
                    'from': 'branches',
                    'localField': 'branches',
                    'foreignField': '_id',
                    'as': 'customer_branches'
                }
            }, {
                '$project': {
                    'customer_branches': 1,
                    "_id": 0,
                }
            }
        ]
        cursor = await  users_collection.aggregate(customer_branches)
        result = await cursor.next()
        return {"customer_branches": [serializer(r) for r in result['customer_branches']]}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

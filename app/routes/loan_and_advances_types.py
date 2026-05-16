import copy
from typing import Optional, Any
from bson import ObjectId
from fastapi import APIRouter, HTTPException, Depends
from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel
from app.core import security
from app.database import get_collection
from app.routes.car_trading import PyObjectId
from app.websocket_config import manager

router = APIRouter()
loan_and_advances_types_collection = get_collection("loan_and_advances_types")

loan_and_advances_types_pipeline = [
    {
        '$lookup': {
            'from': 'payroll_elements',
            'localField': 'based_element',
            'foreignField': '_id',
            'as': 'element_details'
        }
    }, {
        '$addFields': {
            '_id': {
                '$toString': '$_id'
            },
            'based_element': {
                '$toString': '$based_element'
            },
            'company_id': {
                '$toString': '$company_id'
            },
            'based_element_name': {
                '$ifNull': [
                    {
                        '$first': '$element_details.name'
                    }, None
                ]
            }
        }
    }, {
        '$project': {
            'element_details': 0
        }
    }
]


class LoanAndAdvancesTypesModel(BaseModel):
    name: Optional[str] = None
    code: Optional[str] = None
    type: Optional[str] = None
    based_element: Optional[str] = None
    number_of_days: Optional[int] = None


class SearchModel(BaseModel):
    name: Optional[str] = None
    code: Optional[str] = None
    based_element: Optional[PyObjectId] = None


async def get_loan_and_advance_typs_details(type_id: ObjectId):
    try:
        new_pipeline: Any = copy.deepcopy(loan_and_advances_types_pipeline)
        new_pipeline.insert(0, {
            "$match": {
                "_id": type_id
            }
        })

        cursor = await loan_and_advances_types_collection.aggregate(new_pipeline)
        result = await cursor.to_list(None)
        return result[0] if result else None

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/add_new_loan_and_advances_type")
async def add_new_loan_and_advances_type(loan_and_advance_type: LoanAndAdvancesTypesModel, data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        loan_and_advance_type = loan_and_advance_type.model_dump(exclude_unset=True)
        if "based_element" in loan_and_advance_type:
            if loan_and_advance_type["based_element"]:
                loan_and_advance_type['based_element'] = ObjectId(loan_and_advance_type['based_element'])
        loan_and_advance_type['company_id'] = company_id
        loan_and_advance_type['createdAt'] = security.now_utc()
        loan_and_advance_type['updatedAt'] = security.now_utc()
        added_type = await loan_and_advances_types_collection.insert_one(loan_and_advance_type)
        if not added_type.inserted_id:
            raise HTTPException(status_code=404, detail="Failed to add new type")
        loan_and_advance_type['_id'] = str(added_type.inserted_id)
        loan_and_advance_type.pop("company_id")
        added_type_details = await get_loan_and_advance_typs_details(added_type.inserted_id)
        await manager.send_to_company(str(company_id), {
            "type": "loan_and_advances_type_added",
            "data": jsonable_encoder(added_type_details)
        })

    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=str(e))


@router.patch("/update_loan_and_advances_type/{type_id}")
async def update_loan_and_advances_type(type_id: str, loan_and_advance_type: LoanAndAdvancesTypesModel, data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        loan_and_advance_type = loan_and_advance_type.model_dump(exclude_unset=True)
        if "based_element" in loan_and_advance_type:
            if loan_and_advance_type["based_element"]:
                loan_and_advance_type['based_element'] = ObjectId(loan_and_advance_type['based_element'])
        loan_and_advance_type['updatedAt'] = security.now_utc()
        updated_type = await loan_and_advances_types_collection.update_one({"_id": ObjectId(type_id)}, {"$set": loan_and_advance_type})
        if updated_type.matched_count == 0:
            raise HTTPException(status_code=404, detail="Failed to update type")
        updated_type_details = await get_loan_and_advance_typs_details(ObjectId(type_id))
        await manager.send_to_company(str(company_id), {
            "type": "loan_and_advances_type_updated",
            "data": jsonable_encoder(updated_type_details)
        })

    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/delete_loan_and_advances_type/{type_id}")
async def delete_loan_and_advances_type(
        type_id: str,
        data: dict = Depends(security.get_current_user)
):
    try:
        company_id = ObjectId(data.get("company_id"))
        try:
            obj_id = ObjectId(type_id)
        except:
            raise HTTPException(status_code=400, detail="Invalid type_id")
        result = await loan_and_advances_types_collection.delete_one({
            "_id": obj_id,
            "company_id": company_id
        })
        if result.deleted_count == 0:
            raise HTTPException(status_code=404, detail="type not found")

        await manager.send_to_company(str(company_id), {
            "type": "loan_and_advances_type_deleted",
            "data": {"_id": type_id}
        })
        return {
            "message": "type deleted successfully",
            "_id": type_id
        }
    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/search_engine_for_loan_and_advances_types")
async def search_engine_for_loan_and_advances_types(
        filters: SearchModel,
        data: dict = Depends(security.get_current_user)
):
    try:
        company_id = ObjectId(data.get("company_id"))
        match_stage: Any = {}
        print(filters)
        if company_id:
            match_stage["company_id"] = company_id
        if filters.code:
            match_stage["code"] = filters.code
        if filters.name:
            match_stage["name"] = {"$regex": filters.name, "$options": "i"}
        if filters.based_element:
            match_stage["based_element"] = filters.based_element

        types_pipeline_for_search = copy.deepcopy(loan_and_advances_types_pipeline)
        types_pipeline_for_search.insert(0, {"$match": match_stage})

        cursor = await loan_and_advances_types_collection.aggregate(types_pipeline_for_search)
        types = await cursor.to_list(None)
        return {"loan_and_advances_types": types if types else []}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/get_all_loan_and_advances_types_for_lov")
async def get_all_loan_and_advances_types_for_lov(data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        results = await loan_and_advances_types_collection.find({"company_id": company_id} ,{
            "name": 1,
            "type": 1
        }).to_list(None)
        for loan_and_advance_type in results:
            loan_and_advance_type["_id"] = str(loan_and_advance_type["_id"])

        return {"loan_and_advances_types": results if results else []}


    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=str(e))

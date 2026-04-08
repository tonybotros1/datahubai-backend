from typing import Optional, Any
from bson import ObjectId
from fastapi import APIRouter, HTTPException, Depends
from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel
from app.core import security
from app.database import get_collection
from app.websocket_config import manager

router = APIRouter()
payroll_elements_collection = get_collection("payroll_elements")


class PayrollElementsModel(BaseModel):
    key: Optional[str] = None
    name: Optional[str] = None
    type: Optional[str] = None
    priority: Optional[str] = None
    comments: Optional[str] = None
    is_allow_override: Optional[bool] = None
    is_recurring: Optional[bool] = None
    is_entry_value: Optional[bool] = None
    is_standard_link: Optional[bool] = None


class SearchModel(BaseModel):
    key: Optional[str] = None
    name: Optional[str] = None
    type: Optional[str] = None
    priority: Optional[str] = None
    comments: Optional[str] = None


@router.post("/add_new_payroll_element")
async def add_new_payroll_element(payroll_element: PayrollElementsModel,
                                  data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        payroll_element = payroll_element.model_dump(exclude_unset=True)
        payroll_element['company_id'] = company_id
        payroll_element['createdAt'] = security.now_utc()
        payroll_element['updatedAt'] = security.now_utc()
        new_element = await payroll_elements_collection.insert_one(payroll_element)
        payroll_element['_id'] = str(new_element.inserted_id)
        payroll_element['company_id'] = str(company_id)
        added_element = jsonable_encoder(payroll_element)

        await manager.send_to_company(str(company_id), {
            "type": "payroll_element_added",
            "data": added_element
        })
        return {"message": "added successfully!", "added_element": added_element}


    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.patch("/update_payroll_element/{element_id}")
async def add_new_payroll_element(element_id: str, payroll_element: PayrollElementsModel,
                                  data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        payroll_element = payroll_element.model_dump(exclude_unset=True)
        payroll_element['updatedAt'] = security.now_utc()
        new_element = await payroll_elements_collection.update_one({"_id": ObjectId(element_id)},
                                                                   {"$set": payroll_element})
        payroll_element['_id'] = str(new_element.inserted_id)
        payroll_element['company_id'] = str(company_id)
        added_element = jsonable_encoder(payroll_element)

        await manager.send_to_company(str(company_id), {
            "type": "payroll_element_updated",
            "data": added_element
        })
        return {"message": "updated successfully!", "updated_element": added_element}


    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/delete_payroll_element/{element_id}")
async def delete_branch(element_id: str, data: dict = Depends(security.get_current_user)):
    try:
        company_id = data.get("company_id")
        result = await payroll_elements_collection.delete_one({"_id": ObjectId(element_id)})
        if result.deleted_count == 1:
            await manager.send_to_company(str(company_id), {
                "type": "payroll_element_deleted",
                "data": {"_id": element_id}
            })
            return {"message": "Element removed successfully!"}
        else:
            raise HTTPException(status_code=404, detail="Branch not found")

    except Exception as error:
        return {"message": str(error)}


@router.post("/search_engine_for_payroll_elements")
async def search_engine_for_payroll_elements(
        filters: SearchModel,
        data: dict = Depends(security.get_current_user)
):
    try:
        company_id = ObjectId(data.get("company_id"))
        match_stage: Any = {}
        if company_id:
            match_stage["company_id"] = company_id
        if filters.key:
            match_stage["key"] = filters.key
        if filters.name:
            match_stage["name"] = {"$regex": filters.name, "$options": "i"}
        if filters.type:
            match_stage["type"] = filters.type
        if filters.priority:
            match_stage["priority"] = filters.priority
        if filters.comments:
            match_stage["comments"] = {"$regex": filters.comments, "$options": "i"}

        payroll_elements_pipeline = [
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
        cursor = await payroll_elements_collection.aggregate(payroll_elements_pipeline)
        payroll_elements = await cursor.to_list(None)
        return {"payroll_elements": payroll_elements if payroll_elements else []}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/get_payroll_elements_for_lov")
async def get_payroll_elements_for_lov(data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        results = await payroll_elements_collection.find({"company_id": company_id}, {"_id": 1, "name": 1}).to_list(
            None)
        return {
            "elements": jsonable_encoder(
                results,
                custom_encoder={ObjectId: str}
            )
        }


    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=str(e))

import copy
from typing import Optional, Any
from bson import ObjectId
from fastapi import APIRouter, HTTPException, Depends, Body
from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel
from app.core import security
from app.database import get_collection
from app.websocket_config import manager

router = APIRouter()
payroll_elements_collection = get_collection("payroll_elements")
payroll_elements_based_elements_collection = get_collection("payroll_elements_based_elements")


class PayrollElementsModel(BaseModel):
    key: Optional[str] = None
    name: Optional[str] = None
    type: Optional[str] = None
    priority: Optional[str] = None
    comments: Optional[str] = None
    function: Optional[str] = None
    is_allow_override: Optional[bool] = None
    is_recurring: Optional[bool] = None
    is_entry_value: Optional[bool] = None
    is_standard_link: Optional[bool] = None
    is_indirect: Optional[bool] = None


class SearchModel(BaseModel):
    key: Optional[str] = None
    name: Optional[str] = None
    type: Optional[str] = None
    priority: Optional[str] = None
    comments: Optional[str] = None


class BasedElementsModel(BaseModel):
    name: Optional[str] = None
    type: Optional[str] = None


payroll_element_details_pipeline = [
    {
        '$lookup': {
            'from': 'payroll_elements_based_elements',
            'let': {
                'payroll_element_id': '$_id'
            },
            'pipeline': [
                {
                    '$match': {
                        '$expr': {
                            '$eq': [
                                '$payroll_element_id', '$$payroll_element_id'
                            ]
                        }
                    }
                }, {
                    '$lookup': {
                        'from': 'payroll_elements',
                        'localField': 'name',
                        'foreignField': '_id',
                        'as': 'element_details'
                    }
                }, {
                    '$addFields': {
                        '_id': {
                            '$toString': '$_id'
                        },
                        'name': {
                            '$toString': '$name'
                        },
                        'name_value': {
                            '$ifNull': [
                                {
                                    '$first': '$element_details.name'
                                }, None
                            ]
                        }
                    }
                }, {
                    '$project': {
                        '_id': 1,
                        'name_value': 1,
                        'name': 1,
                        'type': 1
                    }
                }
            ],
            'as': 'element_details'
        }
    }, {
        '$addFields': {
            '_id': {
                '$toString': '$_id'
            }
        }
    }, {
        '$project': {
            'company_id': 0
        }
    }
]

based_element_details_pipeline = [
    {
        '$lookup': {
            'from': 'payroll_elements',
            'localField': 'payroll_element_id',
            'foreignField': '_id',
            'as': 'element_details'
        }
    }, {
        '$addFields': {
            '_id': {
                '$toString': '$_id'
            },
            'name': {
                '$toString': '$name'
            },
            'name_value': {
                '$ifNull': [
                    {
                        '$first': '$element_details.name'
                    }, None
                ]
            }
        }
    }, {
        '$project': {
            '_id': 1,
            'name_value': 1,
            'name': 1,
            'type': 1
        }
    }
]


@router.get("/get_payroll_element_details/{element_id}")
async def get_payroll_element_details(element_id: str, _: dict = Depends(security.get_current_user)):
    try:
        element_id = ObjectId(element_id)
        new_pipeline: Any = copy.deepcopy(payroll_element_details_pipeline)
        new_pipeline.insert(0,
                            {
                                '$match': {
                                    '_id': element_id
                                }

                            })
        cursor = await payroll_elements_collection.aggregate(new_pipeline)
        result = await cursor.to_list(None)
        return {"details": result[0] if result else None}
    except Exception:
        raise


#
# @router.get("/get_payroll_element_based_element_details/{element_id}")
# async def get_payroll_element_based_element_details(element_id: str, _: dict = Depends(security.get_current_user)):
#     try:
#         element_id = ObjectId(element_id)
#         new_pipeline: Any = copy.deepcopy(payroll_element_details_pipeline)
#         new_pipeline.insert(0, {
#             {
#                 '$match': {
#                     '_id': element_id
#                 }
#             },
#         })
#         cursor = await payroll_elements_collection.aggregate()
#         result = await cursor.to_list(None)
#         return {"details": result[0] if result else None}
#     except Exception:
#         raise


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
async def update_payroll_element(element_id: str, payroll_element: PayrollElementsModel,
                                 data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        payroll_element = payroll_element.model_dump(exclude_unset=True)
        payroll_element['updatedAt'] = security.now_utc()
        updated_element = await payroll_elements_collection.update_one({"_id": ObjectId(element_id)},
                                                                       {"$set": payroll_element})
        payroll_element['_id'] = str(element_id)
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

            }},
            {
                "$sort": {
                    "priority": 1
                }
            }
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
        results = await payroll_elements_collection.find({"company_id": company_id, "is_indirect": False},
                                                         {"_id": 1, "name": 1}).sort({"name": 1}).to_list(
            None)
        return {
            "elements": jsonable_encoder(
                results,
                custom_encoder={ObjectId: str}
            )
        }


    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


class PayrollElementsForLOVForPayrollElementsModel(BaseModel):
    element_id: Optional[str] = None


@router.post("/get_payroll_elements_for_lov_for_payroll_elements")
async def get_payroll_elements_for_lov_for_payroll_elements(
        element_data: PayrollElementsForLOVForPayrollElementsModel,
        data: dict = Depends(security.get_current_user)
):
    try:
        company_id = ObjectId(data.get("company_id"))

        match_filter: Any = {"company_id": company_id}
        if element_data.element_id:
            match_filter["_id"] = {"$ne": ObjectId(element_data.element_id)}

        result_pipeline = [
            {"$match": match_filter},
            {"$project": {"_id": 1, "name": 1}},
            {"$set": {"_id": {"$toString": "$_id"}}},
            {"$sort": {"name": 1}},
        ]

        cursor = await payroll_elements_collection.aggregate(result_pipeline)
        results = await cursor.to_list(None)

        return {
            "elements": jsonable_encoder(results, custom_encoder={ObjectId: str})
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/add_new_based_element/{payroll_element_id}")
async def add_new_based_element(payroll_element_id: str, element: BasedElementsModel,
                                data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        payroll_element_id = ObjectId(payroll_element_id)
        element_dict = {
            "payroll_element_id": payroll_element_id,
            "company_id": company_id,
            "name": ObjectId(element.name) if element.name else None,
            "type": element.type,
            "createdAt": security.now_utc(),
            "updatedAt": security.now_utc(),
        }
        result = await payroll_elements_based_elements_collection.insert_one(element_dict)
        if not result.inserted_id:
            raise HTTPException(status_code=500, detail="Failed creating new element")
        return {"added_based_element_id": str(result.inserted_id)}

    except Exception:
        raise


@router.patch("/update_based_element/{element_id}")
async def update_based_element(element_id: str, element: BasedElementsModel,
                               _: dict = Depends(security.get_current_user)):
    try:
        element_id = ObjectId(element_id)
        element_dict = {
            "name": ObjectId(element.name) if element.name else None,
            "type": element.type,
            "updatedAt": security.now_utc(),
        }
        result = await payroll_elements_based_elements_collection.update_one({"_id": element_id},
                                                                             {"$set": element_dict})
        if result.matched_count == 0:
            raise HTTPException(status_code=500, detail="Failed updating element")
        # return {"updated_based_element_id": str(result.inserted_id)}

    except Exception:
        raise


@router.delete("/delete_based_element/{element_id}")
async def delete_based_element(element_id: str, _: dict = Depends(security.get_current_user)):
    try:
        result = await payroll_elements_based_elements_collection.delete_one({"_id": ObjectId(element_id)})
        if result.deleted_count == 0:
            raise HTTPException(status_code=500, detail="Failed deleting element")
    except Exception as error:
        return {"message": str(error)}

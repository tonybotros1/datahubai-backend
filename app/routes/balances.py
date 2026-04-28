import copy
from typing import Optional, Any
from bson import ObjectId
from fastapi import APIRouter, HTTPException, Depends
from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel
from app.core import security
from app.database import get_collection
from app.websocket_config import manager

router = APIRouter()
balances_collection = get_collection("balances")
balances_based_elements_collection = get_collection("balances_based_elements")


class SearchModel(BaseModel):
    name: Optional[str] = None
    type: Optional[str] = None


class BasedElementsModel(BaseModel):
    name: Optional[str] = None
    type: Optional[str] = None


class BalanceModel(BaseModel):
    name: Optional[str] = None
    type: Optional[str] = None
    description: Optional[str] = None
    show_on_payroll: Optional[bool] = None
    show_on_assignment: Optional[bool] = None
    show_on_leave: Optional[bool] = None


balance_details_pipeline = [
    {
        '$lookup': {
            'from': 'balances_based_elements',
            'let': {
                'balance_id': '$_id'
            },
            'pipeline': [
                {
                    '$match': {
                        '$expr': {
                            '$eq': [
                                '$balance_id', '$$balance_id'
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


@router.post("/add_new_balance")
async def add_new_balance(balance: BalanceModel,
                          data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        balance = balance.model_dump(exclude_unset=True)
        balance['company_id'] = company_id
        balance['createdAt'] = security.now_utc()
        balance['updatedAt'] = security.now_utc()
        new_balance = await balances_collection.insert_one(balance)
        balance['_id'] = str(new_balance.inserted_id)
        balance['company_id'] = str(company_id)
        added_balance = jsonable_encoder(balance)

        await manager.send_to_company(str(company_id), {
            "type": "hr_balance_added",
            "data": added_balance
        })
        return {"message": "added successfully!", "added_balance_id": str(new_balance.inserted_id)}


    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.patch("/update_balance/{balance_id}")
async def update_balance(balance_id: str, balance: BalanceModel,
                         data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        balance = balance.model_dump(exclude_unset=True)
        balance['updatedAt'] = security.now_utc()
        await balances_collection.update_one({"_id": ObjectId(balance_id)},
                                             {"$set": balance})
        balance['_id'] = str(balance_id)
        balance['company_id'] = str(company_id)
        added_element = jsonable_encoder(balance)

        await manager.send_to_company(str(company_id), {
            "type": "hr_balance_updated",
            "data": added_element
        })
        return {"message": "updated successfully!", "updated_element": added_element}


    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/delete_balance/{balance_id}")
async def delete_balance(balance_id: str, data: dict = Depends(security.get_current_user)):
    try:
        company_id = data.get("company_id")
        result = await balances_collection.delete_one({"_id": ObjectId(balance_id)})
        if result.deleted_count == 1:
            await manager.send_to_company(str(company_id), {
                "type": "hr_balance_deleted",
                "data": {"_id": balance_id}
            })
            return {"message": "Balance removed successfully!"}
        else:
            raise HTTPException(status_code=404, detail="Balance not found")

    except Exception as error:
        return {"message": str(error)}


@router.post("/search_engine_for_balance")
async def search_engine_for_balance(
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
        if filters.type:
            match_stage["type"] = filters.type

        balances_pipeline = [
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
                    "name": 1
                }
            }
        ]
        cursor = await balances_collection.aggregate(balances_pipeline)
        all_balances = await cursor.to_list(None)
        print(all_balances)
        return {"balances": all_balances if all_balances else []}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/add_new_based_element/{balance_id}")
async def add_new_based_element(balance_id: str, element: BasedElementsModel,
                                data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        balance_id = ObjectId(balance_id)
        element_dict = {
            "balance_id": balance_id,
            "company_id": company_id,
            "name": ObjectId(element.name) if element.name else None,
            "type": element.type,
            "createdAt": security.now_utc(),
            "updatedAt": security.now_utc(),
        }
        result = await balances_based_elements_collection.insert_one(element_dict)
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
        result = await balances_based_elements_collection.update_one({"_id": element_id},
                                                                     {"$set": element_dict})
        if result.matched_count == 0:
            raise HTTPException(status_code=500, detail="Failed updating element")
        # return {"updated_based_element_id": str(result.inserted_id)}

    except Exception:
        raise


@router.delete("/delete_based_element/{element_id}")
async def delete_based_element(element_id: str, _: dict = Depends(security.get_current_user)):
    try:
        result = await balances_based_elements_collection.delete_one({"_id": ObjectId(element_id)})
        if result.deleted_count == 0:
            raise HTTPException(status_code=500, detail="Failed deleting element")
    except Exception as error:
        return {"message": str(error)}


@router.get("/get_balance_details/{balance_id}")
async def get_balance_details(balance_id: str, _: dict = Depends(security.get_current_user)):
    try:
        balance_id = ObjectId(balance_id)
        new_pipeline: Any = copy.deepcopy(balance_details_pipeline)
        new_pipeline.insert(0,
                            {
                                '$match': {
                                    '_id': balance_id
                                }

                            })
        cursor = await balances_collection.aggregate(new_pipeline)
        result = await cursor.to_list(None)
        return {"details": result[0] if result else None}
    except Exception:
        raise

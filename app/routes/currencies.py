from typing import Optional, Dict, Any
from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException, Body
from pydantic import BaseModel

from app.core import security
from app.database import get_collection
from datetime import datetime
from app.websocket_config import manager

router = APIRouter()
currencies_collection = get_collection("currencies")


def serializer(currency: dict) -> dict:
    currency["_id"] = str(currency["_id"])
    currency["country_id"] = str(currency["country_id"])
    for key, value in currency.items():
        if isinstance(value, datetime):
            currency[key] = value.isoformat()
    return currency


class Currencies(BaseModel):
    country_id: Optional[str] = None
    rate: Optional[float] = None


pipeline: list[Dict[str, Any]] = [

    {
        '$lookup': {
            'from': 'all_countries',
            'let': {
                'country_id': '$country_id'
            },
            'pipeline': [
                {
                    '$match': {
                        '$expr': {
                            '$eq': [
                                '$_id', '$$country_id'
                            ]
                        }
                    }
                }, {
                    '$project': {
                        'name': 1,
                        'code': 1,
                        'currency_code': 1
                    }
                }
            ],
            'as': 'country'
        }
    }, {
        '$unwind': {
            'path': '$country',
            'preserveNullAndEmptyArrays': True
        }
    }, {
        '$project': {
            '_id': 1,
            'createdAt': 1,
            'updatedAt': 1,
            'status': 1,
            'rate': 1,
            'country_name': '$country.name',
            'country_code': '$country.code',
            'currency_code': '$country.currency_code',
            'country_id': 1
        }
    }
]


async def get_currency_details(currency_id: ObjectId):
    new_pipeline = pipeline.copy()
    new_pipeline.insert(0,
                        {
                            "$match": {
                                "_id": currency_id
                            }
                        }
                        )

    curser = await currencies_collection.aggregate(new_pipeline)
    result = await curser.to_list()
    return result[0]


@router.get("/get_all_currencies")
async def get_all_currencies(data: dict = Depends(security.get_current_user)):
    try:
        company_id = data.get("company_id")
        new_pipeline = pipeline.copy()
        new_pipeline.insert(0,
                            {
                                "$match": {
                                    "company_id": ObjectId(company_id)
                                }
                            }
                            )
        cursor = await currencies_collection.aggregate(new_pipeline)
        results = await cursor.to_list(None)
        return {"currencies": [serializer(c) for c in results]}

    except Exception as e:
        raise e


@router.post("/add_new_currency")
async def add_new_currency(currency: Currencies, data: dict = Depends(security.get_current_user)):
    try:
        currency = currency.model_dump(exclude_unset=True)
        company_id = ObjectId(data.get("company_id"))
        currency_dict = {
            "company_id": company_id,
            "country_id": ObjectId(currency["country_id"]),
            "rate": currency["rate"],
            "createdAt": security.now_utc(),
            "updatedAt": security.now_utc(),
            "status": True,
        }
        result = await currencies_collection.insert_one(currency_dict)
        new_currency = await get_currency_details(result.inserted_id)
        serialized = serializer(new_currency)
        await manager.broadcast({
            "type": "currency_created",
            "data": serialized
        })
    except Exception as e:
        print(e)
        raise e


@router.patch("/update_currency/{currency_id}")
async def update_currency(currency_id: str, currency: Currencies, _: dict = Depends(security.get_current_user)):
    try:
        currency = currency.model_dump(exclude_unset=True)
        currency["updatedAt"] = security.now_utc()
        currency["country_id"] = ObjectId(currency["country_id"])
        currency_id = ObjectId(currency_id)
        await currencies_collection.update_one(
            {"_id": currency_id}, {"$set": currency})
        updated_currency = await get_currency_details(currency_id)
        serialized = serializer(updated_currency)
        await manager.broadcast({
            "type": "currency_updated",
            "data": serialized
        })

    except HTTPException as e:
        print(e)
        raise e
    except Exception:
        raise HTTPException(status_code=500, detail="Internal server error")


@router.delete("/delete_currency/{currency_id}")
async def delete_currency(currency_id: str, _: dict = Depends(security.get_current_user)):
    try:
        result = await currencies_collection.delete_one({"_id": ObjectId(currency_id)})
        if result.deleted_count == 0:
            raise HTTPException(status_code=404, detail="Salesman not found")

        await manager.broadcast({
            "type": "currency_deleted",
            "data": {"_id": str(currency_id)}
        })
    except HTTPException as e:
        raise e
    except Exception:
        raise HTTPException(status_code=500, detail="Internal server error")


@router.patch("/change_currency_status/{currency_id}")
async def change_user_status(currency_id: str, status: bool = Body(None), _: dict = Depends(security.get_current_user)):
    try:
        await currencies_collection.update_one(
            {"_id": ObjectId(currency_id)}, {"$set": {"status": status, "updatedAt": security.now_utc()}},
        )

        await manager.broadcast({
            "type": "currency_status_updated",
            "data": {"status": status, "_id": currency_id}
        })
    except Exception as error:
        return {"message": str(error)}


@router.get("/get_all_currencies_for_drop_down_menu")
async def get_all_currencies_for_drop_down_menu(data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        new_pipeline = pipeline.copy()
        new_pipeline.insert(0, {
            "$match": {
                "company_id": company_id, "status": True
            }
        })
        cursor = await currencies_collection.aggregate(new_pipeline)
        results = await cursor.to_list(None)
        return {"currencies": [serializer(c) for c in results]}
    except Exception as e:
        raise e


@router.get("/get_currency_name_subunit_by_id/{currency_id}")
async def get_currency_name_subunit_by_id(currency_id: str, _: dict = Depends(security.get_current_user)):
    try:
        name_subunit_pipeline = [
            {
                '$match': {
                    '_id': ObjectId(currency_id)
                }
            }, {
                '$lookup': {
                    'from': 'all_countries',
                    'let': {
                        'country_id': '$country_id'
                    },
                    'pipeline': [
                        {
                            '$match': {
                                '$expr': {
                                    '$eq': [
                                        '$_id', '$$country_id'
                                    ]
                                }
                            }
                        }, {
                            '$project': {
                                'name': 1,
                                'code': 1,
                                'currency_name': 1,
                                'subunit_name': 1
                            }
                        }
                    ],
                    'as': 'country'
                }
            }, {
                '$unwind': {
                    'path': '$country',
                    'preserveNullAndEmptyArrays': True
                }
            }, {
                '$project': {
                    '_id': 0,
                    'country_name': '$country.name',
                    'country_code': '$country.code',
                    'currency_name': '$country.currency_name',
                    'subunit_name': "$country.subunit_name",
                }
            }
        ]
        cursor = await currencies_collection.aggregate(name_subunit_pipeline)
        result = await cursor.next()
        return {"name_subunit": result}

    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal server error")

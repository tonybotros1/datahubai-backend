from typing import Dict, Any, Optional
from bson import ObjectId
from fastapi import APIRouter, Depends
from pydantic import BaseModel
from app.core import security
from app.database import get_collection
from datetime import datetime
from app.routes.branches import serializer
from app.websocket_config import manager

router = APIRouter()
all_banks_collection = get_collection("all_banks")

pipeline: list[Dict[str, Any]] = [
    {
        '$lookup': {
            'from': 'all_lists_values',
            'let': {
                'type_id': '$account_type_id'
            },
            'pipeline': [
                {
                    '$match': {
                        '$expr': {
                            '$eq': [
                                '$_id', '$$type_id'
                            ]
                        }
                    }
                }, {
                    '$project': {
                        'value_id': '$_id',
                        'value_name': '$name'
                    }
                }
            ],
            'as': 'account_type_details'
        }
    }, {
        '$unwind': {
            'path': '$account_type_details',
            'preserveNullAndEmptyArrays': True
        }
    }, {
        '$lookup': {
            'from': 'currencies',
            'let': {
                'currency_id': '$currency_id'
            },
            'pipeline': [
                {
                    '$match': {
                        '$expr': {
                            '$and': [
                                {
                                    '$eq': [
                                        '$_id', '$$currency_id'
                                    ]
                                }, {
                                    '$eq': [
                                        '$status', True
                                    ]
                                }
                            ]
                        }
                    }
                }, {
                    '$project': {
                        'rate': 1,
                        'country_id': 1
                    }
                }
            ],
            'as': 'currency_details'
        }
    }, {
        '$unwind': {
            'path': '$currency_details',
            'preserveNullAndEmptyArrays': True
        }
    }, {
        '$lookup': {
            'from': 'all_countries',
            'let': {
                'country_id': '$currency_details.country_id'
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
            'as': 'country_details'
        }
    }, {
        '$unwind': {
            'path': '$country_details',
            'preserveNullAndEmptyArrays': True
        }
    }, {
        '$addFields': {
            'country_name': {
                '$ifNull': [
                    '$country_details.name', None
                ]
            },
            'country_code': {
                '$ifNull': [
                    '$country_details.code', None
                ]
            },
            'country_id': {
                '$ifNull': ["$country_details._id", None]
            },
            'rate': {
                '$ifNull': [
                    '$currency_details.rate', None
                ]
            },
            'currency': {
                '$ifNull': [
                    '$country_details.currency_code', None
                ]
            },
            'account_type_name': {
                '$ifNull': [
                    '$account_type_details.value_name', None
                ]
            }
        }
    }, {
        '$project': {
            'country_details': 0,
            'account_type_details': 0,
            'currency_details': 0,
            'company_id': 0
        }
    }
]


def serializer_doc(doc):
    doc["_id"] = str(doc["_id"])
    doc["account_type_id"] = str(doc["account_type_id"]) if doc["account_type_id"] else ""
    doc["currency_id"] = str(doc["currency_id"]) if doc["currency_id"] else ""
    doc['country_id'] = str(doc["country_id"]) if doc["country_id"] else ""
    for key, value in doc.items():
        if isinstance(value, datetime):
            doc[key] = value.isoformat()
    return doc


class BanksModel(BaseModel):
    account_name: Optional[str]
    account_number: Optional[str]
    currency_id: Optional[str]
    account_type_id: Optional[str]


async def get_bank_details(bank_id: ObjectId):
    new_pipeline = pipeline.copy()
    new_pipeline.insert(1, {
        "$match": {
            "_id": bank_id
        }
    })
    cursor = await all_banks_collection.aggregate(new_pipeline)
    result = await cursor.to_list(None)
    return result[0]


@router.get("/get_all_banks")
async def get_all_banks(data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        new_pipeline = pipeline.copy()
        new_pipeline.insert(1, {
            "$match": {
                "company_id": company_id
            }
        })
        cursor = await all_banks_collection.aggregate(new_pipeline)
        result = await cursor.to_list()
        return {"banks": [serializer_doc(r) for r in result]}

    except Exception as e:
        raise e


@router.post("/add_new_bank")
async def add_new_bank(bank: BanksModel, data: dict = Depends(security.get_current_user)):
    try:
        bank = bank.model_dump(exclude_unset=True)
        company_id = ObjectId(data.get("company_id"))

        bank_dict = {
            "company_id": company_id,
            "account_name": bank["account_name"],
            "account_number": bank["account_number"],
            "currency_id": ObjectId(bank["currency_id"]) if bank["currency_id"] else None,
            "account_type_id": ObjectId(bank["account_type_id"]) if bank["account_type_id"] else None,
            "createdAt": security.now_utc(),
            "updatedAt": security.now_utc(),
        }
        result = await all_banks_collection.insert_one(bank_dict)
        new_bank = await get_bank_details(result.inserted_id)
        serialized = serializer_doc(new_bank)
        await manager.broadcast({
            "type": "bank_created",
            "data": serialized
        })
    except Exception as e:
        print(e)
        raise e


@router.patch("/update_bank/{bank_id}")
async def update_bank(bank_id: str, bank: BanksModel, _: dict = Depends(security.get_current_user)):
    try:
        bank = bank.model_dump(exclude_unset=True)
        bank['currency_id'] = ObjectId(bank['currency_id']) if bank['currency_id'] else None
        bank['account_type_id'] = ObjectId(bank['account_type_id']) if bank['account_type_id'] else None
        bank['updatedAt'] = security.now_utc()

        await all_banks_collection.update_one(
            {"_id": ObjectId(bank_id)}, {"$set": bank}
        )
        new_bank = await get_bank_details(ObjectId(bank_id))
        serialized = serializer_doc(new_bank)
        await manager.broadcast({
            "type": "bank_updated",
            "data": serialized
        })
    except Exception as e:
        print(e)
        raise e


@router.delete("/delete_bank/{bank_id}")
async def delete_bank(bank_id: str, _: dict = Depends(security.get_current_user)):
    try:
        await all_banks_collection.delete_one({"_id": ObjectId(bank_id)})
        await manager.broadcast({
            "type": "bank_deleted",
            "data": {"_id": bank_id}
        })
    except Exception as e:
        raise e

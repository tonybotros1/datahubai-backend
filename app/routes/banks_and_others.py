from typing import Dict, Any

from bson import ObjectId
from fastapi import APIRouter, Body, HTTPException, Depends
from pymongo import ReturnDocument
from app.core import security
from app.database import get_collection
from datetime import datetime, timezone

from app.routes.branches import serializer
from app.websocket_config import manager

router = APIRouter()
all_banks_collection = get_collection("all_banks")

pipeline :  list[Dict[str, Any]]=[
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
    doc["account_type_id"] = str(doc["account_type_id"])
    doc["currency_id"] = str(doc["currency_id"])
    for key,value in doc.items():
        if isinstance(value, datetime):
            doc[key] = value.isoformat()
    return doc


@router.get("/get_all_banks")
async def get_all_banks(data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        new_pipeline = pipeline.copy()
        new_pipeline.insert(1,{
            "$match" :{
                "company_id": company_id
            }
        })
        cursor = await all_banks_collection.aggregate(new_pipeline)
        result = await cursor.to_list()
        return [serializer_doc(r) for r in result]

    except Exception as e:
        raise e
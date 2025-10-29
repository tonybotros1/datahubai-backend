import copy
from typing import Optional, List, Any
from bson import ObjectId
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from app import database
from app.core import security
from app.database import get_collection
from datetime import datetime, timezone, timedelta
from app.routes.car_trading import PyObjectId
from app.routes.counters import create_custom_counter

router = APIRouter()
ap_invoices_collection = get_collection("ap_invoices")
ap_invoices_items_collection = get_collection("ap_invoices_items")

pipeline: List[dict[str,Any]] = [
    {
        '$lookup': {
            'from': 'all_lists_values',
            'localField': 'invoice_type',
            'foreignField': '_id',
            'as': 'invoice_type_details'
        }
    }, {
        '$unwind': {
            'path': '$invoice_type_details',
            'preserveNullAndEmptyArrays': True
        }
    }, {
        '$lookup': {
            'from': 'entity_information',
            'localField': 'vendor',
            'foreignField': '_id',
            'as': 'vendor_details'
        }
    }, {
        '$unwind': {
            'path': '$vendor_details',
            'preserveNullAndEmptyArrays': True
        }
    }, {
        '$lookup': {
            'from': 'ap_invoices_items',
            'let': {
                'payment_id': '$_id'
            },
            'pipeline': [
                {
                    '$match': {
                        '$expr': {
                            '$eq': [
                                '$ap_invoice_id', '$$payment_id'
                            ]
                        }
                    }
                }, {
                    '$lookup': {
                        'from': 'ap_payment_types',
                        'localField': 'transaction_type',
                        'foreignField': '_id',
                        'as': 'transaction_type_details'
                    }
                }, {
                    '$unwind': {
                        'path': '$transaction_type_details',
                        'preserveNullAndEmptyArrays': True
                    }
                }, {
                    '$addFields': {
                        'transaction_type_name': {
                            '$ifNull': [
                                '$transaction_type_details.type', None
                            ]
                        }
                    }
                }, {
                    '$project': {
                        'transaction_type_details': 0
                    }
                }
            ],
            'as': 'items'
        }
    }, {
        '$addFields': {
            'vendor_name': {
                '$ifNull': [
                    '$vendor_details.entity_name', None
                ]
            },
            'invoice_type_name': {
                '$ifNull': [
                    '$invoice_type_details.name', None
                ]
            }
        }
    }, {
        '$project': {
            'vendor_details': 0,
            'invoice_type_details': 0
        }
    }
]

async def get_ap_invoice_details(invoice_id:ObjectId):
    try:
        new_pipeline = copy.deepcopy(pipeline)
        new_pipeline.insert(1,{
            "$match":{
                "_id":invoice_id
            }
        })
        cursor = await ap_invoices_collection.aggregate(new_pipeline)
        result = await cursor.next()
        return result

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))



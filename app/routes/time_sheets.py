import asyncio
from typing import Optional
from bson import ObjectId
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from pymongo import ReturnDocument
from starlette.websockets import WebSocket

from app.core import security
from app.database import get_collection
from datetime import datetime
from app.websocket_config import manager
from bson.json_util import dumps

router = APIRouter()
time_sheets_collection = get_collection("time_sheets")
job_cards_collection = get_collection("job_cards")


def serializer(doc):
    doc['_id'] = str(doc['_id'])
    for key, value in doc.items():
        if isinstance(value, datetime):
            doc[key] = value.isoformat()
    return doc


@router.get("/get_approval_jobs")
async def get_approval_jobs(data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get('company_id'))
        pipeline = [
            {
                '$match': {
                    'company_id': company_id,
                    'job_status_2': 'Approved'
                }
            }, {
                '$lookup': {
                    'from': 'all_brands',
                    'let': {
                        'brand_id': '$car_brand'
                    },
                    'pipeline': [
                        {
                            '$match': {
                                '$expr': {
                                    '$eq': [
                                        '$_id', '$$brand_id'
                                    ]
                                }
                            }
                        }, {
                            '$project': {
                                'name': 1
                            }
                        }
                    ],
                    'as': 'brand_details'
                }
            }, {
                '$unwind': {
                    'path': '$brand_details',
                    'preserveNullAndEmptyArrays': True
                }
            }, {
                '$lookup': {
                    'from': 'all_brand_models',
                    'let': {
                        'model_id': '$car_model'
                    },
                    'pipeline': [
                        {
                            '$match': {
                                '$expr': {
                                    '$eq': [
                                        '$_id', '$$model_id'
                                    ]
                                }
                            }
                        }, {
                            '$project': {
                                'name': 1
                            }
                        }
                    ],
                    'as': 'model_details'
                }
            }, {
                '$unwind': {
                    'path': '$model_details',
                    'preserveNullAndEmptyArrays': True
                }
            }, {
                '$lookup': {
                    'from': 'all_lists_values',
                    'let': {
                        'color_id': '$color'
                    },
                    'pipeline': [
                        {
                            '$match': {
                                '$expr': {
                                    '$eq': [
                                        '$_id', '$$color_id'
                                    ]
                                }
                            }
                        }, {
                            '$project': {
                                'name': 1
                            }
                        }
                    ],
                    'as': 'color_details'
                }
            }, {
                '$unwind': {
                    'path': '$color_details',
                    'preserveNullAndEmptyArrays': True
                }
            }, {
                '$addFields': {
                    'car_brand_name': {
                        '$ifNull': [
                            '$brand_details.name', None
                        ]
                    },
                    'car_model_name': {
                        '$ifNull': [
                            '$model_details.name', None
                        ]
                    },
                    'color_name': {
                        '$ifNull': [
                            '$color_details.name', None
                        ]
                    }
                }
            }, {
                '$project': {
                    '_id': 1,
                    'car_brand_logo': 1,
                    'plate_number': 1,
                    'car_brand_name': 1,
                    'car_model_name': 1,
                    'color_name': 1
                }
            }
        ]

        cursor = await job_cards_collection.aggregate(pipeline)
        results = await cursor.to_list(None)
        serialized = [serializer(r) for r in results]

        return {"approved_jobs": serialized}


    except HTTPException:
        raise
    except Exception as error:
        raise HTTPException(status_code=500, detail=str(error))

@router.get("/get_all_time_sheets")
async def get_all_time_sheets(data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get('company_id'))

    except HTTPException:
        raise
    except Exception as error:
        raise HTTPException(status_code=500, detail=str(error))


@router.websocket("/listen_to_time_sheets")
async def websocket_endpoint(websocket: WebSocket, data: dict = Depends(security.get_current_user)):
    await manager.connect(websocket)
    try:
        company_id = ObjectId(data.get('company_id'))
        time_sheets_pipeline = [
            {
                '$match': {
                    "operationType": {"$in": ["insert", "update", "replace"]},
                    'fullDocument.company_id': company_id,

                }
            }, {
                '$lookup': {
                    'from': 'all_technicians',
                    'localField': 'employee_id',
                    'foreignField': '_id',
                    'as': 'employee_details'
                }
            }, {
                '$unwind': {
                    'path': '$employee_details',
                    'preserveNullAndEmptyArrays': True
                }
            }, {
                '$lookup': {
                    'from': 'all_job_tasks',
                    'localField': 'task_id',
                    'foreignField': '_id',
                    'as': 'task_details'
                }
            }, {
                '$unwind': {
                    'path': '$task_details',
                    'preserveNullAndEmptyArrays': True
                }
            }, {
                '$lookup': {
                    'from': 'job_cards',
                    'localField': 'job_id',
                    'foreignField': '_id',
                    'as': 'job_details'
                }
            }, {
                '$unwind': {
                    'path': '$job_details',
                    'preserveNullAndEmptyArrays': True
                }
            }, {
                '$lookup': {
                    'from': 'all_brands',
                    'localField': 'job_details.car_brand',
                    'foreignField': '_id',
                    'as': 'brand_details'
                }
            }, {
                '$unwind': {
                    'path': '$brand_details',
                    'preserveNullAndEmptyArrays': True
                }
            }, {
                '$lookup': {
                    'from': 'all_brand_models',
                    'localField': 'job_details.car_model',
                    'foreignField': '_id',
                    'as': 'model_details'
                }
            }, {
                '$unwind': {
                    'path': '$model_details',
                    'preserveNullAndEmptyArrays': True
                }
            }, {
                '$lookup': {
                    'from': 'all_lists_values',
                    'localField': 'job_details.color',
                    'foreignField': '_id',
                    'as': 'color_details'
                }
            }, {
                '$unwind': {
                    'path': '$color_details',
                    'preserveNullAndEmptyArrays': True
                }
            }, {
                '$addFields': {
                    'employee_name': {
                        '$ifNull': [
                            '$employee_details.name', None
                        ]
                    },
                    'task_ar_name': {
                        '$ifNull': [
                            '$task_details.name_ar', None
                        ]
                    },
                    'task_en_name': {
                        '$ifNull': [
                            '$task_details.name_en', None
                        ]
                    },
                    'brand_name': {
                        '$ifNull': [
                            '$brand_details.name', None
                        ]
                    },
                    'model_name': {
                        '$ifNull': [
                            '$model_details.name', None
                        ]
                    },
                    'plate_number': {
                        '$ifNull': [
                            '$job_details.plate_number', None
                        ]
                    },
                    'color': {
                        '$ifNull': [
                            '$color_details.name', None
                        ]
                    },
                    'logo': {
                        '$ifNull': [
                            '$job_details.car_brand_logo', None
                        ]
                    }
                }
            }, {
                '$project': {
                    'employee_details': 0,
                    'task_details': 0,
                    'brand_details': 0,
                    'model_details': 0,
                    'color_details': 0,
                    'job_details': 0
                }
            }
        ]
        with time_sheets_collection.watch(pipeline=time_sheets_pipeline,full_document="updateLookup") as stream:
            for change in stream:
                data = dumps(change)
                await manager.broadcast(json_message(data))
                await asyncio.sleep(0.05)
    except Exception as e:
        print(f"❌ WebSocket error: {e}")
    finally:
        manager.disconnect(websocket)


def json_message(data):
    return {
        "type": "timesheet_update",
        "payload": data
    }

from typing import Optional
from bson import ObjectId
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from pymongo import UpdateOne
from app.core import security
from app.database import get_collection
from datetime import datetime
from app.websocket_config import manager

router = APIRouter()
time_sheets_collection = get_collection("time_sheets")
job_cards_collection = get_collection("job_cards")


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


class StartFunction(BaseModel):
    job_id: Optional[str] = None
    task_id: Optional[str] = None
    employee_id: Optional[str] = None


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
                    "plate_code": 1,
                    'car_brand_name': 1,
                    'car_model_name': 1,
                    'color_name': 1
                }
            },
            {
                "$sort": {
                    "car_brand_name": 1
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


time_sheets_pipeline = [
    {
        '$lookup': {
            'from': 'all_lists_values',
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
            'plate_code': {
                '$ifNull': [
                    '$job_details.plate_code', None
                ]
            },
            'employee_job_title': {
                '$ifNull': [
                    '$employee_details.job_title', None
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


async def get_time_sheet_details(time_sheet_id: ObjectId):
    time_sheet_details_pipeline = time_sheets_pipeline.copy()
    time_sheet_details_pipeline.insert(0,
                                       {
                                           "$match": {
                                               "_id": time_sheet_id
                                           }
                                       })

    cursor = await time_sheets_collection.aggregate(time_sheet_details_pipeline)
    try:
        return await cursor.next()
    except StopAsyncIteration:
        return None


@router.get("/get_all_time_sheets")
async def get_all_time_sheets(data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get('company_id'))
        all_sheets_pipeline = time_sheets_pipeline.copy()
        all_sheets_pipeline.insert(0,
                                   {
                                       '$match': {
                                           'company_id': company_id,
                                           'end_date': None

                                       }
                                   }
                                   )
        all_sheets_pipeline.insert(1,
                                   {
                                       '$sort': {
                                           'start_date': -1
                                       }
                                   }
                                   )

        cursor = await time_sheets_collection.aggregate(all_sheets_pipeline)
        results = await cursor.to_list(None)
        serialized = [serializer(r) for r in results]
        return {"time_sheets": serialized}

    except HTTPException:
        raise
    except Exception as error:
        raise HTTPException(status_code=500, detail=str(error))


@router.post("/start_function")
async def start_function(start: StartFunction, data: dict = Depends(security.get_current_user)):
    try:
        start = start.model_dump(exclude_unset=True)
        company_id = ObjectId(data.get("company_id"))
        sheet_dict = {
            "company_id": company_id,
            "job_id": ObjectId(start['job_id']) if 'job_id' in start else None,
            "task_id": ObjectId(start['task_id']) if 'task_id' in start else None,
            "employee_id": ObjectId(start['employee_id']) if 'employee_id' in start else None,
            "start_date": datetime.now(),
            "end_date": None,
            "active_periods": [{
                "from": datetime.now(),
                "to": None
            }]
        }
        result = await  time_sheets_collection.insert_one(sheet_dict)
        new_start_result = await get_time_sheet_details(result.inserted_id)
        serialized = serializer(new_start_result)
        await manager.broadcast({
            "type": "time_sheets_start_function",
            "data": serialized
        })

    except HTTPException:
        raise
    except Exception as error:
        raise HTTPException(status_code=500, detail=str(error))


@router.patch("/pause_function/{time_sheet_id}")
async def pause_function(time_sheet_id: str, data: dict = Depends(security.get_current_user)):
    try:
        time_sheet_id = ObjectId(time_sheet_id)
        company_id = ObjectId(data.get('company_id'))
        current_time_sheet = await time_sheets_collection.find_one({"_id": time_sheet_id, "company_id": company_id})
        if not current_time_sheet:
            raise HTTPException(status_code=404, detail='Time sheet not found')

        active_periods = current_time_sheet.get('active_periods', [])
        if active_periods and active_periods[-1].get("to") is None:
            active_periods[-1]['to'] = datetime.now()
            await time_sheets_collection.update_one(
                {"_id": time_sheet_id},
                {"$set": {"active_periods": active_periods}},
            )
            await manager.broadcast({
                "type": "time_sheets_pause_function",
                "data": {"_id": str(time_sheet_id)}
            })

    except HTTPException:
        raise
    except Exception as error:
        raise HTTPException(status_code=500, detail=str(error))


@router.patch("/continue_function/{time_sheet_id}")
async def continue_function(time_sheet_id: str, data: dict = Depends(security.get_current_user)):
    try:
        time_sheet_id = ObjectId(time_sheet_id)
        company_id = ObjectId(data.get('company_id'))
        current_time_sheet = await time_sheets_collection.find_one({"_id": time_sheet_id, "company_id": company_id})
        if not current_time_sheet:
            raise HTTPException(status_code=404, detail='Time sheet not found')

        active_periods = current_time_sheet.get('active_periods', [])
        if active_periods and active_periods[-1].get("to") is None:
            return
        active_periods.append({
            "from": datetime.now(),
            "to": None
        })
        await time_sheets_collection.update_one(
            {"_id": time_sheet_id},
            {"$set": {"active_periods": active_periods}},
        )
        await manager.broadcast({
            "type": "time_sheets_continue_function",
            "data": {"_id": str(time_sheet_id)}
        })

    except HTTPException:
        raise
    except Exception as error:
        raise HTTPException(status_code=500, detail=str(error))


@router.patch("/finish_function/{time_sheet_id}")
async def finish_function(time_sheet_id: str, data: dict = Depends(security.get_current_user)):
    try:
        time_sheet_id = ObjectId(time_sheet_id)
        company_id = ObjectId(data.get('company_id'))
        current_time_sheet = await time_sheets_collection.find_one({"_id": time_sheet_id, "company_id": company_id})
        if not current_time_sheet:
            raise HTTPException(status_code=404, detail='Time sheet not found')

        active_periods = current_time_sheet.get('active_periods', [])
        end_date = datetime.now()
        if active_periods and active_periods[-1].get("to") is None:
            active_periods[-1]['to'] = datetime.now()

        await time_sheets_collection.update_one(
            {"_id": time_sheet_id},
            {"$set": {"active_periods": active_periods, "end_date": end_date}},
        )
        await manager.broadcast({
            "type": "time_sheets_finish_function",
            "data": {"_id": str(time_sheet_id)}
        })

    except HTTPException:
        raise
    except Exception as error:
        raise HTTPException(status_code=500, detail=str(error))


@router.patch("/pause_all_function")
async def pause_all_function(data: dict = Depends(security.get_current_user)):
    try:

        company_id = ObjectId(data.get('company_id'))
        filter_query = {
            "company_id": company_id,
            "active_periods": {
                "$elemMatch": {"to": None}
            }
        }

        updates = []
        async for sheet in time_sheets_collection.find(filter_query):
            active_periods = sheet.get("active_periods", [])
            if active_periods and active_periods[-1].get("to") is None:
                active_periods[-1]["to"] = datetime.now()
                updates.append(
                    UpdateOne({"_id": sheet["_id"]}, {"$set": {"active_periods": active_periods}})
                )

        if updates:
            await time_sheets_collection.bulk_write(updates)
        await manager.broadcast({
            "type": "time_sheets_pause_all_function",
            "data": {"status": "paused"}
        })

    except HTTPException:
        raise
    except Exception as error:
        raise HTTPException(status_code=500, detail=str(error))

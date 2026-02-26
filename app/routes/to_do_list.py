import copy
from typing import Optional, Any
from bson import ObjectId
from fastapi import APIRouter, HTTPException, Depends
from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel
from app import database
from app.core import security
from app.database import get_collection
from datetime import datetime

from app.routes.car_trading import PyObjectId
from app.routes.counters import create_custom_counter
from app.websocket_config import manager

router = APIRouter()
to_do_list_collection = get_collection("to_do_list")
to_do_list_description_collection = get_collection("to_do_list_description")


class ToDoListModel(BaseModel):
    date: Optional[datetime] = None
    due_date: Optional[datetime] = None
    created_by: Optional[str] = None
    assigned_to: Optional[str] = None
    status: Optional[str] = None
    description: Optional[str] = None


class TaskModel(BaseModel):
    number: Optional[str] = None
    created_by: Optional[PyObjectId] = None
    assigned_to: Optional[PyObjectId] = None
    status: Optional[str] = None
    date: Optional[datetime] = None
    due_date: Optional[datetime] = None
    from_date: Optional[datetime] = None
    to_date: Optional[datetime] = None


class DescriptionNoteModel(BaseModel):
    to_do_list_id: Optional[str] = None
    type: Optional[str] = None
    description: Optional[str] = None


task_details_pipeline = [
    {
        '$lookup': {
            'from': 'sys-users',
            'let': {
                'createdBy': '$created_by',
                'assignedTo': '$assigned_to'
            },
            'pipeline': [
                {
                    '$match': {
                        '$expr': {
                            '$in': [
                                '$_id', [
                                    '$$createdBy', '$$assignedTo'
                                ]
                            ]
                        }
                    }
                }, {
                    '$project': {
                        '_id': 1,
                        'user_name': 1
                    }
                }
            ],
            'as': '_users'
        }
    }, {
        '$set': {
            'created_by_details': {
                '$first': {
                    '$filter': {
                        'input': '$_users',
                        'as': 'u',
                        'cond': {
                            '$eq': [
                                '$$u._id', '$created_by'
                            ]
                        }
                    }
                }
            },
            'assigned_to_details': {
                '$first': {
                    '$filter': {
                        'input': '$_users',
                        'as': 'u',
                        'cond': {
                            '$eq': [
                                '$$u._id', '$assigned_to'
                            ]
                        }
                    }
                }
            }
        }
    }, {
        '$unset': '_users'
    }, {
        '$addFields': {
            'created_by_name': {
                '$ifNull': [
                    '$created_by_details.user_name', ''
                ]
            },
            'assigned_to_name': {
                '$ifNull': [
                    '$assigned_to_details.user_name', ''
                ]
            },
            '_id': {
                '$toString': '$_id'
            },
            'created_by': {
                '$toString': '$created_by'
            },
            'assigned_to': {
                '$toString': '$assigned_to'
            },
            'company_id': {
                '$toString': '$company_id'
            }
        }
    }, {
        '$project': {
            'created_by_details': 0,
            'assigned_to_details': 0
        }
    }
]


def task_description_pipeline(task_id: ObjectId, user_id: ObjectId):
    return [
        {
            '$match': {
                '_id': task_id
            }
        },
        {
            '$lookup': {
                'from': 'to_do_list_description',
                'localField': '_id',
                'foreignField': 'to_do_list_id',
                'as': 'description_details',
                'pipeline': [
                    {
                        '$lookup': {
                            'from': 'sys-users',
                            'localField': 'user_id',
                            'foreignField': '_id',
                            'as': 'user_details',
                            'pipeline': [
                                {
                                    '$project': {
                                        '_id': 0,
                                        'user_name': 1
                                    }
                                }
                            ]
                        }
                    }, {
                        '$set': {
                            'user_name': {
                                '$ifNull': [
                                    {
                                        '$first': '$user_details.user_name'
                                    }, ''
                                ]
                            },
                            '_id': {
                                '$toString': '$_id'
                            },
                            'isThisUserTheCurrentUser': {
                                '$eq': ["$user_id", user_id]
                            },
                            'to_do_list_id': {
                                '$toString': '$to_do_list_id'
                            },
                            'user_id': {
                                '$toString': '$user_id'
                            },
                            'company_id': {
                                '$toString': '$company_id'
                            }
                        }
                    }, {
                        '$unset': 'user_details'
                    }
                ]
            }
        }, {
            '$project': {
                '_id': 0,
                'description_details': 1
            }
        }
    ]


@router.post("/add_new_task")
async def add_new_task(to_do_list: ToDoListModel, data: dict = Depends(security.get_current_user)):
    async with database.client.start_session() as session:
        try:
            await session.start_transaction()
            company_id = ObjectId(data.get('company_id'))
            user_id = ObjectId(data.get('sub'))
            new_task_counter = await create_custom_counter("TDN", "T", data=data, description='To-Do Number',
                                                           session=session)

            to_do_list = to_do_list.model_dump(exclude_unset=True)
            description = to_do_list.pop("description", None)
            to_do_list.update({
                "number": new_task_counter['final_counter'] if new_task_counter['success'] else None,
                "status": "Open",
                "company_id": company_id,
                "createdAt": security.now_utc(),
                "updatedAt": security.now_utc(),
                "created_by": ObjectId(to_do_list["created_by"]) if to_do_list["created_by"] else None,
                "assigned_to": ObjectId(to_do_list["assigned_to"]) if to_do_list["assigned_to"] else None,
            })
            result = await to_do_list_collection.insert_one(to_do_list, session=session)
            to_do_list_id = result.inserted_id
            await to_do_list_description_collection.insert_one({
                "to_do_list_id": to_do_list_id,
                "user_id": user_id,
                "type": "text",
                "description": description,
                "company_id": company_id,
                "createdAt": security.now_utc(),
                "updatedAt": security.now_utc(),
            }, session=session)
            await session.commit_transaction()
            await manager.broadcast({
                "type": "new_task_added",
                "data": ""
            })
        except Exception as e:
            await session.abort_transaction()
            print(e)
            raise HTTPException(status_code=500, detail=str(e))


@router.get("/get_task_descriptions/{task_id}")
async def get_task_descriptions(
        task_id: str, data: dict = Depends(security.get_current_user)
):
    try:
        if not ObjectId.is_valid(task_id):
            raise HTTPException(status_code=400, detail="Invalid task_id")
        user_id = ObjectId(data.get('sub'))
        task_oid = ObjectId(task_id)
        pipeline: Any = task_description_pipeline(task_oid, user_id)
        cursor = await to_do_list_collection.aggregate(pipeline)
        docs = await cursor.to_list(length=1)

        if not docs:
            return {"descriptions": []}

        return {"descriptions": docs[0].get("description_details", [])}

    except HTTPException:
        raise
    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=str(e))


async def get_description_note_data(note_id: ObjectId, user_id: ObjectId):
    try:
        description_note_data_pipeline = [
            {
                '$match': {
                    '_id': note_id
                }
            }, {
                '$lookup': {
                    'from': 'sys-users',
                    'localField': 'user_id',
                    'foreignField': '_id',
                    'as': 'user_details',
                    'pipeline': [
                        {
                            '$project': {
                                '_id': 0,
                                'user_name': 1
                            }
                        }
                    ]
                }
            }, {
                '$set': {
                    'user_name': {
                        '$ifNull': [
                            {
                                '$first': '$user_details.user_name'
                            }, ''
                        ]
                    },
                    '_id': {
                        '$toString': '$_id'
                    },
                    'isThisUserTheCurrentUser': {
                        '$eq': [
                            '$user_id', user_id
                        ]
                    },
                    'to_do_list_id': {
                        '$toString': '$to_do_list_id'
                    },
                    'user_id': {
                        '$toString': '$user_id'
                    },
                    'company_id': {
                        '$toString': '$company_id'
                    }
                }
            }, {
                '$unset': 'user_details'
            }
        ]

        cursor = await to_do_list_description_collection.aggregate(description_note_data_pipeline)
        result = await cursor.to_list(length=1)
        return result[0] if result else None

    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/add_new_task_description_note")
async def add_new_task_description_note(note: DescriptionNoteModel, data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get('company_id'))
        user_id = ObjectId(data.get('sub'))
        note = note.model_dump(exclude_unset=True)
        note_dict = {
            "description": note.get("description"),
            "user_id": user_id,
            "createdAt": security.now_utc(),
            "updatedAt": security.now_utc(),
            "company_id": company_id,
            "to_do_list_id": ObjectId(note.get("to_do_list_id")),
            "type": note.get("type"),
        }
        result = await to_do_list_description_collection.insert_one(note_dict)

        added_note = await get_description_note_data(result.inserted_id, user_id)
        print(added_note)
        # encoded_data = jsonable_encoder(purchase_agreement_item_dict)

        await manager.broadcast({
            "type": "new_task_description_note_added",
            "data": jsonable_encoder(added_note)
        })

    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/search_engine_for_to_do_list")
async def search_engine_for_to_do_list(filter_tasks: TaskModel,
                                       data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        base_search_pipeline: list[dict] = copy.deepcopy(task_details_pipeline)
        match_stage = {}
        if company_id:
            match_stage["company_id"] = company_id
        if filter_tasks.number:
            match_stage["number"] = filter_tasks.number
        if filter_tasks.created_by:
            match_stage["created_by"] = filter_tasks.created_by
        if filter_tasks.assigned_to:
            match_stage["assigned_to"] = filter_tasks.assigned_to
        if filter_tasks.date:
            match_stage["date"] = filter_tasks.date
        if filter_tasks.due_date:
            match_stage["due_date"] = filter_tasks.due_date
        if filter_tasks.status:
            match_stage["status"] = filter_tasks.status

        base_search_pipeline.insert(0, {"$match": match_stage})
        base_search_pipeline.insert(1, {"$sort": {"due_date": 1}})
        cursor = await to_do_list_collection.aggregate(base_search_pipeline)
        results = await cursor.to_list(None)
        return {"tasks": results}


    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

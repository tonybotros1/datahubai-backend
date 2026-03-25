from typing import Optional, Any
from bson import ObjectId
from bson.errors import InvalidId
from fastapi import APIRouter, HTTPException, Depends, Form, File, UploadFile
from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel
from pymongo import ReturnDocument
from pymongo.errors import PyMongoError

from app import database
from app.core import security
from app.database import get_collection
from datetime import datetime

from app.routes.car_trading import PyObjectId
from app.routes.counters import create_custom_counter
from app.websocket_config import manager
from app.widgets.upload_files import upload_file
from app.widgets.upload_images import upload_image

router = APIRouter()
to_do_list_collection = get_collection("to_do_list")
to_do_list_description_collection = get_collection("to_do_list_description")
users_collection = get_collection("sys-users")


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


class UpdateTaskModel(BaseModel):
    status: Optional[str] = None


# class DescriptionNoteModel(BaseModel):
#     to_do_list_id: Optional[str] = None
#     type: Optional[str] = None
#     description: Optional[str] = None


def task_details_pipeline(user_id: ObjectId) -> list:
    return [
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
        },
        {
            '$lookup': {
                'from': 'to_do_list_description',
                'let': {
                    'taskId': '$_id'
                },
                'pipeline': [
                    {
                        '$match': {
                            '$expr': {
                                '$and': [
                                    {
                                        '$eq': [
                                            '$to_do_list_id', '$$taskId'
                                        ]
                                    }, {
                                        '$eq': [
                                            '$read', False
                                        ]
                                    }, {
                                        '$eq': ["$receiver_id", user_id]
                                    }
                                ]
                            }
                        }
                    }, {
                        '$count': 'unread_count'
                    }
                ],
                'as': 'unread_notes'
            }
        }, {
            '$addFields': {
                'unread_notes_count': {
                    '$ifNull': [
                        {
                            '$first': '$unread_notes.unread_count'
                        }, 0
                    ]
                }
            }
        }, {
            '$unset': 'unread_notes'
        },
        {
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
                            }, 'sender_id': {
                                '$toString': '$sender_id'
                            },
                            'receiver_id': {
                                '$toString': '$receiver_id'
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


# @router.post("/add_new_task")
# async def add_new_task(to_do_list: ToDoListModel, data: dict = Depends(security.get_current_user)):
#     async with database.client.start_session() as session:
#         try:
#             await session.start_transaction()
#             company_id = ObjectId(data.get('company_id'))
#             user_id = ObjectId(data.get('sub'))
#             new_task_counter = await create_custom_counter("TDN", "T", data=data, description='To-Do Number',
#                                                            session=session)
#
#             to_do_list = to_do_list.model_dump(exclude_unset=True)
#             description = to_do_list.pop("description", None)
#             to_do_list.update({
#                 "number": new_task_counter['final_counter'] if new_task_counter['success'] else None,
#                 "status": "Open",
#                 "company_id": company_id,
#                 "createdAt": security.now_utc(),
#                 "updatedAt": security.now_utc(),
#                 "created_by": ObjectId(to_do_list["created_by"]) if to_do_list["created_by"] else None,
#                 "assigned_to": ObjectId(to_do_list["assigned_to"]) if to_do_list["assigned_to"] else None,
#             })
#             result = await to_do_list_collection.insert_one(to_do_list, session=session)
#             to_do_list_id = result.inserted_id
#
#             await to_do_list_description_collection.insert_one({
#                 "to_do_list_id": to_do_list_id,
#                 "user_id": user_id,
#                 "type": "text",
#                 "description": description,
#                 "company_id": company_id,
#                 "createdAt": security.now_utc(),
#                 "updatedAt": security.now_utc(),
#                 "sender_id": ObjectId(to_do_list["created_by"]) if to_do_list["created_by"] else None,
#                 "receiver_id": ObjectId(to_do_list["assigned_to"]) if to_do_list["assigned_to"] else None,
#                 "read": False,
#                 "read_at": None
#             }, session=session)
#             await session.commit_transaction()
#
#             to_do_list.update({
#                 "_id": str(to_do_list_id),
#                 "created_by": str(to_do_list["created_by"]) if to_do_list["created_by"] else None,
#                 "assigned_to": str(to_do_list["assigned_to"]) if to_do_list["assigned_to"] else None,
#             })
#             #
#             sender_id = ObjectId(to_do_list["created_by"]) if to_do_list["created_by"] else None
#             receiver_id = ObjectId(to_do_list["assigned_to"]) if to_do_list["assigned_to"] else None
#             # 3) chat message للطرفين
#
#             chat_event = {
#                 "type": "new_task_created",
#                 "data": jsonable_encoder(to_do_list),
#             }
#             #
#             await manager.send_to_user(str(sender_id), chat_event)
#             # if receiver_id and receiver_id != sender_id:
#             #     await manager.send_to_user(str(receiver_id), chat_event)
#             #
#             #     # badge للمستلم فقط
#             #     unread_total = await to_do_list_description_collection.count_documents({
#             #         "company_id": company_id,
#             #         "receiver_id": receiver_id,
#             #         "read": False
#             #     })
#             #
#             #     await manager.send_to_user(str(receiver_id), {
#             #         "type": "chat_unread",
#             #         "task_id": str(to_do_list_id),
#             #         "preview": (description or "")[:60],
#             #         "unread_total": unread_total
#             #     })
#
#         except Exception as e:
#             await session.abort_transaction()
#             print(e)
#             raise HTTPException(status_code=500, detail=str(e))


async def get_task_details(task_id: ObjectId, user_id: ObjectId):
    try:
        base_search_pipeline = task_details_pipeline(user_id)
        base_search_pipeline.insert(0, {
            '$match': {"_id": task_id}
        })
        cursor = await to_do_list_collection.aggregate(base_search_pipeline)
        results = await cursor.to_list(None)
        return results[0]
    except Exception as e:
        print(e)
        raise HTTPException(status_code=404, detail="Task not found")


@router.post("/add_new_task")
async def add_new_task(
        to_do_list: ToDoListModel,
        data: dict = Depends(security.get_current_user)
):
    async with database.client.start_session() as session:
        try:
            await session.start_transaction()
            company_id = ObjectId(data.get("company_id"))
            user_id = ObjectId(data.get("sub"))

            new_task_counter = await create_custom_counter(
                "TDN",
                "T",
                data=data,
                description="To-Do Number",
                session=session
            )

            task_data = to_do_list.model_dump(exclude_unset=True)
            description = task_data.pop("description", None)

            created_by = ObjectId(task_data["created_by"]) if task_data.get("created_by") else None
            assigned_to = ObjectId(task_data["assigned_to"]) if task_data.get("assigned_to") else None

            task_data.update({
                "number": new_task_counter["final_counter"] if new_task_counter["success"] else None,
                "status": "Open",
                "company_id": company_id,
                "createdAt": security.now_utc(),
                "updatedAt": security.now_utc(),
                "created_by": created_by,
                "assigned_to": assigned_to,
            })

            result = await to_do_list_collection.insert_one(task_data, session=session)
            to_do_list_id = result.inserted_id

            await to_do_list_description_collection.insert_one({
                "to_do_list_id": to_do_list_id,
                "user_id": user_id,
                "type": "text",
                "description": description,
                "company_id": company_id,
                "createdAt": security.now_utc(),
                "updatedAt": security.now_utc(),
                "sender_id": created_by,
                "receiver_id": assigned_to,
                "read": False,
                "read_at": None
            }, session=session)

            # ✅ Transaction committed successfully here
            await session.commit_transaction()

        except PyMongoError as e:
            # 🔴 Database / transaction errors
            await session.abort_transaction()
            print("Transaction Error:", e)
            raise HTTPException(status_code=500, detail="Database transaction failed")

        except Exception as e:
            # 🔴 Any other unexpected errors
            print("Unexpected Error:", e)
            raise HTTPException(status_code=500, detail="Unexpected server error")

    # =========================================================
    # AFTER successful transaction → prepare frontend response
    # =========================================================

    # Convert ALL ObjectIds to string
    # response_data = {
    #     **task_data,
    #     "_id": str(to_do_list_id),
    #     "company_id": str(company_id),
    #     "created_by": str(created_by) if created_by else None,
    #     "assigned_to": str(assigned_to) if assigned_to else None,
    # }
    try:
        response_data = await get_task_details(to_do_list_id, user_id)
        print(response_data)

        chat_event = {
            "type": "new_task_created",
            "data": jsonable_encoder(response_data),
        }

        sender_id = created_by
        receiver_id = assigned_to

        await manager.send_to_user(str(user_id), chat_event)
        print(chat_event)

        if receiver_id and receiver_id != sender_id:
            await manager.send_to_user(str(receiver_id), chat_event)

            unread_total = await to_do_list_description_collection.count_documents({
                "company_id": company_id,
                "receiver_id": receiver_id,
                "read": False
            })

            await manager.send_to_user(str(receiver_id), {
                "type": "chat_unread",
                "task_id": str(to_do_list_id),
                "preview": (description or "")[:60],
                "unread_total": unread_total
            })

        return response_data

    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail="failed")


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
                    },
                    'sender_id': {
                        '$toString': '$sender_id'
                    },
                    'receiver_id': {
                        '$toString': '$receiver_id'
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


@router.post("/add_new_task_description_note/{to_do_list_id}")
async def add_new_task_description_note(
        to_do_list_id: str,
        note_type: str = Form(None), note: str = Form(None),
        media_note: UploadFile = File(None), file_name: str = Form(None),
        data: dict = Depends(security.get_current_user)
):
    try:
        note_public_id = None

        company_id = ObjectId(data.get("company_id"))
        sender_id = ObjectId(data.get("sub"))
        if note_type and note_type.lower() != 'text' and media_note is not None:
            if note_type.lower() != 'image':
                result = await upload_file(media_note, folder="to do list descriptions")
                file_name = result["file_name"]
                note = result["url"] if "url" in result else None
                note_public_id = result['public_id'] if "public_id" in result else None
            else:
                result = await upload_image(media_note, folder="to do list descriptions")
                file_name = result["file_name"]
                note = result["url"] if "url" in result else None
                note_public_id = result['public_id'] if "public_id" in result else None

        to_do_list_id = ObjectId(to_do_list_id)

        # 1) جيب المهمة لتحدد الطرف الثاني
        task = await to_do_list_collection.find_one(
            {"_id": to_do_list_id, "company_id": company_id},
            {"created_by": 1, "assigned_to": 1}
        )
        if not task:
            raise HTTPException(status_code=404, detail="Task not found")

        created_by = task.get("created_by")
        assigned_to = task.get("assigned_to")

        if sender_id == created_by:
            receiver_id = assigned_to
        elif sender_id == assigned_to:
            receiver_id = created_by
        else:
            raise HTTPException(status_code=403, detail="User not part of this task")

        # 2) احفظ الرسالة مع sender/receiver
        note_dict = {
            "description": note,
            "user_id": sender_id,
            "sender_id": sender_id,
            "receiver_id": receiver_id,
            "createdAt": security.now_utc(),
            "updatedAt": security.now_utc(),
            "company_id": company_id,
            "to_do_list_id": to_do_list_id,
            "file_name": file_name,
            "type": note_type,
            "read": False,
            "read_at": None,
            "note_public_id": note_public_id,
        }

        result = await to_do_list_description_collection.insert_one(note_dict)
        added_note = await get_description_note_data(result.inserted_id, sender_id)

        # 3) chat message للطرفين
        chat_event = {
            "type": "chat_message",
            "task_id": str(to_do_list_id),
            "data": jsonable_encoder(added_note),
            "from_user_id": str(sender_id)
        }

        await manager.send_to_user(str(sender_id), chat_event)
        if receiver_id and receiver_id != sender_id:
            await manager.send_to_user(str(receiver_id), chat_event)

            # badge للمستلم فقط
            unread_total = await to_do_list_description_collection.count_documents({
                "company_id": company_id,
                "receiver_id": receiver_id,
                "read": False
            })

            await manager.send_to_user(str(receiver_id), {
                "type": "chat_unread",
                "task_id": str(to_do_list_id),
                # "preview": (note_data.get("description") or "")[:60],
                "unread_total": unread_total
            })

        return {"ok": True, "data": jsonable_encoder(added_note)}

    except HTTPException:
        raise
    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/tasks/{task_id}/read")
async def mark_task_chat_as_read(
        task_id: str,
        data: dict = Depends(security.get_current_user)
):
    """
    عند فتح التاسك: علّم رسائل هذا التاسك كمقروءة للمستخدم الحالي
    ثم أرسل unread_total الجديد لنفس المستخدم عبر websocket.
    """
    try:
        user_id = ObjectId(data.get("sub"))
        company_id = ObjectId(data.get("company_id"))
        now = security.now_utc()

        # علّم الرسائل الموجّهة لهذا المستخدم فقط كمقروءة
        await to_do_list_description_collection.update_many(
            {
                "company_id": company_id,
                "to_do_list_id": ObjectId(task_id),
                "receiver_id": user_id,
                "read": False
            },
            {
                "$set": {
                    "read": True,
                    "read_at": now,
                    "updatedAt": now
                }
            }
        )

        # احسب إجمالي unread بعد التحديث
        unread_total = await to_do_list_description_collection.count_documents({
            "company_id": company_id,
            "receiver_id": user_id,
            "read": False
        })

        # ابعث الحدث لنفس المستخدم (كل أجهزته/تبويباته)
        await manager.send_to_user(str(user_id), {
            "type": "chat_unread",
            "unread_total": unread_total
        })

        return {"ok": True, "unread_total": unread_total}

    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/chat/unread_count")
async def get_chat_unread_count(
        data: dict = Depends(security.get_current_user)
):
    try:
        user_id = ObjectId(data.get("sub"))
        company_id = ObjectId(data.get("company_id"))

        unread_total = await to_do_list_description_collection.count_documents({
            "company_id": company_id,
            "receiver_id": user_id,
            "read": False
        })

        return {"unread_total": unread_total}

    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/search_engine_for_to_do_list")
async def search_engine_for_to_do_list(filter_tasks: TaskModel,
                                       data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        user_id = ObjectId(data.get("sub"))
        admins_ids = await get_admin_id(company_id)
        admins_ids.append(user_id)
        base_search_pipeline: list[dict] = task_details_pipeline(user_id)
        match_stage = {}
        if company_id:
            match_stage["company_id"] = company_id
        if admins_ids:
            match_stage["$or"] = [
                {
                    "created_by": {
                        "$in": admins_ids
                    }
                },
                {
                    "assigned_to": {
                        "$in": admins_ids
                    }
                }
            ]
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
        if filter_tasks.from_date or filter_tasks.to_date:
            match_stage['date_field_to_filter'] = {}
            if filter_tasks.from_date:
                match_stage['date_field_to_filter']["$gte"] = filter_tasks.from_date
            if filter_tasks.to_date:
                match_stage['date_field_to_filter']["$lte"] = filter_tasks.to_date

        base_search_pipeline.insert(0, {"$match": match_stage})
        base_search_pipeline.insert(1, {"$sort": {"due_date": 1}})
        cursor = await to_do_list_collection.aggregate(base_search_pipeline)
        results = await cursor.to_list(None)
        return {"tasks": results}


    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


async def get_admin_id(company_id: ObjectId) -> list:
    try:
        list_od_admin_users_id = await users_collection.find({"company_id": company_id, "is_admin": True},
                                                             {"_id": 1}).to_list(None)
        return list_od_admin_users_id

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.patch("/update_task_status/{task_id}")
async def update_task_status(
        task_id: str,
        task: UpdateTaskModel,
        data: dict = Depends(security.get_current_user),
):
    try:
        company_id = data.get("company_id")
        oid = ObjectId(task_id)
    except InvalidId:
        raise HTTPException(status_code=400, detail="Invalid task_id")

    updated_task = await to_do_list_collection.find_one_and_update(
        {"_id": oid},
        {"$set": {"status": task.status}},
        return_document=ReturnDocument.AFTER,
    )

    if not updated_task:
        raise HTTPException(status_code=404, detail="Task not found")

    await manager.send_to_company(company_id, {
        "type": "task_status_updated",
        "data": {"status": task.status, "_id": str(oid)},
    })

    return {"_id": str(oid), "status": task.status}

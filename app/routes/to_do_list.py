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
from datetime import datetime, timedelta

from app.routes.car_trading import PyObjectId
from app.routes.counters import create_custom_counter
from app.websocket_config import manager
from app.widgets.upload_files import upload_file, delete_file_from_server
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
    all: Optional[bool] = None


class UpdateTaskModel(BaseModel):
    status: Optional[str] = None


VALID_TASK_STATUSES = {"Open", "Closed", "Cancelled"}


def parse_object_id(value: Any, field_name: str) -> ObjectId:
    try:
        return ObjectId(str(value))
    except (InvalidId, TypeError):
        raise HTTPException(status_code=400, detail=f"Invalid {field_name}")


async def is_company_admin(company_id: ObjectId, user_id: ObjectId) -> bool:
    user = await users_collection.find_one(
        {"_id": user_id, "company_id": company_id},
        {"is_admin": 1},
    )
    return bool(user and user.get("is_admin"))


async def ensure_company_user(company_id: ObjectId, user_id: Optional[ObjectId], field_name: str) -> None:
    if user_id is None:
        return

    exists = await users_collection.find_one(
        {"_id": user_id, "company_id": company_id},
        {"_id": 1},
    )
    if not exists:
        raise HTTPException(status_code=400, detail=f"Invalid {field_name}")


async def get_task_for_user(
        task_id: ObjectId,
        company_id: ObjectId,
        user_id: ObjectId,
        allow_admin: bool = True,
        edit_only: bool = False,
        projection: Optional[dict] = None,
) -> dict:
    query: dict[str, Any] = {"_id": task_id, "company_id": company_id}
    is_admin = await is_company_admin(company_id, user_id) if allow_admin else False

    if not is_admin:
        if edit_only:
            query["created_by"] = user_id
        else:
            query["$or"] = [{"created_by": user_id}, {"assigned_to": user_id}]

    task = await to_do_list_collection.find_one(query, projection)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    return task


async def send_task_event_to_participants(task: dict, event: dict) -> None:
    for user_id in task_participant_ids(task):
        await manager.send_to_user(str(user_id), event)


def task_participant_ids(*tasks: dict) -> set[ObjectId]:
    user_ids: set[ObjectId] = set()
    for task in tasks:
        if not task:
            continue
        for user_id in [task.get("created_by"), task.get("assigned_to")]:
            if not user_id:
                continue
            user_ids.add(user_id if isinstance(user_id, ObjectId) else ObjectId(str(user_id)))
    return user_ids


async def send_task_data_event_to_participants(
        task_id: ObjectId,
        company_id: ObjectId,
        actor_id: ObjectId,
        event_type: str,
        actor_key: str,
        *tasks: dict,
) -> None:
    for viewer_id in task_participant_ids(*tasks):
        task_data = await get_task_details(task_id, viewer_id, company_id)
        await manager.send_to_user(str(viewer_id), {
            "type": event_type,
            "data": jsonable_encoder(task_data),
            actor_key: str(actor_id),
        })


async def send_task_updated_event(
        existing_task: dict,
        updated_task: dict,
        task_id: ObjectId,
        company_id: ObjectId,
        actor_id: ObjectId,
) -> None:
    old_participants = task_participant_ids(existing_task)
    new_participants = task_participant_ids(updated_task)

    for viewer_id in old_participants - new_participants:
        await manager.send_to_user(str(viewer_id), {
            "type": "task_deleted",
            "data": {"_id": str(task_id)},
            "deleted_by": str(actor_id),
        })

    for viewer_id in new_participants:
        task_data = await get_task_details(task_id, viewer_id, company_id)
        await manager.send_to_user(str(viewer_id), {
            "type": "task_updated",
            "data": jsonable_encoder(task_data),
            "updated_by": str(actor_id),
        })


async def send_note_event_to_participants(
        task: dict,
        task_id: ObjectId,
        note_id: ObjectId,
        sender_id: ObjectId,
) -> None:
    for viewer_id in task_participant_ids(task):
        note_data = await get_description_note_data(note_id, viewer_id)
        await manager.send_to_user(str(viewer_id), {
            "type": "chat_message",
            "task_id": str(task_id),
            "data": jsonable_encoder(note_data),
            "from_user_id": str(sender_id),
        })


async def send_unread_total(company_id: ObjectId, user_id: Optional[ObjectId]) -> None:
    if not user_id:
        return
    unread_total = await to_do_list_description_collection.count_documents({
        "company_id": company_id,
        "receiver_id": user_id,
        "read": False
    })
    await manager.send_to_user(str(user_id), {
        "type": "chat_unread",
        "unread_total": unread_total
    })


def normalize_status(status: Optional[str]) -> str:
    if not status:
        raise HTTPException(status_code=400, detail="Status is required")

    normalized = status.strip().capitalize()
    if normalized not in VALID_TASK_STATUSES:
        raise HTTPException(status_code=400, detail="Invalid status")
    return normalized


def day_range(value: datetime) -> dict:
    day_start = value.replace(hour=0, minute=0, second=0, microsecond=0)
    return {"$gte": day_start, "$lt": day_start + timedelta(days=1)}


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
                    }, {
                        '$sort': {
                            'createdAt': 1
                        }
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


async def get_task_details(task_id: ObjectId, user_id: ObjectId, company_id: Optional[ObjectId] = None):
    try:
        base_search_pipeline = task_details_pipeline(user_id)
        match_stage = {"_id": task_id}
        if company_id:
            match_stage["company_id"] = company_id
        base_search_pipeline.insert(0, {
            '$match': match_stage
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
            description = (task_data.pop("description", None) or "").strip()
            if not task_data.get("date"):
                raise HTTPException(status_code=400, detail="Date is required")
            if not task_data.get("due_date"):
                raise HTTPException(status_code=400, detail="Due date is required")
            if not description:
                raise HTTPException(status_code=400, detail="Description is required")

            created_by = (
                parse_object_id(task_data["created_by"], "created by")
                if task_data.get("created_by")
                else None
            )
            assigned_to = (
                parse_object_id(task_data["assigned_to"], "assigned to")
                if task_data.get("assigned_to")
                else None
            )
            if created_by is None:
                created_by = user_id
            if assigned_to is None:
                raise HTTPException(status_code=400, detail="Assigned user is required")

            requester_is_admin = await is_company_admin(company_id, user_id)
            if created_by != user_id and not requester_is_admin:
                raise HTTPException(status_code=403, detail="Only admins can create tasks for another user")

            await ensure_company_user(company_id, created_by, "created by")
            await ensure_company_user(company_id, assigned_to, "assigned to")

            task_data.update({
                "number": new_task_counter["final_counter"] if new_task_counter["success"] else None,
                "status": "Open",
                "description": description,
                "company_id": company_id,
                "createdAt": security.now_utc(),
                "updatedAt": security.now_utc(),
                "created_by": created_by,
                "assigned_to": assigned_to,
            })

            result = await to_do_list_collection.insert_one(task_data, session=session)
            to_do_list_id = result.inserted_id

            note_read = assigned_to == user_id
            await to_do_list_description_collection.insert_one({
                "to_do_list_id": to_do_list_id,
                "user_id": user_id,
                "type": "text",
                "description": description,
                "company_id": company_id,
                "createdAt": security.now_utc(),
                "updatedAt": security.now_utc(),
                "sender_id": user_id,
                "receiver_id": assigned_to,
                "read": note_read,
                "read_at": security.now_utc() if note_read else None,
                "file_name": None,
                "note_public_id": None,
            }, session=session)

            # ✅ Transaction committed successfully here
            await session.commit_transaction()

        except PyMongoError as e:
            # 🔴 Database / transaction errors
            await session.abort_transaction()
            print("Transaction Error:", e)
            raise HTTPException(status_code=500, detail="Database transaction failed")

        except HTTPException:
            await session.abort_transaction()
            raise

        except Exception as e:
            # 🔴 Any other unexpected errors
            await session.abort_transaction()
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
        response_data = await get_task_details(to_do_list_id, user_id, company_id)

        sender_id = user_id
        receiver_id = assigned_to

        await send_task_data_event_to_participants(
            to_do_list_id,
            company_id,
            sender_id,
            "new_task_created",
            "created_by",
            {"created_by": created_by, "assigned_to": assigned_to},
        )

        if receiver_id and receiver_id != sender_id:
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
        company_id = ObjectId(data.get("company_id"))
        task_oid = ObjectId(task_id)
        await get_task_for_user(
            task_oid,
            company_id,
            user_id,
            projection={"_id": 1, "created_by": 1, "assigned_to": 1},
        )
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
        task_oid = parse_object_id(to_do_list_id, "task id")

        # 1) جيب المهمة لتحدد الطرف الثاني
        task = await get_task_for_user(
            task_oid,
            company_id,
            sender_id,
            projection={"created_by": 1, "assigned_to": 1}
        )

        created_by = task.get("created_by")
        assigned_to = task.get("assigned_to")

        if sender_id == created_by:
            receiver_id = assigned_to
        elif sender_id == assigned_to:
            receiver_id = created_by
        else:
            raise HTTPException(status_code=403, detail="User not part of this task")

        normalized_note_type = (note_type or "text").strip()
        if normalized_note_type.lower() == 'text':
            note = (note or "").strip()
            if not note:
                raise HTTPException(status_code=400, detail="Note is required")
        else:
            if media_note is None:
                raise HTTPException(status_code=400, detail="File is required")
            if normalized_note_type.lower() == 'image':
                result = await upload_image(media_note, folder="to do list descriptions")
            else:
                result = await upload_file(media_note, folder="to do list descriptions")
            file_name = result.get("file_name")
            note = result.get("url")
            note_public_id = result.get("public_id")
            if not note or not note_public_id:
                raise HTTPException(status_code=500, detail="Failed to upload note file")

        note_read = receiver_id == sender_id

        # 2) احفظ الرسالة مع sender/receiver
        note_dict = {
            "description": note,
            "user_id": sender_id,
            "sender_id": sender_id,
            "receiver_id": receiver_id,
            "createdAt": security.now_utc(),
            "updatedAt": security.now_utc(),
            "company_id": company_id,
            "to_do_list_id": task_oid,
            "file_name": file_name,
            "type": normalized_note_type,
            "read": note_read,
            "read_at": security.now_utc() if note_read else None,
            "note_public_id": note_public_id,
        }

        result = await to_do_list_description_collection.insert_one(note_dict)
        sender_note = await get_description_note_data(result.inserted_id, sender_id)

        await send_note_event_to_participants(
            task,
            task_oid,
            result.inserted_id,
            sender_id,
        )
        if receiver_id and receiver_id != sender_id:
            # badge للمستلم فقط
            unread_total = await to_do_list_description_collection.count_documents({
                "company_id": company_id,
                "receiver_id": receiver_id,
                "read": False
            })

            await manager.send_to_user(str(receiver_id), {
                "type": "chat_unread",
                "task_id": str(task_oid),
                # "preview": (note_data.get("description") or "")[:60],
                "unread_total": unread_total
            })

        return {"ok": True, "data": jsonable_encoder(sender_note)}

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
        task_oid = parse_object_id(task_id, "task id")
        await get_task_for_user(
            task_oid,
            company_id,
            user_id,
            projection={"_id": 1, "created_by": 1, "assigned_to": 1},
        )
        now = security.now_utc()

        # علّم الرسائل الموجّهة لهذا المستخدم فقط كمقروءة
        await to_do_list_description_collection.update_many(
            {
                "company_id": company_id,
                "to_do_list_id": task_oid,
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

    except HTTPException:
        raise
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


@router.patch("/update_task/{task_id}")
async def update_task(
        task_id: str,
        to_do_list: ToDoListModel,
        data: dict = Depends(security.get_current_user),
):
    try:
        company_id = ObjectId(data.get("company_id"))
        user_id = ObjectId(data.get("sub"))
        task_oid = parse_object_id(task_id, "task id")
        existing_task = await get_task_for_user(
            task_oid,
            company_id,
            user_id,
            edit_only=True,
        )
        user_is_admin = await is_company_admin(company_id, user_id)
        task_data = to_do_list.model_dump(exclude_unset=True)
        update_note = (task_data.pop("description", None) or "").strip()

        update_doc: dict[str, Any] = {"updatedAt": security.now_utc()}
        if "date" in task_data:
            if task_data["date"] is None:
                raise HTTPException(status_code=400, detail="Date is required")
            update_doc["date"] = task_data["date"]
        if "due_date" in task_data:
            if task_data["due_date"] is None:
                raise HTTPException(status_code=400, detail="Due date is required")
            update_doc["due_date"] = task_data["due_date"]
        if "assigned_to" in task_data:
            assigned_to = parse_object_id(task_data["assigned_to"], "assigned to")
            await ensure_company_user(company_id, assigned_to, "assigned to")
            update_doc["assigned_to"] = assigned_to
        if "created_by" in task_data and task_data.get("created_by"):
            created_by = parse_object_id(task_data["created_by"], "created by")
            if created_by != existing_task.get("created_by") and not user_is_admin:
                raise HTTPException(status_code=403, detail="Only admins can change Created By")
            await ensure_company_user(company_id, created_by, "created by")
            update_doc["created_by"] = created_by

        updated_task = await to_do_list_collection.find_one_and_update(
            {"_id": task_oid, "company_id": company_id},
            {"$set": update_doc},
            return_document=ReturnDocument.AFTER,
        )
        if not updated_task:
            raise HTTPException(status_code=404, detail="Task not found")

        removed_participants = task_participant_ids(existing_task) - task_participant_ids(updated_task)
        if removed_participants:
            now = security.now_utc()
            await to_do_list_description_collection.update_many(
                {
                    "company_id": company_id,
                    "to_do_list_id": task_oid,
                    "receiver_id": {"$in": list(removed_participants)},
                    "read": False,
                },
                {
                    "$set": {
                        "read": True,
                        "read_at": now,
                        "updatedAt": now,
                    }
                },
            )
            for participant_id in removed_participants:
                await send_unread_total(company_id, participant_id)

        if update_note:
            created_by = updated_task.get("created_by")
            assigned_to = updated_task.get("assigned_to")
            receiver_id = assigned_to if user_id != assigned_to else created_by
            note_read = receiver_id == user_id
            note_result = await to_do_list_description_collection.insert_one({
                "description": update_note,
                "user_id": user_id,
                "sender_id": user_id,
                "receiver_id": receiver_id,
                "createdAt": security.now_utc(),
                "updatedAt": security.now_utc(),
                "company_id": company_id,
                "to_do_list_id": task_oid,
                "file_name": None,
                "type": "text",
                "read": note_read,
                "read_at": security.now_utc() if note_read else None,
                "note_public_id": None,
            })
            await send_note_event_to_participants(
                updated_task,
                task_oid,
                note_result.inserted_id,
                user_id,
            )
            await send_unread_total(company_id, receiver_id)

        response_data = await get_task_details(task_oid, user_id, company_id)
        await send_task_updated_event(
            existing_task,
            updated_task,
            task_oid,
            company_id,
            user_id,
        )
        return {"task": response_data}

    except HTTPException:
        raise
    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/delete_task/{task_id}")
async def delete_task(
        task_id: str,
        data: dict = Depends(security.get_current_user),
):
    try:
        company_id = ObjectId(data.get("company_id"))
        user_id = ObjectId(data.get("sub"))
        task_oid = parse_object_id(task_id, "task id")
        task = await get_task_for_user(
            task_oid,
            company_id,
            user_id,
            edit_only=True,
        )

        notes = await to_do_list_description_collection.find({
            "to_do_list_id": task_oid,
            "company_id": company_id,
        }).to_list(None)
        delete_failures = []
        for note in notes:
            public_id = note.get("note_public_id")
            if public_id and not await delete_file_from_server(public_id):
                delete_failures.append(note.get("file_name") or str(note.get("_id")))
        if delete_failures:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to delete files from storage: {', '.join(delete_failures)}"
            )

        await to_do_list_description_collection.delete_many({
            "to_do_list_id": task_oid,
            "company_id": company_id,
        })
        result = await to_do_list_collection.delete_one({
            "_id": task_oid,
            "company_id": company_id,
        })
        if result.deleted_count != 1:
            raise HTTPException(status_code=500, detail="Failed to delete task")

        delete_event = {
            "type": "task_deleted",
            "data": {"_id": str(task_oid)},
            "deleted_by": str(user_id),
        }
        await send_task_event_to_participants(task, delete_event)
        await send_unread_total(company_id, task.get("created_by"))
        await send_unread_total(company_id, task.get("assigned_to"))
        return {"_id": str(task_oid)}

    except HTTPException:
        raise
    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/delete_task_description_note/{note_id}")
async def delete_task_description_note(
        note_id: str,
        data: dict = Depends(security.get_current_user),
):
    try:
        company_id = ObjectId(data.get("company_id"))
        user_id = ObjectId(data.get("sub"))
        note_oid = parse_object_id(note_id, "note id")
        note = await to_do_list_description_collection.find_one({
            "_id": note_oid,
            "company_id": company_id,
        })
        if not note:
            raise HTTPException(status_code=404, detail="Note not found")

        task_oid = note.get("to_do_list_id")
        task = await get_task_for_user(
            task_oid,
            company_id,
            user_id,
            projection={"created_by": 1, "assigned_to": 1},
        )
        if note.get("user_id") != user_id and not await is_company_admin(company_id, user_id):
            raise HTTPException(status_code=403, detail="You can delete only your own notes")

        public_id = note.get("note_public_id")
        if public_id and not await delete_file_from_server(public_id):
            raise HTTPException(status_code=500, detail="Failed to delete note file from storage")

        await to_do_list_description_collection.delete_one({
            "_id": note_oid,
            "company_id": company_id,
        })
        note_event = {
            "type": "task_note_deleted",
            "task_id": str(task_oid),
            "note_id": str(note_oid),
            "deleted_by": str(user_id),
        }
        await send_task_event_to_participants(task, note_event)
        await send_unread_total(company_id, task.get("created_by"))
        await send_unread_total(company_id, task.get("assigned_to"))
        return {"_id": str(note_oid), "task_id": str(task_oid)}

    except HTTPException:
        raise
    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/search_engine_for_to_do_list")
async def search_engine_for_to_do_list(filter_tasks: TaskModel,
                                       data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        user_id = ObjectId(data.get("sub"))
        base_search_pipeline: list[dict] = task_details_pipeline(user_id)
        match_stage = {}
        if company_id:
            match_stage["company_id"] = company_id
        if not await is_company_admin(company_id, user_id):
            match_stage["$or"] = [
                {
                    "created_by": user_id
                },
                {
                    "assigned_to": user_id
                }
            ]
        if filter_tasks.number:
            match_stage["number"] = filter_tasks.number
        if filter_tasks.created_by:
            match_stage["created_by"] = filter_tasks.created_by
        if filter_tasks.assigned_to:
            match_stage["assigned_to"] = filter_tasks.assigned_to
        if filter_tasks.date:
            match_stage["date"] = day_range(filter_tasks.date)
        if filter_tasks.due_date:
            match_stage["due_date"] = day_range(filter_tasks.due_date)
        if filter_tasks.status:
            match_stage["status"] = normalize_status(filter_tasks.status)
        if filter_tasks.from_date or filter_tasks.to_date:
            match_stage['date'] = match_stage.get("date", {})
            if filter_tasks.from_date:
                match_stage['date']["$gte"] = filter_tasks.from_date.replace(
                    hour=0,
                    minute=0,
                    second=0,
                    microsecond=0,
                )
            if filter_tasks.to_date:
                match_stage['date']["$lt"] = filter_tasks.to_date.replace(
                    hour=0,
                    minute=0,
                    second=0,
                    microsecond=0,
                ) + timedelta(days=1)

        base_search_pipeline.insert(0, {"$match": match_stage})
        base_search_pipeline.insert(1, {"$sort": {"due_date": 1}})
        cursor = await to_do_list_collection.aggregate(base_search_pipeline)
        results = await cursor.to_list(None)
        return {"tasks": results}


    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


async def get_admin_id(company_id: ObjectId) -> list:
    try:
        list_od_admin_users_id = await users_collection.find({"company_id": company_id, "is_admin": True},
                                                             {"_id": 1}).to_list(None)
        return [user["_id"] for user in list_od_admin_users_id]

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.patch("/update_task_status/{task_id}")
async def update_task_status(
        task_id: str,
        task: UpdateTaskModel,
        data: dict = Depends(security.get_current_user),
):
    try:
        company_id = ObjectId(data.get("company_id"))
        user_id = ObjectId(data.get("sub"))
        oid = parse_object_id(task_id, "task id")
        status = normalize_status(task.status)
        existing_task = await get_task_for_user(
            oid,
            company_id,
            user_id,
            edit_only=True,
        )
    except InvalidId:
        raise HTTPException(status_code=400, detail="Invalid task_id")

    updated_task = await to_do_list_collection.find_one_and_update(
        {"_id": oid, "company_id": company_id},
        {"$set": {"status": status, "updatedAt": security.now_utc()}},
        return_document=ReturnDocument.AFTER,
    )

    if not updated_task:
        raise HTTPException(status_code=404, detail="Task not found")

    await send_task_event_to_participants(existing_task, {
        "type": "task_status_updated",
        "data": {"status": status, "_id": str(oid)},
        "updated_by": str(user_id),
    })

    return {"_id": str(oid), "status": status}

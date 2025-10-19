from typing import Optional

from bson import ObjectId
from fastapi import APIRouter, Body, HTTPException, Depends
from pydantic import BaseModel
from pymongo import ReturnDocument
from app.core import security
from app.database import get_collection
from datetime import datetime, timezone

from app.routes.auth import companies
from app.websocket_config import manager

router = APIRouter()
job_tasks_collection = get_collection("all_job_tasks")


def serializer(task: dict) -> dict:
    task["_id"] = str(task["_id"])
    if task['company_id']:
        task['company_id'] = str(task['company_id'])
    for key, value in task.items():
        if isinstance(value, datetime):
            task[key] = value.isoformat()
    return task


class JobTaskModel(BaseModel):
    name_en: Optional[str] = None
    name_ar: Optional[str] = None
    category: Optional[str] = None
    points: Optional[float] = None


@router.get("/get_all_job_tasks")
async def get_all_job_tasks(data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        results = await job_tasks_collection.find({"company_id": company_id}).to_list(None)
        return {"job_tasks": [serializer(j) for j in results]}

    except HTTPException:
        raise
    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=f"failed: {str(e)}")


@router.post("/add_new_job_task")
async def add_new_job_task(task: JobTaskModel, data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        task = task.model_dump(exclude_unset=True)
        task_dict = {
            "company_id": company_id,
            "name_en": task['name_en'],
            "name_ar": task['name_ar'],
            "category": task['category'],
            "points": task['points'],
            "createdAt": security.now_utc(),
            "updatedAt": security.now_utc(),
        }
        result = await job_tasks_collection.insert_one(task_dict)
        task_dict['_id'] = str(result.inserted_id)
        serialized = serializer(task_dict)
        await manager.broadcast({
            "type": "job_task_added",
            "data": serialized
        })

    except HTTPException:
        raise
    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=f"failed: {str(e)}")


@router.patch("/update_job_task/{task_id}")
async def update_job_task(task_id: str, task: JobTaskModel, _: dict = Depends(security.get_current_user)):
    try:
        task = task.model_dump(exclude_unset=True)
        task.update({
            "updatedAt": security.now_utc(),
        })
        task_id = ObjectId(task_id)
        result = await job_tasks_collection.find_one_and_update(
            {"_id": task_id}, {"$set": task}, return_document=ReturnDocument.AFTER
        )
        if not result:
            raise HTTPException(status_code=404, detail="Task not found")

        serialized = serializer(result)
        await manager.broadcast({
            "type": "job_task_updated",
            "data": serialized
        })

    except HTTPException:
        raise
    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=f"failed: {str(e)}")


@router.delete("/delete_job_task/{task_id}")
async def delete_job_task(task_id: str, _: dict = Depends(security.get_current_user)):
    try:
        result = await job_tasks_collection.delete_one({"_id": ObjectId(task_id)})
        if result.deleted_count == 1:
            await manager.broadcast({
                "type": "job_task_deleted",
                "data": {"_id": task_id}
            })
            return {"message": "Task removed successfully!"}
        else:
            raise HTTPException(status_code=404, detail="Task not found")

    except HTTPException:
        raise
    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=f"failed: {str(e)}")

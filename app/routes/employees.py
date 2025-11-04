import copy
from typing import Optional, List, Any

from bson import ObjectId
from fastapi import APIRouter, Body, HTTPException, Depends
from pydantic import BaseModel
from pymongo import ReturnDocument
from app.core import security
from app.database import get_collection
from datetime import datetime, timezone

from app.routes.counters import create_custom_counter
from app.websocket_config import manager

router = APIRouter()
employees_collection = get_collection("employees")


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


class EmployeesModel(BaseModel):
    name: Optional[str] = None
    gender: Optional[str] = None
    nationality: Optional[str] = None
    date_of_birth: Optional[datetime] = None
    martial_status: Optional[str] = None
    national_id_or_passport_number: Optional[str] = None
    email: Optional[str] = None
    phone: Optional[str] = None
    address: Optional[str] = None
    emergency_contact_name: Optional[str] = None
    emergency_contact_number: Optional[str] = None
    job_title: Optional[str] = None
    hire_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    job_description: Optional[str] = None
    status: Optional[str] = None
    department: Optional[List[str]] = None


pipeline: list[dict[str, Any]] = [
    {
        '$lookup': {
            'from': 'all_lists_values',
            'localField': 'gender',
            'foreignField': '_id',
            'as': 'gender_details'
        }
    }, {
        '$unwind': {
            'path': '$gender_details',
            'preserveNullAndEmptyArrays': True
        }
    }, {
        '$lookup': {
            'from': 'all_lists_values',
            'localField': 'nationality',
            'foreignField': '_id',
            'as': 'nationality_details'
        }
    }, {
        '$unwind': {
            'path': '$nationality_details',
            'preserveNullAndEmptyArrays': True
        }
    }, {
        '$lookup': {
            'from': 'all_lists_values',
            'localField': 'martial_status',
            'foreignField': '_id',
            'as': 'martial_status_details'
        }
    }, {
        '$unwind': {
            'path': '$martial_status_details',
            'preserveNullAndEmptyArrays': True
        }
    }, {
        '$lookup': {
            'from': 'all_lists_values',
            'localField': 'status',
            'foreignField': '_id',
            'as': 'status_details'
        }
    }, {
        '$unwind': {
            'path': '$status_details',
            'preserveNullAndEmptyArrays': True
        }
    }, {
        '$addFields': {
            'gender_type': {
                '$ifNull': [
                    '$gender_details.name', None
                ]
            },
            'nationality_name': {
                '$ifNull': [
                    '$nationality_details.name', None
                ]
            },
            'martial_status_type': {
                '$ifNull': [
                    '$martial_status_details.name', None
                ]
            },
            'status_type': {
                '$ifNull': [
                    '$status_details.name', None
                ]
            }
        }
    }, {
        '$project': {
            'gender_details': 0,
            'nationality_details': 0,
            'martial_status_details': 0,
            'status_details': 0
        }
    }
]


async def get_employee_details(employee_id: ObjectId):
    new_pipeline = copy.deepcopy(pipeline)
    new_pipeline.insert(1, {
        "$match": {
            "_id": employee_id
        }
    })
    cursor = await employees_collection.aggregate(new_pipeline)
    result = await cursor.next()
    return result


@router.get("/get_all_employees")
async def get_all_employees(data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        all_employees_pipeline = copy.deepcopy(pipeline)
        all_employees_pipeline.insert(1, {
            "$match": {
                "company_id": company_id
            }
        })
        cursor = await employees_collection.aggregate(all_employees_pipeline)
        results = await cursor.to_list(None)
        return {"employees": [serializer(r) for r in results]}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/create_employee")
async def create_employee(employee: EmployeesModel, data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        employee = employee.model_dump(exclude_unset=True)
        ids_list = ["gender", "nationality", "martial_status", "status"]
        for filed in ids_list:
            if employee.get(filed):
                employee[filed] = ObjectId(employee[filed]) if employee[filed] else None
        new_employee_counter = await create_custom_counter("EN", "E", data)
        employee["company_id"] = company_id
        employee["createdAt"] = security.now_utc()
        employee["updatedAt"] = security.now_utc()
        employee["employee_number"] = new_employee_counter['final_counter'] if new_employee_counter[
            'success'] else None

        result = await employees_collection.insert_one(employee)
        new_employee = await get_employee_details(result.inserted_id)
        serialized = serializer(new_employee)
        await manager.broadcast({
            "type": "employee_added",
            "data": serialized
        })

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.patch("/update_employee/{employee_id}")
async def update_employee(employee_id: str, employee: EmployeesModel, _: dict = Depends(security.get_current_user)):
    try:
        employee = employee.model_dump(exclude_unset=True)
        ids_list = ["gender", "nationality", "martial_status", "status"]
        for filed in ids_list:
            if employee.get(filed):
                employee[filed] = ObjectId(employee[filed]) if employee[filed] else None
        employee["updatedAt"] = security.now_utc()
        result = await employees_collection.update_one({"_id": ObjectId(employee_id)}, {"$set": employee})
        if result.modified_count > 0:
            updated_employee = await get_employee_details(ObjectId(employee_id))
            serialized = serializer(updated_employee)
            await manager.broadcast({
                "type": "employee_updated",
                "data": serialized
            })

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))



@router.delete("/delete_employee/{employee_id}")
async def delete_employee(employee_id: str, _: dict = Depends(security.get_current_user)):
    try:
        result = await employees_collection.delete_one({"_id": ObjectId(employee_id)})
        if result.deleted_count == 1:
            await manager.broadcast({
                "type": "employee_deleted",
                "data": {"_id": employee_id}
            })
            return {"message": "Branch removed successfully!"}
        else:
            raise HTTPException(status_code=404, detail="Branch not found")

    except Exception as error:
        return {"message": str(error)}

from bson import ObjectId
from fastapi import APIRouter, Depends

from app.core import security
from .collections import (
    employees_collection,
    payroll_collection,
    payroll_period_details_collection,
)

router = APIRouter()


@router.get("/get_payroll_for_lov")
async def get_payroll_for_lov(data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        cursor = await payroll_collection.aggregate([
            {"$match": {"company_id": company_id}},
            {"$set": {"_id": {"$toString": "$_id"}}},
            {"$project": {
                "_id": 1,
                "name": 1
            }}
        ])
        results = await cursor.to_list(None)
        return {"all_payrolls": results}

    except Exception:
        raise


@router.get("/get_payroll_periods_for_lov/{payroll_id}")
async def get_payroll_periods_for_lov(payroll_id: str, _: dict = Depends(security.get_current_user)):
    try:
        payroll_id = ObjectId(payroll_id)
        cursor = await payroll_period_details_collection.aggregate([
            {"$match": {"payroll_id": payroll_id, "status": "Active"}},
            {"$set": {"_id": {"$toString": "$_id"}}},
            {"$project": {
                "_id": 1,
                "period_name": 1
            }},
            {"$sort": {"period_name": -1}}
        ])
        results = await cursor.to_list(None)
        return {"all_periods": results}

    except Exception:
        raise


@router.get("/get_all_employees_for_payroll_runs_lov/{payroll_id}")
async def get_all_employees_for_payroll_runs_lov(payroll_id: str, _: dict = Depends(security.get_current_user)):
    try:
        payroll_id = ObjectId(payroll_id)
        cursor = await employees_collection.aggregate([
            {"$match": {"payroll": payroll_id}},
            {"$set": {"_id": {"$toString": "$_id"}}},
            {"$project": {
                "_id": 1,
                "full_name": 1
            }},
        ])
        results = await cursor.to_list(None)
        return {"all_employees": results}

    except Exception:
        raise

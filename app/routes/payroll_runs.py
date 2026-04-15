import copy
from datetime import datetime
from typing import Optional, Any
from bson import ObjectId
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from app import database
from app.core import security
from app.database import get_collection
from app.routes.car_trading import PyObjectId

router = APIRouter()
payroll_runs_collection = get_collection("payroll_runs")
payroll_collection = get_collection("payroll")
payroll_period_details_collection = get_collection("payroll_period_details")
employees_collection = get_collection("employees")
employees_payrolls_collection = get_collection("employees_payrolls")
payroll_elements_collection = get_collection("payroll_elements")


class PayrollRunModel(BaseModel):
    payroll_id: Optional[PyObjectId] = None
    period_id: Optional[PyObjectId] = None
    employee_id: Optional[PyObjectId] = None
    element_id: Optional[PyObjectId] = None


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
            {"$match": {"payroll": payroll_id, "status": "Active", "person_type": "Employee"}},
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


@router.post("/run_payroll")
async def run_payroll(run: PayrollRunModel, _: dict = Depends(security.get_current_user)):
    try:
        if run.element_id:
            result = await payroll_elements_collection.find_one({"_id": run.element_id})
            if result:
                statement: str = result.get("function")
                if statement.lower() == 'PY_INPUT_VALUE_FF':
                    await py_input_value_ff(run.employee_id, run.period_id, run.element_id)

    except Exception:
        raise


async def py_input_value_ff(employee_id: ObjectId, period_id: ObjectId, element_id: ObjectId):
    try:
        period_document = await payroll_period_details_collection.find_one({"_id": period_id})
        period_start_date = period_document.get("start_date")
        period_end_date = period_document.get("end_date")
        if not period_start_date or not period_end_date:
            raise HTTPException(status_code=400, detail="period_start_date and period_end_date are required")
        employee_document = await employees_collection.find_one({"_id": employee_id})
        if not employee_document:
            raise HTTPException(status_code=404, detail="Employee not found")
        employee_hire_date = employee_document.get("hire_date") or datetime.min
        employee_end_date = employee_document.get("end_date") or datetime.max

        employee_payroll = await employees_payrolls_collection.find_one(
            {"employee_id": employee_id, "name": element_id})

        employee_payroll_element_start_date = employee_payroll.get("start_date") or datetime.min
        employee_payroll_element_end_date = employee_payroll.get("end_date") or datetime.max
        value = employee_payroll.get("value", 0)

        date1 = max(employee_hire_date, employee_payroll_element_start_date, period_start_date)
        date2 = min(employee_end_date, employee_payroll_element_end_date, period_end_date)
        result_for_date1_and_date_2 = abs((date2 - date1).days) + 1
        print(result_for_date1_and_date_2)

        result_for_periods_dates = abs((period_start_date - period_end_date).days) + 1
        print(result_for_periods_dates)

        final_result_for_dates = result_for_date1_and_date_2 / result_for_periods_dates

        final_value = value * final_result_for_dates
        print(round(final_value,2))

    except Exception:
        raise

#
# @router.get("/get_elements_for_selected_employee/{employee_id}")
# async def get_elements_for_selected_employee(employee_id: str, _: dict = Depends(security.get_current_user)):
#     try:
#         employee_id = ObjectId(employee_id)
#         cursor = await employees_payrolls_collection.aggregate([
#             {
#                 '$match': {
#                     'employee_id': employee_id
#                 }
#             }, {
#                 '$lookup': {
#                     'from': 'payroll_elements',
#                     'localField': 'name',
#                     'foreignField': '_id',
#                     'as': 'element_details'
#                 }
#             }, {
#                 '$addFields': {
#                     '_id': {
#                         '$toString': '$_id'
#                     },
#                     'element_name': {
#                         '$ifNull': [
#                             {
#                                 '$first': '$element_details.name'
#                             }, None
#                         ]
#                     }
#                 }
#             }, {
#                 '$project': {
#                     '_id': 1,
#                     'element_name': 1
#                 }
#             }
#         ])
#         results = await cursor.to_list(None)
#         return {"all_elements": results}
#
#     except Exception:
#         raise

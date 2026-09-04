"""Select the employees included in a payroll run."""

from typing import Optional

from bson import ObjectId

from ..collections import employees_collection
from .context import PayrollPeriod


async def get_payroll_employees(
    payroll_id: Optional[ObjectId],
    employee_id: Optional[ObjectId],
    period: PayrollPeriod,
) -> list[dict]:
    period_start_date = period.start_date
    period_end_date = period.end_date
    all_employees = []

    employee_filter = {
        "payroll": payroll_id,
        "hire_date": {"$lte": period_end_date},
        "$or": [
            {"end_date": {"$gte": period_start_date}},
            {"end_date": None}
        ]
    }

    employee_projection = {
        "_id": 1,
        "hire_date": 1,
        "end_date": 1,
        "full_name": 1,
        "legislation": 1,
    }

    if employee_id:
        employee_filter["_id"] = employee_id
        employee_document = await employees_collection.find_one(employee_filter, employee_projection)
        if employee_document:
            all_employees.append(employee_document)
    else:
        employees = await employees_collection.find(employee_filter, employee_projection).to_list(None)
        all_employees.extend(employees)

    return all_employees

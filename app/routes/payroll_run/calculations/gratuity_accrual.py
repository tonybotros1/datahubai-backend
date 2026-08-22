from datetime import datetime
from typing import Optional

from bson import ObjectId
from fastapi import HTTPException

from app.routes.payroll_runs_widgets.helpers_functions import (
    get_employee_element_value,
    get_previous_gratuity_accrual,
)
from ..collections import legislations_collection


async def py_gratuity_accrual_ff(employee_id: ObjectId, employee_hire_date: datetime, employee_end_date: datetime,
                                 element_start: datetime, element_end: datetime, period_start_date: datetime,
                                 period_end_date: datetime, based_element_id: ObjectId, legislation: ObjectId,
                                 based_value: Optional[float] = None,
                                 legislation_document: Optional[dict] = None):
    try:
        basic_salary = based_value
        if basic_salary is None:
            basic_salary = await get_employee_element_value(based_element_id, employee_id)
        legislation_doc = legislation_document
        if legislation_doc is None:
            legislation_doc = await legislations_collection.find_one({"_id": legislation})

        if not legislation_doc:
            raise HTTPException(status_code=404, detail="Legislation not found")

        gratuity_first_5_years = legislation_doc.get("gratuity_first_5_years", 21)
        gratuity_after_5_years = legislation_doc.get("gratuity_after_5_years", 30)

        date1 = max(employee_hire_date, element_start)
        date2 = min(employee_end_date, element_end, period_end_date)

        if date2 < date1:
            return 0

        total_service_days = (date2 - employee_hire_date).days + 1
        first_5_years_days = min(total_service_days, 5 * 365)
        after_5_years_days = max(total_service_days - (5 * 365), 0)
        gratuity_days_first_5 = (first_5_years_days / 365) * gratuity_first_5_years
        gratuity_days_after_5 = (after_5_years_days / 365) * gratuity_after_5_years
        total_gratuity_days = gratuity_days_first_5 + gratuity_days_after_5
        total_gratuity_liability = (total_gratuity_days * basic_salary) / 30
        previous_accrued_amount = await get_previous_gratuity_accrual(
            employee_id=employee_id,
        )
        current_period_accrual = (total_gratuity_liability - previous_accrued_amount)
        return round(current_period_accrual, 2)
    except Exception as e:
        raise e

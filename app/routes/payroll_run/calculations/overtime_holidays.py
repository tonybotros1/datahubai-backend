from datetime import datetime
from typing import Optional

from bson import ObjectId
from fastapi import HTTPException

from app.routes.payroll_runs_widgets.helpers_functions import (
    get_employee_element_value,
    get_period_days,
)
from ..collections import legislations_collection


async def py_overtime_holidays_ff(employee_id: ObjectId, period_start_date: datetime, period_end_date: datetime,
                                  based_element_id: ObjectId, legislation: ObjectId, element_value: float,
                                  based_value: Optional[float] = None,
                                  legislation_document: Optional[dict] = None):
    try:
        value = based_value
        if value is None:
            value = await get_employee_element_value(based_element_id, employee_id, period_start_date, period_end_date)
        # No. of Month Days
        period_days = get_period_days(period_start_date, period_end_date)

        legislation_doc = legislation_document
        if legislation_doc is None:
            legislation_doc = await legislations_collection.find_one({"_id": legislation})
        if not legislation_doc:
            raise HTTPException(status_code=404, detail="Legislation not found")
        # No. of working hours
        working_hours = legislation_doc.get("number_of_working_hours_for_overtime_holidays", 0)

        total_value = element_value / working_hours / period_days * value
        return round(total_value, 2)

    except Exception as e:
        raise e

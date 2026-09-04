from datetime import datetime
from typing import Optional

from bson import ObjectId
from fastapi import HTTPException

from app.routes.employees import (
    NumberOfDaysForWorkingDaysModel,
    calculate_number_of_days,
)
from app.routes.payroll_runs_widgets.helpers_functions import (
    get_current_leave_used_days,
    get_employee_element_value,
    get_period_days,
)
from ..collections import legislations_collection


async def py_compassionate_leave_ff(leave_id: ObjectId, employee_id: ObjectId, period_start_date: datetime,
                                    period_end_date: datetime,
                                    based_element_id: ObjectId, legislation: ObjectId, leave_start_date: datetime,
                                    leave_end_date: datetime, user_data: dict,
                                    based_value: Optional[float] = None,
                                    legislation_document: Optional[dict] = None):
    try:
        value = based_value
        if value is None:
            value = await get_employee_element_value(based_element_id, employee_id, period_start_date, period_end_date)
        period_days = get_period_days(period_start_date, period_end_date)

        legislation_doc = legislation_document
        if legislation_doc is None:
            legislation_doc = await legislations_collection.find_one({"_id": legislation})
        if not legislation_doc:
            raise HTTPException(status_code=404, detail="Legislation not found")

        full_limit = legislation_doc.get("number_of_paid_days_for_compassionate_leave", 0)

        used_days_before = get_current_leave_used_days(period_start_date, leave_start_date)

        date1 = max(period_start_date, leave_start_date)
        date2 = min(period_end_date, leave_end_date)

        # l_days = (date2 - date1).days + 1
        number_of_days = await calculate_number_of_days(str(employee_id),
                                                        NumberOfDaysForWorkingDaysModel(start_date=date1,
                                                                                        end_date=date2,
                                                                                        leave_type=str(leave_id)),
                                                        user_data)
        l_days: int = number_of_days['working_days']

        remaining_days = l_days
        total_value = 0

        # === FULL PAID ===
        remaining_full = max(0, full_limit - used_days_before)
        full_paid_days = min(remaining_days, remaining_full)
        remaining_days -= full_paid_days

        if remaining_days > 0:
            total_value = value * (remaining_days / period_days)

        return round(total_value, 2), l_days

    except Exception:
        raise

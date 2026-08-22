from datetime import datetime
from typing import Optional

from bson import ObjectId

from app.routes.payroll_runs_widgets.helpers_functions import (
    get_employee_element_value,
    get_period_days,
)


async def py_annual_leave_entitlement_ff(employee_hire_date: datetime, employee_end_date: datetime,
                                         element_start: datetime,
                                         element_value: float, element_end: datetime, period_start_date: datetime,
                                         period_end_date: datetime, based_element_id: ObjectId, employee_id: ObjectId,
                                         based_value: Optional[float] = None):
    try:
        value = based_value
        if value is None:
            value = await get_employee_element_value(based_element_id, employee_id)
        date1 = max(employee_hire_date, element_start, period_start_date)
        date2 = min(employee_end_date, element_end, period_end_date)
        if date2 < date1:
            return 0, 0
        working_days = max((date2 - date1).days + 1, 0)
        period_days = get_period_days(period_start_date, period_end_date)

        if period_days == 0:
            l_days = 0
        else:
            l_days = element_value / 12 * (working_days / period_days)
        final_value = l_days / 30 * value
        return round(l_days, 2), round(final_value, 2)

    except Exception as e:
        raise e

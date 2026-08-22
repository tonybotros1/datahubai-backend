from datetime import datetime
from typing import Optional

from bson import ObjectId

from app.routes.employees import (
    NumberOfDaysForWorkingDaysModel,
    calculate_number_of_days,
)
from app.routes.payroll_runs_widgets.helpers_functions import (
    get_employee_element_value,
    get_period_days,
)


async def py_unpaid_leave_ff(leave_id: ObjectId, employee_id: ObjectId, period_start_date: datetime,
                             period_end_date: datetime,
                             based_element_id: ObjectId, leave_start_date: datetime, leave_end_date: datetime,
                             user_data: dict, based_value: Optional[float] = None):
    try:
        value = based_value
        if value is None:
            value = await get_employee_element_value(based_element_id, employee_id)
        period_days = get_period_days(period_start_date, period_end_date)

        date1 = max(period_start_date, leave_start_date)
        date2 = min(period_end_date, leave_end_date)
        # l_days = (date2 - date1).days + 1
        number_of_days = await calculate_number_of_days(str(employee_id),
                                                        NumberOfDaysForWorkingDaysModel(start_date=date1,
                                                                                        end_date=date2,
                                                                                        leave_type=str(leave_id)),
                                                        user_data)
        l_days: int = number_of_days['working_days']

        final_value = round(((value or 0) * (l_days / period_days)), 2)

        return final_value, l_days

    except Exception:
        raise

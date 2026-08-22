from datetime import datetime

from app.routes.payroll_runs_widgets.helpers_functions import get_period_days


async def py_input_value_ff(employee_hire_date: datetime, employee_end_date: datetime,
                            element_start: datetime,
                            element_value: float, element_end: datetime, period_start_date: datetime,
                            period_end_date: datetime):
    try:
        date1 = max(employee_hire_date, element_start, period_start_date)
        date2 = min(employee_end_date, element_end, period_end_date)
        # number_of_leave_days_dict = await get_leave_days(employee_id, date1, date2, company_id)

        # number_of_leave_days = number_of_leave_days_dict['number_of_leave_days']
        # working_days = max((date2 - date1).days + 1, 0) - number_of_leave_days
        working_days = max((date2 - date1).days + 1, 0)
        period_days = get_period_days(period_start_date, period_end_date)

        if period_days == 0:
            final_value = 0
        else:
            final_value = element_value * (working_days / period_days)

        return round(final_value, 2)

    except Exception as e:
        raise e

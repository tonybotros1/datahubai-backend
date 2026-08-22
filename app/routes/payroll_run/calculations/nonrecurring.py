from datetime import datetime

from app.routes.payroll_runs_widgets.helpers_functions import is_within_period


async def py_nonrecurring_ff(period_start_date: datetime, period_end_date: datetime,
                             element_start: datetime, element_end: datetime, element_value: float):
    try:
        if is_within_period(element_start, element_end, period_start_date, period_end_date):
            return element_value
        else:
            return None

    except Exception as e:
        raise e

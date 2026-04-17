from datetime import datetime, timedelta

from bson import ObjectId

from app.routes.employees import employees_leaves_collection, get_number_of_days_for_working_days, \
    NumberOfDaysForWorkingDaysModel


# ==== GET_LEAVE_DAYS ====
async def get_leave_days(employee_id: ObjectId, period_start_date: datetime, period_end_date: datetime) -> dict:
    try:
        leaves = await employees_leaves_collection.find(
            {"employee_id": employee_id, "start_date": {"$lte": period_end_date},
             "end_date": {"$gte": period_start_date}
             }).to_list(length=None)
        number_of_leave_days = 0
        current_date = period_start_date
        while current_date <= period_end_date:
            is_on_leave = any(
                leave["start_date"] <= current_date <= leave["end_date"] for leave in leaves)
            if is_on_leave:
                days_number = await get_number_of_days_for_working_days(str(employee_id),
                                                                        NumberOfDaysForWorkingDaysModel(
                                                                            start_date=current_date,
                                                                            end_date=current_date))
                if days_number['working_days'] == 1:
                    number_of_leave_days += 1

            current_date += timedelta(days=1)
        print(number_of_leave_days)
        return {"number_of_leave_days": number_of_leave_days}

    except Exception as e:
        raise e



# ==== GET_ELEMENT_VALUE ====
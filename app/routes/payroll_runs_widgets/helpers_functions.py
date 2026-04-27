from datetime import datetime, timedelta

from bson import ObjectId
from fastapi import APIRouter, HTTPException

from app.routes.employees import employees_leaves_collection, get_number_of_days, NumberOfDaysForWorkingDaysModel, \
    employees_payrolls_collection, leave_types_collection
from app.routes.payroll_elements import payroll_elements_based_elements_collection

router = APIRouter()


# ==== GET_PERIOD_DYS ====
def get_period_days(period_start_date: datetime, period_end_date: datetime):
    return max((period_end_date - period_start_date).days + 1, 0)


#
# # ==== GET_LEAVE_DAYS ====
# async def get_leave_days(employee_id: ObjectId, period_start_date: datetime, period_end_date: datetime,
#                          company_id: ObjectId) -> dict:
#     try:
#         annual_leave_doc = await leave_types_collection.find_one({"company_id": company_id, "code": "AL"}, {"_id": 1})
#         leaves = await employees_leaves_collection.find(
#             {
#                 "employee_id": employee_id,
#                 "status": "Posted",
#                 "start_date": {"$lte": period_end_date},
#                 "end_date": {"$gte": period_start_date},
#                 # "leave_type": annual_leave_doc["_id"]
#             }
#         ).to_list(length=None)
#
#         number_of_leave_days = 0
#         current_date = period_start_date
#
#         while current_date <= period_end_date:
#             matching_leave = next(
#                 (leave for leave in leaves if leave["start_date"] <= current_date <= leave["end_date"]),
#                 None
#             )
#
#             if matching_leave:
#                 days_number = await get_number_of_days(
#                     str(employee_id),
#                     NumberOfDaysForWorkingDaysModel(
#                         start_date=current_date,
#                         end_date=current_date,
#                         leave_type=str(matching_leave["leave_type"])
#                     )
#                 )
#
#                 if days_number['working_days'] == 1:
#                     number_of_leave_days += 1
#
#             current_date += timedelta(days=1)
#
#         return {"number_of_leave_days": number_of_leave_days}
#
#     except Exception as e:
#         raise e


# ==== GET_ELEMENT_VALUE ====
async def get_employee_element_value(element_id: ObjectId, employee_id: ObjectId) -> float:
    try:
        employee_payroll_element_doc = await employees_payrolls_collection.find_one({"_id": ObjectId(element_id)})
        if employee_payroll_element_doc:
            employee_payroll_element_value = employee_payroll_element_doc.get("value", 0)
            payroll_element_id: ObjectId = employee_payroll_element_doc.get("name", None)
            # employee_id = employee_payroll_element_doc.get("employee_id", None)
        else:
            employee_payroll_element_value = None
            payroll_element_id = element_id

        # if not payroll_element_id:
        #     raise HTTPException(status_code=404, detail="Payroll Element not found")
        # if not employee_payroll_element_value:
        #     raise HTTPException(status_code=404, detail="Employee Payroll Element value not found")

        payroll_element_based_elements_docs = await payroll_elements_based_elements_collection.find(
            {"payroll_element_id": payroll_element_id}).to_list(length=None)

        if len(payroll_element_based_elements_docs) == 0:
            if employee_payroll_element_value:
                return employee_payroll_element_value
            else:
                raise HTTPException(status_code=404, detail="no value found for this element")

        total_value = 0.0
        for element in payroll_element_based_elements_docs:
            element_id = element.get("name")
            element_type = (element.get("type") or "Add").strip().lower()
            employee_payroll_elements_docs = await employees_payrolls_collection.find(
                {"employee_id": ObjectId(employee_id), "name": ObjectId(element_id)}).to_list(length=None)
            element_total = sum(
                float(doc.get("value", 0) or 0)
                for doc in employee_payroll_elements_docs
            )
            if element_type == "subtract":
                total_value -= element_total
            else:
                total_value += element_total
        return total_value
    except Exception:
        raise


async def get_used_leave_days(leave_code: str, employee_id: ObjectId, leave_start_date: datetime,
                              period_start_date: datetime) -> float:
    year_start = datetime(leave_start_date.year, 1, 1)
    year_end = datetime(leave_start_date.year, 12, 31)
    leave_type = await leave_types_collection.find_one({"code": leave_code})
    leave_type_id = leave_type["_id"]

    results = await employees_leaves_collection.find({
        'employee_id': employee_id,
        'leave_type': leave_type_id,
        'status': 'Posted',
        'start_date': {'$lt': leave_start_date},
        'end_date': {'$gte': year_start}
    }).to_list(length=None)
    total_days = 0
    for result in results:
        l_s_d = result["start_date"]
        l_e_d = result["end_date"]

        date1 = max(year_start, l_s_d)
        date2 = min(year_end, l_e_d)
        total_days += max((date2 - date1).days + 1, 0)

    total_days = total_days + get_current_leave_used_days(period_start_date, leave_start_date)

    return total_days


# this function is to get number of days used in this leave if it started before this period
def get_current_leave_used_days(period_start_date: datetime, leave_start_date: datetime) -> float:
    total_days = max((period_start_date - leave_start_date).days, 0)
    return total_days

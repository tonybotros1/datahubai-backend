from datetime import datetime, timedelta

from bson import ObjectId
from fastapi import APIRouter, HTTPException

from app.routes.employees import employees_leaves_collection, get_number_of_days, NumberOfDaysForWorkingDaysModel, \
    employees_payrolls_collection
from app.routes.payroll_elements import payroll_elements_based_elements_collection

router = APIRouter()


# ==== GET_PERIOD_DYS ====
def get_period_days(period_start_date: datetime, period_end_date: datetime):
    return max((period_end_date - period_start_date).days + 1, 0)


# ==== GET_LEAVE_DAYS ====
async def get_leave_days(employee_id: ObjectId, period_start_date: datetime, period_end_date: datetime) -> dict:
    try:
        leaves = await employees_leaves_collection.find(
            {"employee_id": employee_id, "status": "Posted", "start_date": {"$lte": period_end_date},
             "end_date": {"$gte": period_start_date}
             }).to_list(length=None)
        number_of_leave_days = 0
        current_date = period_start_date
        while current_date <= period_end_date:
            is_on_leave = any(
                leave["start_date"] <= current_date <= leave["end_date"] for leave in leaves)
            if is_on_leave:
                days_number = await get_number_of_days(str(employee_id),
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


#
# # ==== GET_ELEMENT_VALUE ====
# async def get_employee_element_value(element_id: ObjectId, employee_id: ObjectId) -> float:
#     try:
#         employee_payroll_element_doc = await employees_payrolls_collection.find_one({"_id": ObjectId(element_id)})
#         if not employee_payroll_element_doc:
#             raise HTTPException(status_code=404, detail="Employee Payroll element not found")
#         print(employee_payroll_element_doc)
#         employee_payroll_element_value: float = employee_payroll_element_doc.get("value", 0)
#         payroll_element_id: ObjectId = employee_payroll_element_doc.get("name", None)
#         employee_id = employee_payroll_element_doc.get("employee_id", None)
#
#         if not payroll_element_id:
#             raise HTTPException(status_code=404, detail="Payroll Element not found")
#         if not employee_payroll_element_value:
#             raise HTTPException(status_code=404, detail="Employee Payroll Element value not found")
#
#         payroll_element_based_elements_docs = await payroll_elements_based_elements_collection.find(
#             {"payroll_element_id": payroll_element_id}).to_list(length=None)
#
#         if len(payroll_element_based_elements_docs) == 0:
#             return employee_payroll_element_value
#
#         total_value = 0.0
#         for element in payroll_element_based_elements_docs:
#             element_id = element.get("name")
#             element_type = (element.get("type") or "Add").strip().lower()
#             employee_payroll_elements_docs = await employees_payrolls_collection.find(
#                 {"employee_id": ObjectId(employee_id), "name": ObjectId(element_id)}).to_list(length=None)
#             element_total = sum(
#                 float(doc.get("value", 0) or 0)
#                 for doc in employee_payroll_elements_docs
#             )
#             if element_type == "subtract":
#                 total_value -= element_total
#             else:
#                 total_value += element_total
#         print(total_value)
#         return total_value
#     except Exception:
#         raise


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

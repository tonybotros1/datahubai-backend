from datetime import datetime

from bson import ObjectId
from fastapi import APIRouter, HTTPException

from app.database import get_collection
from app.routes.employees import employees_leaves_collection, employees_payrolls_collection, leave_types_collection
from app.routes.payroll_elements import payroll_elements_based_elements_collection

router = APIRouter()
balances_collection = get_collection("balances")


def to_float(value, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


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


def is_within_period(
        element_start: datetime,
        element_end: datetime,
        period_start: datetime,
        period_end: datetime
) -> bool:
    if element_start > element_end:
        raise ValueError("element_start must be <= element_end")
    if period_start > period_end:
        raise ValueError("period_start must be <= period_end")

    return element_start <= period_end and element_end >= period_start


async def get_previous_gratuity_accrual(employee_id: ObjectId):
    cursor = await balances_collection.aggregate([
        {
            "$match": {
                "name": "Gratuity Balance"
            }
        },
        {
            "$lookup": {
                "from": "balances_based_elements",
                "localField": "_id",
                "foreignField": "balance_id",
                "as": "based_elements"
            }
        },
        {
            "$unwind": "$based_elements"
        },
        {
            "$lookup": {
                "from": "payroll_runs_employees_elements",
                "let": {
                    "based_element_id": "$based_elements.name"
                },
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$and": [
                                    {"$eq": ["$employee_id", employee_id]},
                                    {"$eq": ["$payroll_element_id", "$$based_element_id"]}
                                ]
                            }
                        }
                    }
                ],
                "as": "payroll_results"
            }
        },
        {
            "$unwind": {
                "path": "$payroll_results",
                "preserveNullAndEmptyArrays": True
            }
        },
        {
            "$group": {
                "_id": None,
                "total": {
                    "$sum": {
                        "$switch": {
                            "branches": [
                                {
                                    "case": {"$eq": ["$based_elements.type", "Add"]},
                                    "then": {"$ifNull": ["$payroll_results.value", 0]}
                                },
                                {
                                    "case": {"$eq": ["$based_elements.type", "Subtract"]},
                                    "then": {
                                        "$multiply": [
                                            {"$ifNull": ["$payroll_results.value", 0]},
                                            -1
                                        ]
                                    }
                                }
                            ],
                            "default": 0
                        }
                    }
                }
            }
        }
    ])

    result = await cursor.to_list(None)
    return result[0]["total"] if result else 0


def calculate_progressive_income_tax(taxable_amount: float, brackets: list[dict]) -> float:
    total_tax = 0.0

    for bracket in brackets:
        from_amount = to_float(bracket.get("from_amount"))
        to_amount = bracket.get("to_amount")
        percentage = to_float(bracket.get("percentage"))

        if taxable_amount <= from_amount or percentage <= 0:
            continue

        upper_amount = taxable_amount
        if to_amount is not None:
            upper_amount = min(taxable_amount, to_float(to_amount))

        bracket_amount = max(upper_amount - from_amount, 0)
        total_tax += bracket_amount * (percentage / 100)

    return total_tax


def income_tax_brackets(legislation_doc: dict) -> list[dict]:
    raw_brackets = legislation_doc.get("income_tax_brackets") or []
    brackets = []

    for bracket in raw_brackets:
        if not isinstance(bracket, dict):
            continue
        from_amount = to_float(bracket.get("from_amount"), 0)
        to_value = bracket.get("to_amount")
        to_amount = None if to_value is None or to_value == "" else to_float(to_value)
        percentage = to_float(bracket.get("percentage"))

        if percentage <= 0:
            continue

        brackets.append({
            "from_amount": max(from_amount, 0),
            "to_amount": to_amount if to_amount and to_amount > 0 else None,
            "percentage": percentage,
        })

    if not brackets:
        percentage = to_float(legislation_doc.get("income_tax_percentage"))
        ceiling = to_float(legislation_doc.get("income_tax_ceiling"))
        if percentage > 0:
            brackets.append({
                "from_amount": 0,
                "to_amount": ceiling if ceiling > 0 else None,
                "percentage": percentage,
            })

    return sorted(brackets, key=lambda item: item["from_amount"])

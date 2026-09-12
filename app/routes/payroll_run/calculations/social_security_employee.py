from datetime import datetime
from typing import Optional

from bson import ObjectId
from fastapi import HTTPException

from ..collections import legislations_collection, employees_payrolls_collection, \
    payroll_elements_based_elements_collection


async def py_social_security_employee_ff(employee_id: ObjectId, main_payroll_element_id: ObjectId,
                                         legislation: ObjectId,
                                         period_start_date: datetime, period_end_date: datetime,
                                         based_value: Optional[float] = None,
                                         legislation_document: Optional[dict] = None):
    try:
        # value = based_value
        # if value is None:
        value = await get_employee_element_value(main_payroll_element_id, employee_id,
                                                 period_end_date)
        legislation_doc = legislation_document
        if legislation_doc is None:
            legislation_doc = await legislations_collection.find_one({"_id": legislation})
        if not legislation_doc:
            raise HTTPException(status_code=404, detail="Legislation not found")

        ceiling = 0
        social_security_employee_percentage = 0
        social_security_ceilings: list = legislation_doc.get("social_security_ceilings")
        for social_security_ceiling in social_security_ceilings:
            if social_security_ceiling is None:
                continue
            start_date = social_security_ceiling.get("start_date")
            end_date = social_security_ceiling.get("end_date")
            if (start_date <= period_end_date) and (end_date is None or end_date >= period_end_date):
                ceiling = social_security_ceiling.get("ceiling", 0)
                social_security_employee_percentage = social_security_ceiling.get("employee_percentage", 0)

        social_security_employee_percentage = social_security_employee_percentage / 100
        social_security_ceiling = ceiling
        if not social_security_ceiling or social_security_ceiling == 0:
            social_security_ceiling = value

        social_security_employee = social_security_employee_percentage * min(value, social_security_ceiling)
        return round(social_security_employee, 2)

    except Exception as e:
        raise e


# this element gets the based element for the social security employee then get the value of each one from the employee payroll elements:
# if the end date was within the current year then if there was more than one value then gets the one with min end date
async def get_employee_element_value(element_id: ObjectId, employee_id: ObjectId,
                                     period_end_date: datetime) -> float:
    try:
        current_year_start = datetime(period_end_date.year, 1, 1)
        next_year_start = datetime(period_end_date.year + 1, 1, 1)

        payroll_element_based_elements_docs = await payroll_elements_based_elements_collection.find(
            {"payroll_element_id": element_id}).to_list(length=None)

        if len(payroll_element_based_elements_docs) == 0:
            raise HTTPException(status_code=404, detail="no value found for this element")

        total_value = 0.0
        for element in payroll_element_based_elements_docs:
            element_id = element.get("name")
            element_type = (element.get("type") or "Add").strip().lower()
            employee_payroll_elements_docs = await employees_payrolls_collection.find(
                {"employee_id": ObjectId(employee_id), "name": ObjectId(element_id),
                 "start_date": {"$lte": period_end_date},
                 "$or": [
                     {
                         "end_date": {
                             "$gte": current_year_start,
                             "$lt": next_year_start,
                         }
                     },
                     {"end_date": None},
                 ], }).to_list(length=None)
            selected_element = min(
                employee_payroll_elements_docs,
                key=lambda doc: (
                    doc.get("end_date") is None,
                    doc.get("end_date") or datetime.max,
                ),
                default=None,
            )
            element_value = float(selected_element.get("value", 0) or 0) if selected_element else 0.0

            if element_type == "subtract":
                total_value -= element_value
            else:
                total_value += element_value
        return total_value
    except Exception:
        raise

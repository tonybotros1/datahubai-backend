from datetime import datetime
from typing import Optional

from bson import ObjectId
from fastapi import HTTPException

from app.routes.payroll_runs_widgets.helpers_functions import get_employee_element_value
from ..collections import legislations_collection


async def py_social_security_employee_ff(employee_id: ObjectId, based_element_id: ObjectId, legislation: ObjectId,
                                         period_end_date: datetime,
                                         based_value: Optional[float] = None,
                                         legislation_document: Optional[dict] = None):
    try:
        value = based_value
        if value is None:
            value = await get_employee_element_value(based_element_id, employee_id)

        legislation_doc = legislation_document
        if legislation_doc is None:
            legislation_doc = await legislations_collection.find_one({"_id": legislation})
        if not legislation_doc:
            raise HTTPException(status_code=404, detail="Legislation not found")
        # No. of working hours
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

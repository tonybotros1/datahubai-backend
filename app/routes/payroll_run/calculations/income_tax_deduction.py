from datetime import datetime
from typing import Optional

from bson import ObjectId
from fastapi import HTTPException

from app.routes.payroll_runs_widgets.helpers_functions import (
    calculate_progressive_income_tax,
    get_employee_element_value,
    income_tax_brackets,
)
from ..collections import legislations_collection


async def py_income_tax_deduction_ff(element_value: float, employee_id: ObjectId, based_element_id: ObjectId,
                                     legislation: ObjectId,
                                     period_start_date: datetime, period_end_date: datetime,
                                     based_value: Optional[float] = None,
                                     legislation_document: Optional[dict] = None):
    try:
        value = based_value
        if value is None:
            value = await get_employee_element_value(based_element_id, employee_id, period_start_date, period_end_date)
        income_tax_exemption = element_value
        taxable_income_before_exemption = (value or 0)

        legislation_doc = legislation_document
        if legislation_doc is None:
            legislation_doc = await legislations_collection.find_one({"_id": legislation})
        if not legislation_doc:
            raise HTTPException(status_code=404, detail="Legislation not found")

        taxable_amount = max(taxable_income_before_exemption - income_tax_exemption, 0) * 12
        income_tax = calculate_progressive_income_tax(
            taxable_amount,
            income_tax_brackets(legislation_doc),
        )

        return round(income_tax / 12, 2)

    except Exception as e:
        raise e

from typing import Optional

from bson import ObjectId
from fastapi import HTTPException

from app.routes.payroll_runs_widgets.helpers_functions import (
    get_employee_element_value,
    to_float,
)
from ..collections import legislations_collection


async def py_service_tax_ff(employee_id: ObjectId, based_element_id: ObjectId, legislation: ObjectId,
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

        service_tax_percentage = to_float(legislation_doc.get("service_tax_percentage")) / 100
        service_tax = (value or 0) * service_tax_percentage
        return round(service_tax, 2)

    except Exception as e:
        raise e

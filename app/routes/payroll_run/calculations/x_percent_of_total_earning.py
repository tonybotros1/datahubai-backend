import math
from collections.abc import Mapping, Sequence
from typing import Any, Optional

from bson import ObjectId
from fastapi import HTTPException


async def py_x_percent_of_total_earnings(
    percent_value: Optional[float],
    employee_elements: Sequence[Mapping[str, Any]],
    payroll_definitions_by_id: Mapping[ObjectId, Mapping[str, Any]],
) -> float:
    """Return a percentage of the employee's calculated earning elements.

    ``employee_elements`` contains values already calculated for the current
    payroll run. Looking up each row's definition in the in-memory mapping
    avoids a database query for every element.
    """
    try:
        percentage = float(percent_value or 0)
    except (TypeError, ValueError) as error:
        raise HTTPException(
            status_code=400,
            detail="Percentage value must be a valid number",
        ) from error

    if not math.isfinite(percentage) or percentage < 0:
        raise HTTPException(
            status_code=400,
            detail="Percentage value must be a finite, non-negative number",
        )

    earning_values = []
    for element in employee_elements:
        definition = payroll_definitions_by_id.get(
            element.get("payroll_element_id")
        )
        element_type = str((definition or {}).get("type") or "").strip().lower()
        if element_type != "earning":
            continue

        try:
            value = float(element.get("value") or 0)
        except (TypeError, ValueError) as error:
            raise HTTPException(
                status_code=400,
                detail="Earning element value must be a valid number",
            ) from error
        if not math.isfinite(value):
            raise HTTPException(
                status_code=400,
                detail="Earning element value must be finite",
            )
        earning_values.append(value)

    total_earnings = math.fsum(earning_values)
    return round(total_earnings * percentage / 100, 2)

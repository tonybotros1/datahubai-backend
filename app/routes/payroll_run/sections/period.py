"""Load and validate the selected payroll period."""

from bson import ObjectId
from fastapi import HTTPException

from ..collections import payroll_period_details_collection
from .context import PayrollPeriod


async def load_payroll_period(period_id: ObjectId) -> PayrollPeriod:
    period_document = await payroll_period_details_collection.find_one({"_id": period_id})
    period_start_date = period_document.get("start_date")
    period_end_date = period_document.get("end_date")

    if not period_start_date or not period_end_date:
        raise HTTPException(status_code=400, detail="period dates are required")

    return PayrollPeriod(
        start_date=period_start_date,
        end_date=period_end_date,
    )

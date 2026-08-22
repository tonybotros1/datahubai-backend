"""Request models for payroll-run endpoints."""

from typing import Optional

from pydantic import BaseModel

from app.routes.car_trading import PyObjectId


class PayrollRunModel(BaseModel):
    payroll_id: Optional[PyObjectId] = None
    period_id: Optional[PyObjectId] = None
    employee_id: Optional[PyObjectId] = None
    element_id: Optional[PyObjectId] = None

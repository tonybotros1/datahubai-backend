"""Shared values passed between the payroll-run sections."""

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from bson import ObjectId


@dataclass(frozen=True)
class PayrollPeriod:
    start_date: datetime
    end_date: datetime


@dataclass(frozen=True)
class PayrollRunContext:
    payroll_elements_by_employee: dict[ObjectId, list[dict[str, Any]]]
    leaves_by_employee: dict[ObjectId, list[dict[str, Any]]]
    loans_by_employee: dict[ObjectId, list[dict[str, Any]]]
    loan_payments_by_id: dict[ObjectId, float]
    processed_element_ids: set[ObjectId]
    leave_types_by_id: dict[ObjectId, dict[str, Any]]
    loan_types_by_id: dict[ObjectId, dict[str, Any]]
    payroll_definitions_by_id: dict[ObjectId, dict[str, Any]]
    legislations_by_id: dict[ObjectId, dict[str, Any]]
    employee_element_value: Callable[[ObjectId, ObjectId], float]

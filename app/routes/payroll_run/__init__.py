"""Payroll-run routes split into focused, editable sections."""

from fastapi import APIRouter

from . import bank_export, lookups, payslip_email, queries, rollback, run
from .bank_export import prepare_bank_export
from .calculations import *
from .lookups import (
    get_all_employees_for_payroll_runs_lov,
    get_payroll_for_lov,
    get_payroll_periods_for_lov,
)
from .models import PayrollRunModel
from .payslip_email import email_payslips
from .queries import get_all_payroll_runs, get_payroll_runs_details
from .rollback import rollback_payroll_run
from .run import payroll_run, save_payroll_run

router = APIRouter()
router.include_router(run.router)
router.include_router(rollback.router)
router.include_router(queries.router)
router.include_router(bank_export.router)
router.include_router(payslip_email.router)
router.include_router(lookups.router)

__all__ = [
    "PayrollRunModel",
    "email_payslips",
    "get_all_employees_for_payroll_runs_lov",
    "get_all_payroll_runs",
    "get_payroll_for_lov",
    "get_payroll_periods_for_lov",
    "get_payroll_runs_details",
    "payroll_run",
    "prepare_bank_export",
    "rollback_payroll_run",
    "router",
    "save_payroll_run",
] + [name for name in globals() if name.startswith("py_")]

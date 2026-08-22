"""Payroll-run endpoint coordinator.

Detailed work is kept in ``payroll_run.sections`` so each part can be
read and modified independently.
"""

from fastapi import APIRouter, Depends

from app.core import security
from .models import PayrollRunModel
from .queries import get_payroll_runs_details
from .sections import (
    load_payroll_employees,
    load_payroll_period,
    load_payroll_run_context,
    process_payroll_employees,
    save_payroll_run,
)

router = APIRouter()


@router.post("/payroll_run")
async def payroll_run(run: PayrollRunModel, data: dict = Depends(security.get_current_user)):
    try:
        period = await load_payroll_period(run.period_id)
        employees = await load_payroll_employees(
            run.payroll_id,
            run.employee_id,
            period,
        )
        context = await load_payroll_run_context(
            employees,
            run.period_id,
            run.element_id,
            period,
        )
        description, element_values = await process_payroll_employees(
            employees,
            period,
            context,
            data,
        )

        run_id = await save_payroll_run(
            run.payroll_id,
            run.period_id,
            description,
            [employee["_id"] for employee in employees],
            element_values,
            data,
        )
        details = await get_payroll_runs_details(run_id, data)
        return {"added_run": details["payroll_runs_details"]}
    except Exception:
        raise

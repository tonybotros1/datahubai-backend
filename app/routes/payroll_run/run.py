"""Payroll-run endpoint coordinator.

Detailed work is kept in ``payroll_run.sections`` so each part can be
read and modified independently.
"""

from fastapi import APIRouter, Depends

from app.core import security
from .models import PayrollRunModel
from .queries import get_payroll_runs_details
from .sections import (
    get_payroll_employees,
    get_payroll_period_start_date_and_end_date,
    load_payroll_run_context,
    process_payroll_employees,
    save_payroll_run,
)

router = APIRouter()


@router.post("/payroll_run")
async def payroll_run(run: PayrollRunModel, data: dict = Depends(security.get_current_user)):
    try:
        #======================================================
        # This function get the period from DB and get the Start Date and End Date of it
        #======================================================
        period = await get_payroll_period_start_date_and_end_date(run.period_id)
        #======================================================

        #======================================================
        # This function is to get all employees for the selected payroll
        #======================================================
        employees = await get_payroll_employees(
            run.payroll_id,
            run.employee_id,
            period,
        )
        #======================================================

        #======================================================
        # This function is to get all the payroll elements / leave elements / loan elements for each employee with the legislations information
        #======================================================
        context = await load_payroll_run_context(
            employees,
            run.period_id,
            run.element_id,
            period,
        )
        #======================================================

        #======================================================
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

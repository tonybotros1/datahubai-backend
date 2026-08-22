"""Coordinate all calculation sections for the selected employees."""

from .context import PayrollPeriod, PayrollRunContext
from .leaves import calculate_employee_leaves
from .loans import calculate_employee_loans
from .payroll_elements import calculate_employee_payroll_elements


async def process_payroll_employees(
    all_employees: list[dict],
    period: PayrollPeriod,
    context: PayrollRunContext,
    data: dict,
) -> tuple[str, dict]:
    description = ""
    elements_values_maps = {}

    for employee in all_employees:
        employee_id = employee.get("_id")
        employee_name = employee.get("full_name") or None
        description = employee_name if len(all_employees) == 1 else "All Employees"

        employee_elements = await calculate_employee_payroll_elements(
            employee,
            period,
            context,
        )
        employee_elements.extend(await calculate_employee_leaves(
            employee,
            period,
            context,
            data,
        ))
        employee_elements.extend(await calculate_employee_loans(
            employee,
            context,
        ))
        elements_values_maps[employee_id] = employee_elements

    return description, elements_values_maps

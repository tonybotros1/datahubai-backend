"""Calculate regular payroll elements for one employee."""

from datetime import datetime

from bson import ObjectId

from app.routes.payroll_runs_widgets.helpers_functions import is_within_period
from ..calculations import (
    py_annual_leave_entitlement_ff,
    py_gratuity_accrual_ff,
    py_income_tax_deduction_ff,
    py_input_value_ff,
    py_nonrecurring_ff,
    py_overtime_holidays_ff,
    py_overtime_normal_ff,
    py_service_tax_ff,
    py_social_security_employee_ff,
    py_social_security_employer_ff,
)
from .context import PayrollPeriod, PayrollRunContext


async def calculate_employee_payroll_elements(
        employee: dict,
        period: PayrollPeriod,
        context: PayrollRunContext,
) -> list[dict]:
    current_employee_id = employee.get("_id")
    employee_hire_date = employee.get("hire_date") or datetime.min
    employee_end_date = employee.get("end_date") or datetime.max
    legislation = employee.get("legislation") or None
    legislation_document = context.legislations_by_id.get(legislation)
    period_start_date = period.start_date
    period_end_date = period.end_date

    payroll_elements_by_employee = context.payroll_elements_by_employee
    processed_element_ids = context.processed_element_ids
    payroll_definitions_by_id = context.payroll_definitions_by_id
    employee_element_value = context.employee_element_value
    elements_values_maps = {current_employee_id: []}

    payroll_elements = payroll_elements_by_employee.get(current_employee_id, [])

    for employee_payroll in payroll_elements:
        if not employee_payroll:
            continue
        if employee_payroll["_id"] not in processed_element_ids:
            element_start = employee_payroll.get("start_date") or datetime.min
            element_end = employee_payroll.get("end_date") or datetime.max
            element_value = employee_payroll.get("value")
            element_definition = payroll_definitions_by_id.get(employee_payroll.get("name"))
            element_function = element_definition.get("function") if element_definition else None
            if element_function:
                if element_function.upper() == "PY_INPUT_VALUE_FF":
                    value = await py_input_value_ff(employee_hire_date, employee_end_date, element_start,
                                                    element_value,
                                                    element_end, period_start_date, period_end_date)
                    elements_values_maps[current_employee_id].append({
                        "element_id": employee_payroll.get("_id"),
                        "value": value,
                        "payroll_element_id": employee_payroll.get("name"),
                        "number": None
                    })

                if element_function.upper() == "PY_ANNUAL_LEAVE_ENTITLEMENT_FF":
                    number, value = await py_annual_leave_entitlement_ff(employee_hire_date, employee_end_date,
                                                                         element_start,
                                                                         element_value,
                                                                         element_end, period_start_date,
                                                                         period_end_date,
                                                                         employee_payroll.get("name"),
                                                                         current_employee_id,
                                                                         employee_element_value(
                                                                             employee_payroll.get("name"),
                                                                             current_employee_id))
                    elements_values_maps[current_employee_id].append({
                        "element_id": employee_payroll.get("_id"),
                        "value": value,
                        "payroll_element_id": employee_payroll.get("name"),
                        "number": number
                    })
                if element_function.upper() == "PY_OVERTIME_NORMAL_FF":
                    if is_within_period(element_start, element_end, period_start_date, period_end_date):
                        value = await py_overtime_normal_ff(employee_payroll.get("employee_id"),
                                                            period_start_date, period_end_date,
                                                            employee_payroll.get("name"), legislation,
                                                            element_value,
                                                            employee_element_value(
                                                                employee_payroll.get("name"),
                                                                current_employee_id),
                                                            legislation_document)
                        elements_values_maps[current_employee_id].append({
                            "element_id": employee_payroll.get("_id"),
                            "value": value,
                            "payroll_element_id": employee_payroll.get("name"),
                            "number": 0
                        })
                if element_function.upper() == "PY_OVERTIME_HOLIDAYS_FF":
                    if is_within_period(element_start, element_end, period_start_date, period_end_date):
                        value = await py_overtime_holidays_ff(employee_payroll.get("employee_id"),
                                                              period_start_date, period_end_date,
                                                              employee_payroll.get("name"), legislation,
                                                              element_value,
                                                              employee_element_value(
                                                                  employee_payroll.get("name"),
                                                                  current_employee_id),
                                                              legislation_document)
                        elements_values_maps[current_employee_id].append({
                            "element_id": employee_payroll.get("_id"),
                            "value": value,
                            "payroll_element_id": employee_payroll.get("name"),
                            "number": 0
                        })
                if element_function.upper() == "PY_NONRECURRING_FF":
                    if is_within_period(element_start, element_end, period_start_date, period_end_date):
                        value = await py_nonrecurring_ff(period_start_date, period_end_date, element_start,
                                                         element_end, element_value)
                        if value:
                            elements_values_maps[current_employee_id].append({
                                "element_id": employee_payroll.get("_id"),
                                "value": value,
                                "payroll_element_id": employee_payroll.get("name"),
                                "number": value
                            })
                if element_function.upper() == "PY_SOCIAL_SECURITY_EMPLOYEE_FF":
                    if is_within_period(element_start, element_end, period_start_date, period_end_date):
                        value = await py_social_security_employee_ff(ObjectId(current_employee_id),
                                                                     employee_payroll.get("name"), legislation,
                                                                     period_start_date,
                                                                     period_end_date,
                                                                     employee_element_value(
                                                                         employee_payroll.get("name"),
                                                                         current_employee_id),
                                                                     legislation_document)
                        if value:
                            elements_values_maps[current_employee_id].append({
                                "element_id": employee_payroll.get("_id"),
                                "value": value,
                                "payroll_element_id": employee_payroll.get("name"),
                                "number": 0
                            })
                if element_function.upper() == "PY_SOCIAL_SECURITY_EMPLOYER_FF":
                    if is_within_period(element_start, element_end, period_start_date, period_end_date):
                        value = await py_social_security_employer_ff(ObjectId(current_employee_id),
                                                                     employee_payroll.get("name"), legislation,
                                                                     period_start_date,
                                                                     period_end_date,
                                                                     employee_element_value(
                                                                         employee_payroll.get("name"),
                                                                         current_employee_id),
                                                                     legislation_document)
                        if value:
                            elements_values_maps[current_employee_id].append({
                                "element_id": employee_payroll.get("_id"),
                                "value": value,
                                "payroll_element_id": employee_payroll.get("name"),
                                "number": 0
                            })
                if element_function.upper() == "PY_SERVICE_TAX_FF":
                    if is_within_period(element_start, element_end, period_start_date, period_end_date):
                        value = await py_service_tax_ff(ObjectId(current_employee_id),
                                                        employee_payroll.get("name"), legislation,
                                                        period_start_date,period_end_date,
                                                        employee_element_value(
                                                            employee_payroll.get("name"),
                                                            current_employee_id),
                                                        legislation_document)
                        if value:
                            elements_values_maps[current_employee_id].append({
                                "element_id": employee_payroll.get("_id"),
                                "value": value,
                                "payroll_element_id": employee_payroll.get("name"),
                                "number": 0
                            })
                if element_function.upper() == "PY_INCOME_TAX_DEDUCTION_FF":
                    if is_within_period(element_start, element_end, period_start_date, period_end_date):
                        value = await py_income_tax_deduction_ff(element_value, ObjectId(current_employee_id),
                                                                 employee_payroll.get("name"), legislation,
                                                                 period_start_date, period_end_date,
                                                                 employee_element_value(
                                                                     employee_payroll.get("name"),
                                                                     current_employee_id),
                                                                 legislation_document)
                        if value:
                            elements_values_maps[current_employee_id].append({
                                "element_id": employee_payroll.get("_id"),
                                "value": value,
                                "payroll_element_id": employee_payroll.get("name"),
                                "number": 0
                            })
                if element_function.upper() == "PY_GRATUITY_ACCRUAL_FF":
                    if is_within_period(element_start, element_end, period_start_date, period_end_date):
                        value = await py_gratuity_accrual_ff(ObjectId(current_employee_id), employee_hire_date,
                                                             employee_end_date, element_start, element_end,
                                                             period_start_date, period_end_date,
                                                             employee_payroll.get("name"), legislation,
                                                             employee_element_value(
                                                                 employee_payroll.get("name"),
                                                                 current_employee_id),
                                                             legislation_document)
                        if value:
                            elements_values_maps[current_employee_id].append({
                                "element_id": employee_payroll.get("_id"),
                                "value": value,
                                "payroll_element_id": employee_payroll.get("name"),
                                "number": 0
                            })

    return elements_values_maps[current_employee_id]

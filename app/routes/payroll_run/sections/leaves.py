"""Calculate leave-related payroll elements for one employee."""

from fastapi import HTTPException

from ..calculations import (
    py_annual_leave_ff,
    py_compassionate_leave_ff,
    py_maternity_leave_ff,
    py_paternity_leave_ff,
    py_sick_leave_ff,
    py_unpaid_leave_ff,
)
from .context import PayrollPeriod, PayrollRunContext


async def calculate_employee_leaves(
    employee: dict,
    period: PayrollPeriod,
    context: PayrollRunContext,
    data: dict,
) -> list[dict]:
    current_employee_id = employee.get("_id")
    legislation = employee.get("legislation") or None
    legislation_document = context.legislations_by_id.get(legislation)
    period_start_date = period.start_date
    period_end_date = period.end_date

    leaves_by_employee = context.leaves_by_employee
    leave_types_by_id = context.leave_types_by_id
    processed_element_ids = context.processed_element_ids
    payroll_definitions_by_id = context.payroll_definitions_by_id
    employee_element_value = context.employee_element_value
    elements_values_maps = {current_employee_id: []}

    employee_leaves = leaves_by_employee.get(current_employee_id, [])

    for leave in employee_leaves:
        # leave_id = leave.get("_id")
        leave_type = leave.get("leave_type")
        leave_start_date = leave.get("start_date")
        leave_end_date = leave.get("end_date")
        if not leave_type:
            continue

        leave_type_doc = leave_types_by_id.get(leave_type)
        if not leave_type_doc:
            continue

        based_element_id = leave_type_doc.get("based_element")
        number_of_days = leave.get("number_of_days")
        if not based_element_id:
            continue

        if not number_of_days:
            leave_type_name = leave_type_doc.get("name", "Selected leave type")
            raise HTTPException(
                status_code=400,
                detail=f"{leave_type_name} is missing number_of_days for annual leave calculation",
            )

        if leave["_id"] not in processed_element_ids:
            payroll_element_doc = payroll_definitions_by_id.get(based_element_id)
            function = payroll_element_doc.get("function") if payroll_element_doc else None
            if function and function.upper() == "PY_ANNUAL_LEAVE_FF":
                is_pay_in_advanced: bool = leave.get("pay_in_advance", False)
                l_days, final_value = await py_annual_leave_ff(leave_type, current_employee_id,
                                                               period_start_date,
                                                               period_end_date, based_element_id,
                                                               leave_start_date, leave_end_date,
                                                               is_pay_in_advanced, data,
                                                               employee_element_value(
                                                                   based_element_id,
                                                                   current_employee_id))

                elements_values_maps[current_employee_id].append({
                    "element_id": leave["_id"],
                    "value": final_value,
                    "payroll_element_id": based_element_id,
                    "number": l_days
                })
            elif function.upper() == "PY_UNPAID_LEAVE_FF":
                final_value, leave_days = await py_unpaid_leave_ff(leave_type, current_employee_id,
                                                                   period_start_date,
                                                                   period_end_date, based_element_id,
                                                                   leave_start_date, leave_end_date, data,
                                                                   employee_element_value(
                                                                       based_element_id,
                                                                       current_employee_id))

                elements_values_maps[current_employee_id].append({
                    "element_id": leave["_id"],
                    "value": final_value,
                    "payroll_element_id": based_element_id,
                    "number": leave_days
                })
            elif function.upper() == "PY_SICK_LEAVE_FF":
                final_value, leave_days = await py_sick_leave_ff(leave_type, current_employee_id,
                                                                 period_start_date,
                                                                 period_end_date, based_element_id,
                                                                 legislation,
                                                                 leave_start_date, leave_end_date, data,
                                                                 employee_element_value(
                                                                     based_element_id,
                                                                     current_employee_id),
                                                                 legislation_document)

                elements_values_maps[current_employee_id].append({
                    "element_id": leave["_id"],
                    "value": final_value,
                    "payroll_element_id": based_element_id,
                    "number": leave_days
                })
            elif function.upper() == "PY_MATERNITY_LEAVE_FF":
                final_value, leave_days = await py_maternity_leave_ff(leave_type, current_employee_id,
                                                                      period_start_date,
                                                                      period_end_date, based_element_id,
                                                                      legislation,
                                                                      leave_start_date, leave_end_date, data,
                                                                      employee_element_value(
                                                                          based_element_id,
                                                                          current_employee_id),
                                                                      legislation_document)

                elements_values_maps[current_employee_id].append({
                    "element_id": leave["_id"],
                    "value": final_value,
                    "payroll_element_id": based_element_id,
                    "number": leave_days
                })
            elif function.upper() == "PY_PATERNITY_LEAVE_FF":
                final_value, leave_days = await py_paternity_leave_ff(leave_type, current_employee_id,
                                                                      period_start_date,
                                                                      period_end_date, based_element_id,
                                                                      legislation,
                                                                      leave_start_date, leave_end_date, data,
                                                                      employee_element_value(
                                                                          based_element_id,
                                                                          current_employee_id),
                                                                      legislation_document)

                elements_values_maps[current_employee_id].append({
                    "element_id": leave["_id"],
                    "value": final_value,
                    "payroll_element_id": based_element_id,
                    "number": leave_days
                })
            elif function.upper() == "PY_COMPASSIONATE_LEAVE_FF":
                final_value, leave_days = await py_compassionate_leave_ff(leave_type, current_employee_id,
                                                                          period_start_date,
                                                                          period_end_date, based_element_id,
                                                                          legislation,
                                                                          leave_start_date, leave_end_date,
                                                                          data,
                                                                          employee_element_value(
                                                                              based_element_id,
                                                                              current_employee_id),
                                                                          legislation_document)

                elements_values_maps[current_employee_id].append({
                    "element_id": leave["_id"],
                    "value": final_value,
                    "payroll_element_id": based_element_id,
                    "number": leave_days
                })

    return elements_values_maps[current_employee_id]

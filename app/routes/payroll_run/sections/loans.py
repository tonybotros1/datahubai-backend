"""Calculate loan and advance deductions for one employee."""

from ..calculations import py_loan_and_advances_ff
from .context import PayrollRunContext


async def calculate_employee_loans(
    employee: dict,
    context: PayrollRunContext,
) -> list[dict]:
    current_employee_id = employee.get("_id")

    loans_by_employee = context.loans_by_employee
    loan_types_by_id = context.loan_types_by_id
    processed_element_ids = context.processed_element_ids
    payroll_definitions_by_id = context.payroll_definitions_by_id
    loan_payments_by_id = context.loan_payments_by_id
    elements_values_maps = {current_employee_id: []}

    employee_loan_and_advances = loans_by_employee.get(current_employee_id, [])

    for loan in employee_loan_and_advances:
        loan_and_advances_id = loan.get("_id")
        total_amount = loan.get("total_amount", 0)
        monthly_installment = loan.get("monthly_installment", 0)
        loan_and_advances_type = loan.get("type", 0)

        loan_and_advances_type_doc = loan_types_by_id.get(loan_and_advances_type)
        if not loan_and_advances_type_doc:
            continue

        based_element_id = loan_and_advances_type_doc.get("based_element")
        if not based_element_id:
            continue

        if loan_and_advances_id not in processed_element_ids:
            payroll_element_doc = payroll_definitions_by_id.get(based_element_id)
            function = payroll_element_doc.get("function") if payroll_element_doc else None
            if function and function.upper() == "PY_LOAN_AND_ADVANCES_FF":
                final_value = await py_loan_and_advances_ff(
                    loan_and_advances_id,
                    total_amount,
                    monthly_installment,
                    loan_payments_by_id.get(loan_and_advances_id, 0),
                )
                if final_value == 0:
                    continue
                elements_values_maps[current_employee_id].append({
                    "element_id": loan_and_advances_id,
                    "value": final_value,
                    "payroll_element_id": based_element_id,
                    "number": 0
                })

    return elements_values_maps[current_employee_id]

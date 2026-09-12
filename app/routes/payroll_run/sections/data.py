"""Load and index the data required by employee calculations."""

import asyncio
from typing import Any, Optional

from bson import ObjectId
from fastapi import HTTPException

from ..collections import (
    employees_leaves_collection,
    employees_loan_and_advances_collection,
    employees_payrolls_collection,
    leave_types_collection,
    legislations_collection,
    loan_and_advances_types_collection,
    payroll_elements_based_elements_collection,
    payroll_elements_collection,
    payroll_runs_employees_elements_collection,
)
from .context import PayrollPeriod, PayrollRunContext


async def load_payroll_run_context(
    all_employees: list[dict],
    period_id: Optional[ObjectId],
    element_id: Optional[ObjectId],
    period: PayrollPeriod,  # has the period start date and period end date
) -> PayrollRunContext:
    period_start_date = period.start_date
    period_end_date = period.end_date

    # all employees ids for the selected payroll (like Datahub AI payroll)
    employee_ids = [employee["_id"] for employee in all_employees]

    payroll_element_filter: Any = {
        "employee_id": {"$in": employee_ids},
        "start_date": {"$lte": period_end_date},
        "$or": [
            {"end_date": {"$gte": period_start_date}},
            {"end_date": None},
        ],
    }

    if element_id:
        payroll_element_filter["name"] = element_id

    # This gets the payroll elements in [employee screen] for the employees related to the selected payroll (like Datahub AI payroll)
    payroll_elements_for_selected_employees_within_the_period = employees_payrolls_collection.find(
        payroll_element_filter,
        {
            "_id": 1,
            "employee_id": 1,
            "start_date": 1,
            "end_date": 1,
            "name": 1,
            "value": 1,
        },
    ).to_list(None)

    # This gets the leave elements in [employee screen] for the employees related to the selected payroll (like Datahub AI payroll)
    leaves_task = employees_leaves_collection.find(
        {
            "employee_id": {"$in": employee_ids},
            "status": "Posted",
            "start_date": {"$lte": period_end_date},
            "end_date": {"$gte": period_start_date},
        },
        {
            "_id": 1,
            "employee_id": 1,
            "leave_type": 1,
            "start_date": 1,
            "end_date": 1,
            "number_of_days": 1,
            "pay_in_advance": 1,
        },
    ).to_list(None)

    # This gets the loan elements in [employee screen] for the employees related to the selected payroll (like Datahub AI payroll)
    loans_task = employees_loan_and_advances_collection.find(
        {
            "employee_id": {"$in": employee_ids},
            "deduction_date": {"$lte": period_end_date},
        },
        {"_id": 1, "employee_id": 1, "total_amount": 1,
            "monthly_installment": 1, "type": 1},
    ).to_list(None)

    # This gets all payroll elements in [employee screen] for the employees related to the selected payroll (like Datahub AI payroll) in all time without period filter
    payroll_elements_for_selected_employee_all_the_time = employees_payrolls_collection.find(
        {"employee_id": {"$in": employee_ids},
         "start_date": {"$lte": period_end_date},
         "$or": [
            {"end_date": {"$gte": period_start_date}},
            {"end_date": None},
        ], },
        {"employee_id": 1, "name": 1, "value": 1},
    ).to_list(None)

    all_payroll_elements, all_employee_leaves, all_employee_loans, all_employee_values = await asyncio.gather(
        payroll_elements_for_selected_employees_within_the_period,
        leaves_task,
        loans_task,
        payroll_elements_for_selected_employee_all_the_time,
    )

    # ==================================================================================
    # This assign each payroll element to its employee                                #=
    # ==================================================================================
    payroll_elements_by_employee = {employee_id: []
                                    for employee_id in employee_ids}  # =
    # =
    for payroll_element in all_payroll_elements:  # =
        payroll_elements_by_employee.setdefault(  # =
            payroll_element["employee_id"], []  # =
        ).append(payroll_element)  # =
    # ==================================================================================

    # ==============================================================================
    # This assign each leave element to its employee                              #=
    # ==============================================================================
    leaves_by_employee = {employee_id: [] for employee_id in employee_ids}  # =
    # =
    for leave in all_employee_leaves:  # =
        leaves_by_employee.setdefault(
            leave["employee_id"], []).append(leave)  # =
    # ==============================================================================

    # ==============================================================================
    # This assign each loan element to its employee                               #=
    # ==============================================================================
    loans_by_employee = {employee_id: [] for employee_id in employee_ids}  # =
    # =
    for loan in all_employee_loans:  # =
        loans_by_employee.setdefault(loan["employee_id"], []).append(loan)  # =
    # ==============================================================================

    leave_type_ids = {
        leave.get("leave_type")
        for leave in all_employee_leaves
        if leave.get("leave_type")
    }

    loan_type_ids = {
        loan.get("type")
        for loan in all_employee_loans
        if loan.get("type")
    }

    candidate_element_ids = {
        document["_id"]
        for document in (
            all_payroll_elements
            + all_employee_leaves
            + all_employee_loans
        )
    }

    processed_task = payroll_runs_employees_elements_collection.find(
        {
            "period_id": period_id,
            "element_id": {"$in": list(candidate_element_ids)},
        },
        {"element_id": 1},
    ).to_list(None)

    leave_types_task = leave_types_collection.find(
        {"_id": {"$in": list(leave_type_ids)}},
        {"based_element": 1, "name": 1},
    ).to_list(None)

    loan_types_task = loan_and_advances_types_collection.find(
        {"_id": {"$in": list(loan_type_ids)}},
        {"based_element": 1},
    ).to_list(None)

    loan_ids = [loan["_id"] for loan in all_employee_loans]

    loan_payments_cursor = await payroll_runs_employees_elements_collection.aggregate(
        [
            {"$match": {"element_id": {"$in": loan_ids}}},
            {
                "$group": {
                    "_id": "$element_id",
                    "paid_to_date": {
                        "$sum": {"$ifNull": ["$value", 0]}
                    },
                }
            },
        ]
    )

    loan_payments_task = loan_payments_cursor.to_list(None)

    processed_documents, leave_type_documents, loan_type_documents, loan_payment_documents = await asyncio.gather(
        processed_task,
        leave_types_task,
        loan_types_task,
        loan_payments_task,
    )

    loan_payments_by_id = {
        document["_id"]: document.get("paid_to_date", 0)
        for document in loan_payment_documents
    }

    processed_element_ids = {
        document["element_id"]
        for document in processed_documents
        if document.get("element_id")
    }

    leave_types_by_id = {
        document["_id"]: document for document in leave_type_documents}

    loan_types_by_id = {document["_id"]                        : document for document in loan_type_documents}

    payroll_definition_ids = {
        payroll_element.get("name")
        for payroll_element in all_payroll_elements
        if payroll_element.get("name")
    }

    payroll_definition_ids.update(
        document.get("based_element")
        for document in leave_type_documents + loan_type_documents
        if document.get("based_element")
    )

    legislation_ids = {
        employee.get("legislation")
        for employee in all_employees
        if employee.get("legislation")
    }

    payroll_definitions_task = payroll_elements_collection.find(
        {"_id": {"$in": list(payroll_definition_ids)}},
        {"function": 1},
    ).to_list(None)

    based_elements_task = payroll_elements_based_elements_collection.find(
        {"payroll_element_id": {"$in": list(payroll_definition_ids)}},
        {"payroll_element_id": 1, "name": 1, "type": 1},
    ).to_list(None)

    legislations_task = legislations_collection.find(
        {"_id": {"$in": list(legislation_ids)}}
    ).to_list(None)

    payroll_definition_documents, based_element_documents, legislation_documents = await asyncio.gather(
        payroll_definitions_task,
        based_elements_task,
        legislations_task,
    )

    payroll_definitions_by_id = {
        document["_id"]: document
        for document in payroll_definition_documents
    }

    legislations_by_id = {
        document["_id"]: document
        for document in legislation_documents
    }

    employee_values_by_name = {}

    employee_payrolls_by_id = {}

    for employee_value in all_employee_values:
        employee_payrolls_by_id[employee_value["name"]
                                ] = employee_value  # this was ['_id']
        key = (employee_value.get("employee_id"), employee_value.get("name"))
        employee_values_by_name[key] = (
            employee_values_by_name.get(key, 0)
            + float(employee_value.get("value", 0) or 0)
        )

    based_elements_by_payroll = {}

    for based_element in based_element_documents:
        based_elements_by_payroll.setdefault(
            based_element.get("payroll_element_id"), []
        ).append(based_element)

    def employee_element_value(payroll_element_id: ObjectId, current_employee_id: ObjectId) -> float:
        # Here the payroll_element_id is the id pf payroll element from payroll elements screen

        direct_element = employee_payrolls_by_id.get(payroll_element_id)

        direct_value = direct_element.get("value") if direct_element else None
        definition_id = (
            direct_element.get("name")
            if direct_element
            else payroll_element_id
        )
        based_elements = based_elements_by_payroll.get(definition_id, [])
        if not based_elements:
            if direct_value:
                return direct_value
            raise HTTPException(
                status_code=404,
                detail="no value found for this element",
            )

        total_value = 0.0
        for payroll_based_element in based_elements:
            value = employee_values_by_name.get(
                (current_employee_id, payroll_based_element.get("name")),
                0,
            )
            if (payroll_based_element.get("type") or "Add").strip().lower() == "subtract":
                total_value -= value
            else:
                total_value += value
        return total_value

    return PayrollRunContext(
        payroll_elements_by_employee=payroll_elements_by_employee,
        leaves_by_employee=leaves_by_employee,
        loans_by_employee=loans_by_employee,
        loan_payments_by_id=loan_payments_by_id,
        processed_element_ids=processed_element_ids,
        leave_types_by_id=leave_types_by_id,
        loan_types_by_id=loan_types_by_id,
        payroll_definitions_by_id=payroll_definitions_by_id,
        legislations_by_id=legislations_by_id,
        employee_element_value=employee_element_value,
    )

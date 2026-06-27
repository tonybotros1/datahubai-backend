import asyncio
import copy
from datetime import datetime
from typing import Optional, Any
from bson import ObjectId
from bson.errors import InvalidId
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from app import database
from app.core import security
from app.database import get_collection
from app.routes.car_trading import PyObjectId
from app.routes.counters import create_custom_counter
from app.routes.employees import calculate_number_of_days, NumberOfDaysForWorkingDaysModel
from app.routes.payroll_runs_widgets.helpers_functions import get_employee_element_value, \
    get_period_days, get_current_leave_used_days, get_used_leave_days, is_within_period, get_previous_gratuity_accrual, \
    to_float, calculate_progressive_income_tax, income_tax_brackets

router = APIRouter()
payroll_runs_collection = get_collection("payroll_runs")
payroll_runs_employees_collection = get_collection("payroll_runs_employees")
payroll_runs_employees_elements_collection = get_collection("payroll_runs_employees_elements")

payroll_collection = get_collection("payroll")
payroll_period_details_collection = get_collection("payroll_period_details")
leave_types_collection = get_collection("leave_types")
loan_and_advances_types_collection = get_collection("loan_and_advances_types")

employees_collection = get_collection("employees")
employees_payrolls_collection = get_collection("employees_payrolls")
payroll_elements_collection = get_collection("payroll_elements")
employees_leaves_collection = get_collection("employees_leaves")
employees_loan_and_advances_collection = get_collection("employees_loan_and_advances")
legislations_collection = get_collection("legislations")

payroll_elements_based_elements_collection = get_collection("payroll_elements_based_elements")


class PayrollRunModel(BaseModel):
    payroll_id: Optional[PyObjectId] = None
    period_id: Optional[PyObjectId] = None
    employee_id: Optional[PyObjectId] = None
    element_id: Optional[PyObjectId] = None


async def save_payroll_run(
        payroll_id: ObjectId,
        period_id: ObjectId,
        description: str,
        employee_ids: list[ObjectId],
        elements_values_maps: dict,
        data: dict = Depends(security.get_current_user),
) -> str:
    all_element_ids = {
        element["element_id"]
        for employee_elements in elements_values_maps.values()
        for element in employee_elements
    }
    existing_pairs = set()
    if employee_ids and all_element_ids:
        existing_documents = await payroll_runs_employees_elements_collection.find(
            {
                "employee_id": {"$in": employee_ids},
                "element_id": {"$in": list(all_element_ids)},
                "period_id": period_id,
                "payroll_id": payroll_id,
            },
            {"employee_id": 1, "element_id": 1},
        ).to_list(None)
        existing_pairs = {
            (document["employee_id"], document["element_id"])
            for document in existing_documents
        }

    async with database.client.start_session() as session:
        try:
            await session.start_transaction()

            company_id = ObjectId(data.get("company_id"))
            now = security.now_utc()

            new_run_counter = await create_custom_counter(
                "PRN",
                "R",
                description="Payroll Run Number",
                data=data,
                session=session,
            )
            run_counter = new_run_counter["final_counter"] if new_run_counter["success"] else None

            run_result = await payroll_runs_collection.insert_one(
                {
                    "company_id": company_id,
                    "run_number": run_counter,
                    "payroll_id": payroll_id,
                    "period_id": period_id,
                    "description": description,
                    "payment_number": "",
                    "createdAt": now,
                    "updatedAt": now,
                },
                session=session,
            )
            run_id = run_result.inserted_id

            run_employee_ids = []
            if employee_ids:
                run_employees_result = await payroll_runs_employees_collection.insert_many(
                    [
                        {
                            "company_id": company_id,
                            "run_id": run_id,
                            "period_id": period_id,
                            "payroll_id": payroll_id,
                            "employee_id": employee_id,
                            "createdAt": now,
                            "updatedAt": now,
                        }
                        for employee_id in employee_ids
                    ],
                    ordered=True,
                    session=session,
                )
                run_employee_ids = run_employees_result.inserted_ids

            run_elements = []
            for employee_id, run_employee_id in zip(employee_ids, run_employee_ids):
                for element in elements_values_maps.get(employee_id, []):
                    value = element["value"]
                    if (employee_id, element["element_id"]) in existing_pairs:
                        value = 0

                    run_elements.append({
                        "company_id": company_id,
                        "run_employee_id": run_employee_id,
                        "employee_id": employee_id,
                        "element_id": element["element_id"],
                        "value": value,
                        "payroll_element_id": element["payroll_element_id"],
                        "run_id": run_id,
                        "number": element["number"],
                        "period_id": period_id,
                        "payroll_id": payroll_id,
                        "createdAt": now,
                        "updatedAt": now,
                    })

            if run_elements:
                await payroll_runs_employees_elements_collection.insert_many(
                    run_elements,
                    ordered=True,
                    session=session,
                )

            await session.commit_transaction()
            return str(run_id)
        except Exception:
            await session.abort_transaction()
            raise


@router.post("/payroll_run")
async def payroll_run(run: PayrollRunModel, data: dict = Depends(security.get_current_user)):
    try:
        payroll_id = run.payroll_id
        period_id = run.period_id
        employee_id = run.employee_id
        element_id = run.element_id
        all_employees = []
        # === period section ===:
        period_document = await payroll_period_details_collection.find_one({"_id": period_id})
        period_start_date = period_document.get("start_date")
        period_end_date = period_document.get("end_date")

        if not period_start_date or not period_end_date:
            raise HTTPException(status_code=400, detail="period dates are required")

        # === employees section ===:
        employee_filter = {
            "payroll": payroll_id,
            "hire_date": {"$lte": period_end_date},
            "$or": [
                {"end_date": {"$gte": period_start_date}},
                {"end_date": None}
            ]
        }
        employee_projection = {
            "_id": 1,
            "hire_date": 1,
            "end_date": 1,
            "full_name": 1,
            "legislation": 1,
        }

        if employee_id:
            employee_filter["_id"] = employee_id
            employee_document = await employees_collection.find_one(employee_filter, employee_projection)
            if employee_document:
                all_employees.append(employee_document)
        else:
            employees = await employees_collection.find(employee_filter, employee_projection).to_list(None)
            all_employees.extend(employees)

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

        payroll_elements_task = employees_payrolls_collection.find(
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
        loans_task = employees_loan_and_advances_collection.find(
            {
                "employee_id": {"$in": employee_ids},
                "deduction_date": {"$lte": period_end_date},
            },
            {"_id": 1, "employee_id": 1, "total_amount": 1, "monthly_installment": 1, "type": 1},
        ).to_list(None)
        employee_values_task = employees_payrolls_collection.find(
            {"employee_id": {"$in": employee_ids}},
            {"employee_id": 1, "name": 1, "value": 1},
        ).to_list(None)

        all_payroll_elements, all_employee_leaves, all_employee_loans, all_employee_values = await asyncio.gather(
            payroll_elements_task,
            leaves_task,
            loans_task,
            employee_values_task,
        )

        payroll_elements_by_employee = {employee_id: [] for employee_id in employee_ids}
        for payroll_element in all_payroll_elements:
            payroll_elements_by_employee.setdefault(
                payroll_element["employee_id"], []
            ).append(payroll_element)

        leaves_by_employee = {employee_id: [] for employee_id in employee_ids}
        for leave in all_employee_leaves:
            leaves_by_employee.setdefault(leave["employee_id"], []).append(leave)

        loans_by_employee = {employee_id: [] for employee_id in employee_ids}
        for loan in all_employee_loans:
            loans_by_employee.setdefault(loan["employee_id"], []).append(loan)

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
        leave_types_by_id = {document["_id"]: document for document in leave_type_documents}
        loan_types_by_id = {document["_id"]: document for document in loan_type_documents}

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
            employee_payrolls_by_id[employee_value["_id"]] = employee_value
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
            for based_element in based_elements:
                value = employee_values_by_name.get(
                    (current_employee_id, based_element.get("name")),
                    0,
                )
                if (based_element.get("type") or "Add").strip().lower() == "subtract":
                    total_value -= value
                else:
                    total_value += value
            return total_value

        # === loop employees ===:
        description = ""
        elements_values_maps = {}
        for employee in all_employees:
            current_employee_id = employee.get("_id")
            elements_values_maps[current_employee_id] = []
            employee_hire_date = employee.get("hire_date") or datetime.min
            employee_end_date = employee.get("end_date") or datetime.max
            employee_name = employee.get("full_name") or None
            description = employee_name if len(all_employees) == 1 else "All Employees"
            legislation = employee.get("legislation") or None
            legislation_document = legislations_by_id.get(legislation)

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

            # === employee leaves ===:
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

        run_id: str = await save_payroll_run(payroll_id, period_id, description,
                                             [item["_id"] for item in all_employees],
                                             elements_values_maps, data)

        details = await get_payroll_runs_details(run_id, data)
        return {"added_run": details["payroll_runs_details"]}


    except Exception:
        raise


# ==== PY_INPUT_VALUE_FF ====
async def py_input_value_ff(employee_hire_date: datetime, employee_end_date: datetime,
                            element_start: datetime,
                            element_value: float, element_end: datetime, period_start_date: datetime,
                            period_end_date: datetime):
    try:
        date1 = max(employee_hire_date, element_start, period_start_date)
        date2 = min(employee_end_date, element_end, period_end_date)
        # number_of_leave_days_dict = await get_leave_days(employee_id, date1, date2, company_id)

        # number_of_leave_days = number_of_leave_days_dict['number_of_leave_days']
        # working_days = max((date2 - date1).days + 1, 0) - number_of_leave_days
        working_days = max((date2 - date1).days + 1, 0)
        period_days = get_period_days(period_start_date, period_end_date)

        if period_days == 0:
            final_value = 0
        else:
            final_value = element_value * (working_days / period_days)

        return round(final_value, 2)

    except Exception as e:
        raise e


# ==== PY_ANNUAL_LEAVE_ENTITLEMENT_FF ====
async def py_annual_leave_entitlement_ff(employee_hire_date: datetime, employee_end_date: datetime,
                                         element_start: datetime,
                                         element_value: float, element_end: datetime, period_start_date: datetime,
                                         period_end_date: datetime, based_element_id: ObjectId, employee_id: ObjectId,
                                         based_value: Optional[float] = None):
    try:
        value = based_value
        if value is None:
            value = await get_employee_element_value(based_element_id, employee_id)
        date1 = max(employee_hire_date, element_start, period_start_date)
        date2 = min(employee_end_date, element_end, period_end_date)
        if date2 < date1:
            return 0, 0
        working_days = max((date2 - date1).days + 1, 0)
        period_days = get_period_days(period_start_date, period_end_date)

        if period_days == 0:
            l_days = 0
        else:
            l_days = element_value / 12 * (working_days / period_days)
        final_value = l_days / 30 * value
        return round(l_days, 2), round(final_value, 2)

    except Exception as e:
        raise e


# ==== PY_ANNUAL_LEAVE_FF ====
async def py_annual_leave_ff(leave_id: ObjectId, employee_id: ObjectId, period_start_date: datetime,
                             period_end_date: datetime,
                             based_element_id: ObjectId, leave_start_date: datetime, leave_end_date: datetime,
                             is_pay_in_advanced: bool, user_data: dict,
                             based_value: Optional[float] = None):
    try:
        value = based_value
        if value is None:
            value = await get_employee_element_value(based_element_id, employee_id)
        # period_days = get_period_days(period_start_date, period_end_date)

        date1 = max(period_start_date, leave_start_date)
        date2 = min(period_end_date, leave_end_date)
        # l_days = (date2 - date1).days + 1
        number_of_days = await calculate_number_of_days(str(employee_id),
                                                        NumberOfDaysForWorkingDaysModel(start_date=date1,
                                                                                        end_date=date2,
                                                                                        leave_type=str(leave_id)),
                                                        user_data)
        l_days: int = number_of_days['working_days']

        final_value = round(((value or 0) * (l_days * 12 / 365)), 2)

        return l_days, final_value

    except Exception:
        raise


# ==== PY_UNPAID_LEAVE_FF ====
async def py_unpaid_leave_ff(leave_id: ObjectId, employee_id: ObjectId, period_start_date: datetime,
                             period_end_date: datetime,
                             based_element_id: ObjectId, leave_start_date: datetime, leave_end_date: datetime,
                             user_data: dict, based_value: Optional[float] = None):
    try:
        value = based_value
        if value is None:
            value = await get_employee_element_value(based_element_id, employee_id)
        period_days = get_period_days(period_start_date, period_end_date)

        date1 = max(period_start_date, leave_start_date)
        date2 = min(period_end_date, leave_end_date)
        # l_days = (date2 - date1).days + 1
        number_of_days = await calculate_number_of_days(str(employee_id),
                                                        NumberOfDaysForWorkingDaysModel(start_date=date1,
                                                                                        end_date=date2,
                                                                                        leave_type=str(leave_id)),
                                                        user_data)
        l_days: int = number_of_days['working_days']

        final_value = round(((value or 0) * (l_days / period_days)), 2)

        return final_value, l_days

    except Exception:
        raise


# ==== PY_SICK_LEAVE_FF ====
async def py_sick_leave_ff(leave_id: ObjectId, employee_id: ObjectId, period_start_date: datetime,
                           period_end_date: datetime,
                           based_element_id: ObjectId, legislation: ObjectId, leave_start_date: datetime,
                           leave_end_date: datetime, user_data: dict,
                           based_value: Optional[float] = None,
                           legislation_document: Optional[dict] = None):
    try:
        value = based_value
        if value is None:
            value = await get_employee_element_value(based_element_id, employee_id)
        period_days = get_period_days(period_start_date, period_end_date)

        legislation_doc = legislation_document
        if legislation_doc is None:
            legislation_doc = await legislations_collection.find_one({"_id": legislation})
        if not legislation_doc:
            raise HTTPException(status_code=404, detail="Legislation not found")

        full_limit = legislation_doc.get("number_of_paid_days_for_sick_leave", 0)
        half_limit = legislation_doc.get("number_of_half_paid_days_for_sick_leave", 0)

        #  get previously used days
        used_days_before = await get_used_leave_days("SL", employee_id, leave_start_date, period_start_date)

        date1 = max(period_start_date, leave_start_date)
        date2 = min(period_end_date, leave_end_date)
        # l_days = (date2 - date1).days + 1
        number_of_days = await calculate_number_of_days(str(employee_id),
                                                        NumberOfDaysForWorkingDaysModel(start_date=date1,
                                                                                        end_date=date2,
                                                                                        leave_type=str(leave_id)),
                                                        user_data)
        l_days: int = number_of_days['working_days']

        remaining_days = l_days
        total_value = 0

        # === FULL PAID ===
        remaining_full = max(0, full_limit - used_days_before)
        full_paid_days = min(remaining_days, remaining_full)
        remaining_days -= full_paid_days

        # === HALF PAID ===
        used_after_full = max(0, used_days_before - full_limit)
        remaining_half = max(0, half_limit - used_after_full)

        half_paid_days = min(remaining_days, remaining_half)

        total_value += value * (half_paid_days / period_days) * 0.5
        remaining_days -= half_paid_days

        # === UNPAID ===
        if remaining_days > 0:
            total_value += value * (remaining_days / period_days) * 1.0

        return round(total_value, 2), l_days

    except Exception:
        raise


# ==== PY_MATERNITY_LEAVE_FF ====
async def py_maternity_leave_ff(leave_id: ObjectId, employee_id: ObjectId, period_start_date: datetime,
                                period_end_date: datetime,
                                based_element_id: ObjectId, legislation: ObjectId, leave_start_date: datetime,
                                leave_end_date: datetime, user_data: dict,
                                based_value: Optional[float] = None,
                                legislation_document: Optional[dict] = None):
    try:
        value = based_value
        if value is None:
            value = await get_employee_element_value(based_element_id, employee_id)
        period_days = get_period_days(period_start_date, period_end_date)

        legislation_doc = legislation_document
        if legislation_doc is None:
            legislation_doc = await legislations_collection.find_one({"_id": legislation})
        if not legislation_doc:
            raise HTTPException(status_code=404, detail="Legislation not found")

        full_limit = legislation_doc.get("number_of_paid_days_for_maternity_leave", 0)

        used_days_before = get_current_leave_used_days(period_start_date, leave_start_date)

        date1 = max(period_start_date, leave_start_date)
        date2 = min(period_end_date, leave_end_date)

        # l_days = (date2 - date1).days + 1
        number_of_days = await calculate_number_of_days(str(employee_id),
                                                        NumberOfDaysForWorkingDaysModel(start_date=date1,
                                                                                        end_date=date2,
                                                                                        leave_type=str(leave_id)),
                                                        user_data)
        l_days: int = number_of_days['working_days']

        remaining_days = l_days
        total_value = 0

        # === FULL PAID ===
        remaining_full = max(0, full_limit - used_days_before)
        full_paid_days = min(remaining_days, remaining_full)
        remaining_days -= full_paid_days

        if remaining_days > 0:
            total_value = value * (remaining_days / period_days)

        return round(total_value, 2), l_days

    except Exception:
        raise


# ==== PY_PATERNITY_LEAVE_FF ====
async def py_paternity_leave_ff(leave_id: ObjectId, employee_id: ObjectId, period_start_date: datetime,
                                period_end_date: datetime,
                                based_element_id: ObjectId, legislation: ObjectId, leave_start_date: datetime,
                                leave_end_date: datetime, user_data: dict,
                                based_value: Optional[float] = None,
                                legislation_document: Optional[dict] = None):
    try:
        value = based_value
        if value is None:
            value = await get_employee_element_value(based_element_id, employee_id)
        period_days = get_period_days(period_start_date, period_end_date)

        legislation_doc = legislation_document
        if legislation_doc is None:
            legislation_doc = await legislations_collection.find_one({"_id": legislation})
        if not legislation_doc:
            raise HTTPException(status_code=404, detail="Legislation not found")

        full_limit = legislation_doc.get("number_of_paid_days_for_paternity_leave", 0)

        used_days_before = get_current_leave_used_days(period_start_date, leave_start_date)

        date1 = max(period_start_date, leave_start_date)
        date2 = min(period_end_date, leave_end_date)

        # l_days = (date2 - date1).days + 1
        number_of_days = await calculate_number_of_days(str(employee_id),
                                                        NumberOfDaysForWorkingDaysModel(start_date=date1,
                                                                                        end_date=date2,
                                                                                        leave_type=str(leave_id)),
                                                        user_data)
        l_days: int = number_of_days['working_days']

        remaining_days = l_days
        total_value = 0

        # === FULL PAID ===
        remaining_full = max(0, full_limit - used_days_before)
        full_paid_days = min(remaining_days, remaining_full)
        remaining_days -= full_paid_days

        if remaining_days > 0:
            total_value = value * (remaining_days / period_days)

        return round(total_value, 2), l_days

    except Exception:
        raise


# ==== PY_COMPASSIONATE_LEAVE_FF ====
async def py_compassionate_leave_ff(leave_id: ObjectId, employee_id: ObjectId, period_start_date: datetime,
                                    period_end_date: datetime,
                                    based_element_id: ObjectId, legislation: ObjectId, leave_start_date: datetime,
                                    leave_end_date: datetime, user_data: dict,
                                    based_value: Optional[float] = None,
                                    legislation_document: Optional[dict] = None):
    try:
        value = based_value
        if value is None:
            value = await get_employee_element_value(based_element_id, employee_id)
        period_days = get_period_days(period_start_date, period_end_date)

        legislation_doc = legislation_document
        if legislation_doc is None:
            legislation_doc = await legislations_collection.find_one({"_id": legislation})
        if not legislation_doc:
            raise HTTPException(status_code=404, detail="Legislation not found")

        full_limit = legislation_doc.get("number_of_paid_days_for_compassionate_leave", 0)

        used_days_before = get_current_leave_used_days(period_start_date, leave_start_date)

        date1 = max(period_start_date, leave_start_date)
        date2 = min(period_end_date, leave_end_date)

        # l_days = (date2 - date1).days + 1
        number_of_days = await calculate_number_of_days(str(employee_id),
                                                        NumberOfDaysForWorkingDaysModel(start_date=date1,
                                                                                        end_date=date2,
                                                                                        leave_type=str(leave_id)),
                                                        user_data)
        l_days: int = number_of_days['working_days']

        remaining_days = l_days
        total_value = 0

        # === FULL PAID ===
        remaining_full = max(0, full_limit - used_days_before)
        full_paid_days = min(remaining_days, remaining_full)
        remaining_days -= full_paid_days

        if remaining_days > 0:
            total_value = value * (remaining_days / period_days)

        return round(total_value, 2), l_days

    except Exception:
        raise


# ==== PY_OVERTIME_NORMAL_FF ====
async def py_overtime_normal_ff(employee_id: ObjectId, period_start_date: datetime, period_end_date: datetime,
                                based_element_id: ObjectId, legislation: ObjectId, element_value: float,
                                based_value: Optional[float] = None,
                                legislation_document: Optional[dict] = None):
    try:
        value = based_value
        if value is None:
            value = await get_employee_element_value(based_element_id, employee_id)
        # No. of Month Days
        period_days = get_period_days(period_start_date, period_end_date)

        legislation_doc = legislation_document
        if legislation_doc is None:
            legislation_doc = await legislations_collection.find_one({"_id": legislation})
        if not legislation_doc:
            raise HTTPException(status_code=404, detail="Legislation not found")
        # No. of working hours
        working_hours = legislation_doc.get("number_of_working_hours_for_overtime_normal", 0)

        total_value = element_value / working_hours / period_days * value
        return round(total_value, 2)

    except Exception as e:
        raise e


# ==== PY_OVERTIME_HOLIDAYS_FF ====
async def py_overtime_holidays_ff(employee_id: ObjectId, period_start_date: datetime, period_end_date: datetime,
                                  based_element_id: ObjectId, legislation: ObjectId, element_value: float,
                                  based_value: Optional[float] = None,
                                  legislation_document: Optional[dict] = None):
    try:
        value = based_value
        if value is None:
            value = await get_employee_element_value(based_element_id, employee_id)
        # No. of Month Days
        period_days = get_period_days(period_start_date, period_end_date)

        legislation_doc = legislation_document
        if legislation_doc is None:
            legislation_doc = await legislations_collection.find_one({"_id": legislation})
        if not legislation_doc:
            raise HTTPException(status_code=404, detail="Legislation not found")
        # No. of working hours
        working_hours = legislation_doc.get("number_of_working_hours_for_overtime_holidays", 0)

        total_value = element_value / working_hours / period_days * value
        return round(total_value, 2)

    except Exception as e:
        raise e


# ==== PY_NONRECURRING_FF ====
async def py_nonrecurring_ff(period_start_date: datetime, period_end_date: datetime,
                             element_start: datetime, element_end: datetime, element_value: float):
    try:
        if is_within_period(element_start, element_end, period_start_date, period_end_date):
            return element_value
        else:
            return None

    except Exception as e:
        raise e


# ==== PY_SOCIAL_SECURITY_EMPLOYEE_FF ====
async def py_social_security_employee_ff(employee_id: ObjectId, based_element_id: ObjectId, legislation: ObjectId,
                                         based_value: Optional[float] = None,
                                         legislation_document: Optional[dict] = None):
    try:
        value = based_value
        if value is None:
            value = await get_employee_element_value(based_element_id, employee_id)

        legislation_doc = legislation_document
        if legislation_doc is None:
            legislation_doc = await legislations_collection.find_one({"_id": legislation})
        if not legislation_doc:
            raise HTTPException(status_code=404, detail="Legislation not found")
        # No. of working hours
        social_security_employee_percentage = legislation_doc.get("social_security_employee_percentage", 0) / 100
        social_security_ceiling = legislation_doc.get("social_security_ceiling", 0)
        if not social_security_ceiling or social_security_ceiling == 0:
            social_security_ceiling = value

        social_security_employee = social_security_employee_percentage * min(value, social_security_ceiling)
        return round(social_security_employee, 2)

    except Exception as e:
        raise e


# ==== PY_SOCIAL_SECURITY_EMPLOYER_FF ====
async def py_social_security_employer_ff(employee_id: ObjectId, based_element_id: ObjectId, legislation: ObjectId,
                                         based_value: Optional[float] = None,
                                         legislation_document: Optional[dict] = None):
    try:
        value = based_value
        if value is None:
            value = await get_employee_element_value(based_element_id, employee_id)

        legislation_doc = legislation_document
        if legislation_doc is None:
            legislation_doc = await legislations_collection.find_one({"_id": legislation})
        if not legislation_doc:
            raise HTTPException(status_code=404, detail="Legislation not found")
        # No. of working hours
        social_security_employer_percentage = legislation_doc.get("social_security_employer_percentage", 0) / 100
        social_security_ceiling = legislation_doc.get("social_security_ceiling", 0)
        if not social_security_ceiling or social_security_ceiling == 0:
            social_security_ceiling = value

        social_security_employer = social_security_employer_percentage * min(value, social_security_ceiling)
        return round(social_security_employer, 2)

    except Exception as e:
        raise e


# ==== PY_GRATUITY_ACCRUAL_FF ====
async def py_gratuity_accrual_ff(employee_id: ObjectId, employee_hire_date: datetime, employee_end_date: datetime,
                                 element_start: datetime, element_end: datetime, period_start_date: datetime,
                                 period_end_date: datetime, based_element_id: ObjectId, legislation: ObjectId,
                                 based_value: Optional[float] = None,
                                 legislation_document: Optional[dict] = None):
    try:
        basic_salary = based_value
        if basic_salary is None:
            basic_salary = await get_employee_element_value(based_element_id, employee_id)
        legislation_doc = legislation_document
        if legislation_doc is None:
            legislation_doc = await legislations_collection.find_one({"_id": legislation})

        if not legislation_doc:
            raise HTTPException(status_code=404, detail="Legislation not found")

        gratuity_first_5_years = legislation_doc.get("gratuity_first_5_years", 21)
        gratuity_after_5_years = legislation_doc.get("gratuity_after_5_years", 30)

        date1 = max(employee_hire_date, element_start)
        date2 = min(employee_end_date, element_end, period_end_date)

        if date2 < date1:
            return 0

        total_service_days = (date2 - employee_hire_date).days + 1
        first_5_years_days = min(total_service_days, 5 * 365)
        after_5_years_days = max(total_service_days - (5 * 365), 0)
        gratuity_days_first_5 = (first_5_years_days / 365) * gratuity_first_5_years
        gratuity_days_after_5 = (after_5_years_days / 365) * gratuity_after_5_years
        total_gratuity_days = gratuity_days_first_5 + gratuity_days_after_5
        total_gratuity_liability = (total_gratuity_days * basic_salary) / 30
        previous_accrued_amount = await get_previous_gratuity_accrual(
            employee_id=employee_id,
        )
        current_period_accrual = (total_gratuity_liability - previous_accrued_amount)
        return round(current_period_accrual, 2)
    except Exception as e:
        raise e


# ==== PY_LOAN_AND_ADVANCES_FF ====
async def py_loan_and_advances_ff(loan_id: ObjectId, total_amount: float, monthly_installment: float,
                                  paid_to_date: Optional[float] = None):
    try:
        if paid_to_date is None:
            paid_cursor = await payroll_runs_employees_elements_collection.aggregate([
                {
                    "$match": {
                        "element_id": loan_id,
                    }
                },
                {
                    "$group": {
                        "_id": None,
                        "paid_to_date": {
                            "$sum": {
                                "$ifNull": [
                                    "$value", 0
                                ]
                            }
                        }
                    }
                }
            ])
            paid_result = await paid_cursor.to_list(1)
            paid_to_date = paid_result[0]["paid_to_date"] if paid_result else 0

        remaining_amount = max((total_amount or 0) - paid_to_date, 0)
        return round(min(monthly_installment or 0, remaining_amount), 2)

    except Exception as e:
        raise e


# ==== PY_SERVICE_TAX_FF ====
async def py_service_tax_ff(employee_id: ObjectId, based_element_id: ObjectId, legislation: ObjectId,
                            based_value: Optional[float] = None,
                            legislation_document: Optional[dict] = None):
    try:
        value = based_value
        if value is None:
            value = await get_employee_element_value(based_element_id, employee_id)

        legislation_doc = legislation_document
        if legislation_doc is None:
            legislation_doc = await legislations_collection.find_one({"_id": legislation})
        if not legislation_doc:
            raise HTTPException(status_code=404, detail="Legislation not found")

        service_tax_percentage = to_float(legislation_doc.get("service_tax_percentage")) / 100
        service_tax = (value or 0) * service_tax_percentage
        return round(service_tax, 2)

    except Exception as e:
        raise e


# ==== PY_INCOME_TAX_DEDUCTION_FF ====
async def py_income_tax_deduction_ff(element_value: float, employee_id: ObjectId, based_element_id: ObjectId,
                                     legislation: ObjectId,
                                     period_start_date: datetime, period_end_date: datetime,
                                     based_value: Optional[float] = None,
                                     legislation_document: Optional[dict] = None):
    try:
        value = based_value
        if value is None:
            value = await get_employee_element_value(based_element_id, employee_id)
        income_tax_exemption = element_value
        taxable_income_before_exemption = (value or 0)

        legislation_doc = legislation_document
        if legislation_doc is None:
            legislation_doc = await legislations_collection.find_one({"_id": legislation})
        if not legislation_doc:
            raise HTTPException(status_code=404, detail="Legislation not found")

        taxable_amount = max(taxable_income_before_exemption - income_tax_exemption, 0) * 12
        income_tax = calculate_progressive_income_tax(
            taxable_amount,
            income_tax_brackets(legislation_doc),
        )

        return round(income_tax / 12, 2)

    except Exception as e:
        raise e


# === ROLLBACK ===
@router.delete("/rollback_payroll_run/{run_id}")
async def rollback_payroll_run(run_id: str, _: dict = Depends(security.get_current_user)):
    try:
        run_id = ObjectId(run_id)
    except InvalidId:
        raise HTTPException(status_code=400, detail="Invalid run_id")

    async with database.client.start_session() as session:
        try:
            await session.start_transaction()

            # Delete children first
            await payroll_runs_employees_elements_collection.delete_many(
                {"run_id": run_id}, session=session
            )
            await payroll_runs_employees_collection.delete_many(
                {"run_id": run_id}, session=session
            )

            # Delete main run
            result = await payroll_runs_collection.delete_one(
                {"_id": run_id}, session=session
            )

            if result.deleted_count == 0:
                await session.abort_transaction()
                raise HTTPException(status_code=404, detail="Payroll run not found")

            await session.commit_transaction()

            return {"message": "Payroll run rolled back successfully"}

        except Exception as e:
            await session.abort_transaction()
            raise HTTPException(status_code=500, detail=str(e))


## ====================================================================================================================================================================================================
## ====================================================================================================================================================================================================


all_payroll_runs_pipeline = [
    {
        '$lookup': {
            'from': 'payroll',
            'localField': 'payroll_id',
            'foreignField': '_id',
            'as': 'payroll_details'
        }
    }, {
        '$lookup': {
            'from': 'payroll_period_details',
            'localField': 'period_id',
            'foreignField': '_id',
            'as': 'period_details'
        }
    }, {
        '$addFields': {
            'payroll_name': {
                '$ifNull': [
                    {
                        '$first': '$payroll_details.name'
                    }, None
                ]
            },
            'period_name': {
                '$ifNull': [
                    {
                        '$first': '$period_details.period_name'
                    }, None
                ]
            }
        }
    }, {
        '$project': {
            '_id': {
                '$toString': '$_id'
            },
            'run_number': 1,
            'description': 1,
            'payment_number': 1,
            'payroll_name': 1,
            'period_name': 1
        }
    }
]

payroll_runs_details_pipeline = [
    {
        '$lookup': {
            'from': 'payroll',
            'localField': 'payroll_id',
            'foreignField': '_id',
            'pipeline': [
                {
                    '$project': {
                        '_id': 0,
                        'name': 1
                    }
                }
            ],
            'as': 'payroll_details'
        }
    }, {
        '$lookup': {
            'from': 'payroll_period_details',
            'localField': 'period_id',
            'foreignField': '_id',
            'pipeline': [
                {
                    '$project': {
                        '_id': 0,
                        'period_name': 1,
                        'start_date': 1,
                        'end_date': 1
                    }
                }
            ],
            'as': 'period_details'
        }
    }, {
        '$lookup': {
            'from': 'payroll_runs_employees',
            'localField': '_id',
            'foreignField': 'run_id',
            'pipeline': [
                {
                    '$lookup': {
                        'from': 'employees',
                        'localField': 'employee_id',
                        'foreignField': '_id',
                        'pipeline': [
                            {
                                '$project': {
                                    '_id': 0,
                                    'full_name': 1,
                                    'people_counter': 1
                                }
                            }
                        ],
                        'as': 'employee_details'
                    }
                }, {
                    '$lookup': {
                        'from': 'payroll_runs_employees_elements',
                        'localField': '_id',
                        'foreignField': 'run_employee_id',
                        'pipeline': [
                            {
                                '$lookup': {
                                    'from': 'payroll_elements',
                                    'localField': 'payroll_element_id',
                                    'foreignField': '_id',
                                    'pipeline': [
                                        {
                                            '$project': {
                                                '_id': 0,
                                                'name': 1,
                                                'type': 1
                                            }
                                        }
                                    ],
                                    'as': 'element_details'
                                }
                            }, {
                                '$set': {
                                    'element_name': {
                                        '$first': '$element_details.name'
                                    },
                                    'element_type': {
                                        '$first': '$element_details.type'
                                    }
                                }
                            }, {
                                '$project': {
                                    '_id': {
                                        '$toString': '$_id'
                                    },
                                    'value': 1,
                                    'element_name': 1,
                                    'element_type': 1,
                                    'priority': 1,
                                    'payment': {
                                        '$cond': [
                                            {
                                                '$eq': [
                                                    '$element_type', 'Earning'
                                                ]
                                            }, '$value', 0
                                        ]
                                    },
                                    'deduction': {
                                        '$cond': [
                                            {
                                                '$eq': [
                                                    '$element_type', 'Deduction'
                                                ]
                                            }, '$value', 0
                                        ]
                                    },
                                    'information': {
                                        '$cond': [
                                            {
                                                '$eq': [
                                                    '$element_type', 'Information'
                                                ]
                                            }, '$value', 0
                                        ]
                                    },
                                    'number': {
                                        '$cond': [
                                            {
                                                '$eq': [
                                                    '$element_type', 'Information'
                                                ]
                                            }, '$number', 0
                                        ]
                                    }
                                }
                            }, {
                                '$sort': {
                                    'priority': 1
                                }
                            }
                        ],
                        'as': 'run_employee_details'
                    }
                }, {
                    '$lookup': {
                        'from': 'employees_bank_accounts',
                        'localField': 'employee_id',
                        'foreignField': 'employee_id',
                        'pipeline': [
                            {
                                '$sort': {
                                    'createdAt': -1
                                }
                            }, {
                                '$limit': 1
                            }, {
                                '$lookup': {
                                    'from': 'all_lists_values',
                                    'localField': 'bank_name',
                                    'foreignField': '_id',
                                    'pipeline': [
                                        {
                                            '$project': {
                                                '_id': 0,
                                                'name': 1
                                            }
                                        }
                                    ],
                                    'as': 'bank_name_details'
                                }
                            }, {
                                '$project': {
                                    '_id': 0,
                                    'bank_name': {
                                        '$first': '$bank_name_details.name'
                                    },
                                    'account_number': 1,
                                    'iban': 1,
                                    'swift_code': 1
                                }
                            }
                        ],
                        'as': 'bank_account_details'
                    }
                }, {
                    '$set': {
                        'employee_name': {
                            '$first': '$employee_details.full_name'
                        },
                        'employee_number': {
                            '$first': '$employee_details.people_counter'
                        },
                        'bank_account': {
                            '$first': '$bank_account_details'
                        },
                        'total_payments': {
                            '$sum': '$run_employee_details.payment'
                        },
                        'total_deductions': {
                            '$sum': '$run_employee_details.deduction'
                        }
                    }
                }, {
                    '$set': {
                        'net_salary': {
                            '$subtract': [
                                '$total_payments', '$total_deductions'
                            ]
                        }
                    }
                }, {
                    '$set': {
                        'run_employee_information': {
                            '$filter': {
                                'input': '$run_employee_details',
                                'as': 'el',
                                'cond': {
                                    '$eq': [
                                        '$$el.element_type', 'Information'
                                    ]
                                }
                            }
                        },
                        'run_employee_details': {
                            '$filter': {
                                'input': '$run_employee_details',
                                'as': 'el',
                                'cond': {
                                    '$ne': [
                                        '$$el.element_type', 'Information'
                                    ]
                                }
                            }
                        }
                    }
                }, {
                    '$project': {
                        '_id': {
                            '$toString': '$_id'
                        },
                        'employee_name': 1,
                        'employee_number': 1,
                        'bank_name': '$bank_account.bank_name',
                        'account_number': '$bank_account.account_number',
                        'iban': '$bank_account.iban',
                        'swift_code': '$bank_account.swift_code',
                        'total_payments': 1,
                        'total_deductions': 1,
                        'net_salary': 1,
                        'run_employee_details': 1,
                        'run_employee_information': 1
                    }
                }, {
                    '$sort': {
                        'employee_name': 1
                    }
                }
            ],
            'as': 'employees_details'
        }
    }, {
        '$project': {
            '_id': {
                '$toString': '$_id'
            },
            'run_number': 1,
            'description': 1,
            'payment_number': 1,
            'payroll_name': {
                '$first': '$payroll_details.name'
            },
            'period_name': {
                '$first': '$period_details.period_name'
            },
            'period_start_date': {
                '$first': '$period_details.start_date'
            },
            'period_end_date': {
                '$first': '$period_details.end_date'
            },
            'employees_details': 1
        }
    }
]


@router.get("/get_all_payroll_runs")
async def get_all_payroll_runs(data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        new_pipeline: Any = copy.deepcopy(all_payroll_runs_pipeline)
        new_pipeline.insert(0, {"$match": {"company_id": company_id}})
        new_pipeline.append({"$sort": {"period_name": -1}})
        cursor = await payroll_runs_collection.aggregate(new_pipeline)
        results = await cursor.to_list(None)
        return {"payroll_runs": results}

    except Exception:
        raise


@router.get("/get_payroll_runs_details/{run_id}")
async def get_payroll_runs_details(run_id: str, data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        run_id = ObjectId(run_id)
        new_pipeline: Any = copy.deepcopy(payroll_runs_details_pipeline)
        new_pipeline.insert(0, {"$match": {"company_id": company_id, "_id": run_id}})
        cursor = await payroll_runs_collection.aggregate(new_pipeline)
        results = await cursor.to_list(None)

        # # =================================================
        # start = datetime(2026, 4, 1)
        # end = datetime(2026, 4, 30)
        # await get_leave_days(ObjectId("69cfa8718f07622eb9ce9b68"), start, end)
        # # =================================================

        # # =================================================
        # await get_payroll_element_value(ObjectId("69d64ad68fc5df07583ec9a8"),ObjectId("69cfa8718f07622eb9ce9b68"))
        # # =================================================

        return {"payroll_runs_details": results[0] if len(results) > 0 else None}

    except Exception:
        raise


@router.patch("/prepare_bank_export/{run_id}")
async def prepare_bank_export(run_id: str, data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        run_object_id = ObjectId(run_id)
        payroll_run_document = await payroll_runs_collection.find_one({
            "_id": run_object_id,
            "company_id": company_id
        })

        if not payroll_run_document:
            raise HTTPException(status_code=404, detail="Payroll run not found")

        payment_number = payroll_run_document.get("payment_number") or ""
        if not payment_number:
            new_payment_counter = await create_custom_counter("PPN", "PP", description="Payroll Payment Number",
                                                              data=data)
            payment_number = new_payment_counter["final_counter"] if new_payment_counter["success"] else ""

        await payroll_runs_collection.update_one(
            {"_id": run_object_id, "company_id": company_id},
            {"$set": {
                "payment_number": payment_number,
                "bank_exported_at": security.now_utc(),
                "updatedAt": security.now_utc(),
            }}
        )

        details = await get_payroll_runs_details(run_id, data)
        return {"payroll_runs_details": details["payroll_runs_details"]}

    except InvalidId:
        raise HTTPException(status_code=400, detail="Invalid payroll run id")
    except HTTPException:
        raise
    except Exception:
        raise


##### ============= FUNCTIONS TO GET LOVs ============= #####
@router.get("/get_payroll_for_lov")
async def get_payroll_for_lov(data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        cursor = await payroll_collection.aggregate([
            {"$match": {"company_id": company_id}},
            {"$set": {"_id": {"$toString": "$_id"}}},
            {"$project": {
                "_id": 1,
                "name": 1
            }}
        ])
        results = await cursor.to_list(None)
        return {"all_payrolls": results}

    except Exception:
        raise


@router.get("/get_payroll_periods_for_lov/{payroll_id}")
async def get_payroll_periods_for_lov(payroll_id: str, _: dict = Depends(security.get_current_user)):
    try:
        payroll_id = ObjectId(payroll_id)
        cursor = await payroll_period_details_collection.aggregate([
            {"$match": {"payroll_id": payroll_id, "status": "Active"}},
            {"$set": {"_id": {"$toString": "$_id"}}},
            {"$project": {
                "_id": 1,
                "period_name": 1
            }},
            {"$sort": {"period_name": -1}}
        ])
        results = await cursor.to_list(None)
        return {"all_periods": results}

    except Exception:
        raise


@router.get("/get_all_employees_for_payroll_runs_lov/{payroll_id}")
async def get_all_employees_for_payroll_runs_lov(payroll_id: str, _: dict = Depends(security.get_current_user)):
    try:
        payroll_id = ObjectId(payroll_id)
        cursor = await employees_collection.aggregate([
            {"$match": {"payroll": payroll_id}},
            {"$set": {"_id": {"$toString": "$_id"}}},
            {"$project": {
                "_id": 1,
                "full_name": 1
            }},
        ])
        results = await cursor.to_list(None)
        return {"all_employees": results}

    except Exception:
        raise

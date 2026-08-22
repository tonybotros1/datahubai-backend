"""Persist a completed payroll run in one transaction."""

from bson import ObjectId
from fastapi import Depends

from app import database
from app.core import security
from app.routes.counters import create_custom_counter
from ..collections import (
    payroll_runs_collection,
    payroll_runs_employees_collection,
    payroll_runs_employees_elements_collection,
)

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

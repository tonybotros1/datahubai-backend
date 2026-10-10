import unittest
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

from bson import ObjectId
from fastapi import HTTPException

from app.routes import employees
from app.routes.payroll_run.sections.context import PayrollPeriod, PayrollRunContext
from app.routes.payroll_run.sections.payroll_elements import (
    calculate_employee_payroll_elements,
)


class EmployeePayrollTypeTests(unittest.IsolatedAsyncioTestCase):
    async def test_prepare_keeps_type_when_definition_requires_it(self):
        company_id = ObjectId()
        element_id = ObjectId()
        list_id = ObjectId()
        type_id = ObjectId()
        payroll = {"name": str(element_id), "type": "  Monthly  "}
        element_collection = SimpleNamespace(
            find_one=AsyncMock(return_value={"has_type": True}),
        )
        list_collection = SimpleNamespace(
            find_one=AsyncMock(return_value={"_id": list_id, "status": True}),
        )
        value_collection = SimpleNamespace(
            find_one=AsyncMock(return_value={"_id": type_id, "name": "Monthly"}),
        )

        with patch.object(
            employees,
            "payroll_elements_collection",
            element_collection,
        ), patch.object(
            employees,
            "all_lists_collection",
            list_collection,
        ), patch.object(
            employees,
            "all_lists_values_collection",
            value_collection,
        ):
            await employees.prepare_employee_payroll(payroll, company_id)

        self.assertEqual(payroll["name"], element_id)
        self.assertEqual(payroll["type"], type_id)
        self.assertEqual(payroll["type_name"], "Monthly")
        element_collection.find_one.assert_awaited_once_with(
            {"_id": element_id, "company_id": company_id},
            {"has_type": 1},
        )

    async def test_prepare_requires_type_when_enabled(self):
        payroll = {"name": str(ObjectId()), "type": ""}
        collection = SimpleNamespace(
            find_one=AsyncMock(return_value={"has_type": True}),
        )

        with patch.object(employees, "payroll_elements_collection", collection):
            with self.assertRaises(HTTPException) as context:
                await employees.prepare_employee_payroll(payroll, ObjectId())

        self.assertEqual(context.exception.status_code, 400)
        self.assertEqual(context.exception.detail, "Type is required")

    async def test_prepare_clears_type_when_definition_does_not_use_it(self):
        payroll = {"name": str(ObjectId()), "type": "Monthly"}
        collection = SimpleNamespace(
            find_one=AsyncMock(return_value={"has_type": False}),
        )

        with patch.object(employees, "payroll_elements_collection", collection):
            await employees.prepare_employee_payroll(payroll, ObjectId())

        self.assertEqual(payroll["type"], "")
        self.assertEqual(payroll["type_name"], "")

    async def test_prepare_rejects_type_from_outside_company_list(self):
        company_id = ObjectId()
        list_id = ObjectId()
        payroll = {"name": str(ObjectId()), "type": str(ObjectId())}
        element_collection = SimpleNamespace(
            find_one=AsyncMock(return_value={"has_type": True}),
        )
        list_collection = SimpleNamespace(
            find_one=AsyncMock(return_value={"_id": list_id, "status": True}),
        )
        value_collection = SimpleNamespace(find_one=AsyncMock(return_value=None))

        with patch.object(
            employees,
            "payroll_elements_collection",
            element_collection,
        ), patch.object(
            employees,
            "all_lists_collection",
            list_collection,
        ), patch.object(
            employees,
            "all_lists_values_collection",
            value_collection,
        ):
            with self.assertRaises(HTTPException) as context:
                await employees.prepare_employee_payroll(payroll, company_id)

        self.assertEqual(context.exception.status_code, 400)
        self.assertEqual(context.exception.detail, "Invalid Type")

    async def test_migration_turns_existing_text_types_into_list_values(self):
        assignment_id = ObjectId()
        company_id = ObjectId()
        list_id = ObjectId()
        type_id = ObjectId()
        list_collection = SimpleNamespace(
            find_one=AsyncMock(return_value={"_id": list_id, "status": True}),
        )
        value_collection = SimpleNamespace(
            find_one=AsyncMock(return_value=None),
            insert_one=AsyncMock(return_value=SimpleNamespace(inserted_id=type_id)),
        )
        payroll_collection = SimpleNamespace(
            find=MagicMock(
                return_value=SimpleNamespace(
                    to_list=AsyncMock(
                        return_value=[
                            {
                                "_id": assignment_id,
                                "company_id": company_id,
                                "type": "Overtime",
                            }
                        ]
                    )
                )
            ),
            update_one=AsyncMock(),
        )

        with patch.object(
            employees,
            "all_lists_collection",
            list_collection,
        ), patch.object(
            employees,
            "all_lists_values_collection",
            value_collection,
        ), patch.object(
            employees,
            "employees_payrolls_collection",
            payroll_collection,
        ):
            await employees.migrate_employee_payroll_type_values()

        saved_value = value_collection.insert_one.await_args.args[0]
        self.assertEqual(saved_value["list_id"], list_id)
        self.assertEqual(saved_value["company_id"], company_id)
        self.assertEqual(saved_value["name"], "Overtime")
        payroll_collection.update_one.assert_awaited_once_with(
            {"_id": assignment_id},
            {"$set": {"type": type_id, "type_name": "Overtime"}},
        )

    async def test_payroll_run_carries_employee_assignment_type(self):
        employee_id = ObjectId()
        assignment_id = ObjectId()
        definition_id = ObjectId()
        period = PayrollPeriod(
            start_date=datetime(2026, 10, 1),
            end_date=datetime(2026, 10, 31),
        )
        context = PayrollRunContext(
            payroll_elements_by_employee={
                employee_id: [
                    {
                        "_id": assignment_id,
                        "employee_id": employee_id,
                        "name": definition_id,
                        "type": ObjectId(),
                        "type_name": "Monthly",
                        "value": 2500,
                    }
                ]
            },
            leaves_by_employee={},
            loans_by_employee={},
            loan_payments_by_id={},
            processed_element_ids=set(),
            leave_types_by_id={},
            loan_types_by_id={},
            payroll_definitions_by_id={
                definition_id: {"function": "PY_INPUT_VALUE_FF"}
            },
            legislations_by_id={},
            employee_element_value=lambda _element_id, _employee_id: 0,
        )

        with patch(
            "app.routes.payroll_run.sections.payroll_elements.py_input_value_ff",
            new=AsyncMock(return_value=2500),
        ):
            results = await calculate_employee_payroll_elements(
                {"_id": employee_id},
                period,
                context,
            )

        self.assertEqual(results[0]["employee_type"], "Monthly")


if __name__ == "__main__":
    unittest.main()

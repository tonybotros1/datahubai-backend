import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from bson import ObjectId
from fastapi import HTTPException

from app.routes import employees


class EmployeePayrollTypeTests(unittest.IsolatedAsyncioTestCase):
    async def test_prepare_keeps_type_when_definition_requires_it(self):
        company_id = ObjectId()
        element_id = ObjectId()
        payroll = {"name": str(element_id), "type": "  Monthly  "}
        collection = SimpleNamespace(
            find_one=AsyncMock(return_value={"has_type": True}),
        )

        with patch.object(employees, "payroll_elements_collection", collection):
            await employees.prepare_employee_payroll(payroll, company_id)

        self.assertEqual(payroll["name"], element_id)
        self.assertEqual(payroll["type"], "Monthly")
        collection.find_one.assert_awaited_once_with(
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


if __name__ == "__main__":
    unittest.main()

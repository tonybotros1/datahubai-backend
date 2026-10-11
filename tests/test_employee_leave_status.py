import unittest
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from bson import ObjectId
from fastapi import HTTPException

from app.routes import employees


class EmployeeLeaveStatusTests(unittest.IsolatedAsyncioTestCase):
    def _leave_model(self, status: str):
        return employees.EmployeeLeavesModel(
            leave_type=str(ObjectId()),
            start_date=datetime(2026, 10, 1, tzinfo=timezone.utc),
            end_date=datetime(2026, 10, 2, tzinfo=timezone.utc),
            number_of_days=2,
            status=status,
        )

    async def test_create_honors_posted_status(self):
        company_id = ObjectId()
        employee_id = ObjectId()
        inserted_id = ObjectId()
        collection = SimpleNamespace(
            insert_one=AsyncMock(
                return_value=SimpleNamespace(inserted_id=inserted_id),
            ),
        )

        with patch.object(
            employees,
            "employees_leaves_collection",
            collection,
        ), patch.object(
            employees,
            "get_employee_leave_details",
            AsyncMock(return_value={"_id": str(inserted_id), "status": "Posted"}),
        ):
            response = await employees.add_new_employee_leave(
                str(employee_id),
                self._leave_model("Posted"),
                {"company_id": str(company_id)},
            )

        self.assertEqual(response["new_leave"]["status"], "Posted")
        saved_document = collection.insert_one.await_args.args[0]
        self.assertEqual(saved_document["status"], "Posted")
        self.assertEqual(saved_document["company_id"], company_id)
        self.assertEqual(saved_document["employee_id"], employee_id)

    async def test_create_rejects_unknown_status(self):
        collection = SimpleNamespace(insert_one=AsyncMock())

        with patch.object(
            employees,
            "employees_leaves_collection",
            collection,
        ):
            with self.assertRaises(HTTPException) as context:
                await employees.add_new_employee_leave(
                    str(ObjectId()),
                    self._leave_model("Approved"),
                    {"company_id": str(ObjectId())},
                )

        self.assertEqual(context.exception.status_code, 422)
        collection.insert_one.assert_not_awaited()

    async def test_new_leave_can_be_cancelled(self):
        company_id = ObjectId()
        leave_id = ObjectId()
        collection = SimpleNamespace(
            find_one=AsyncMock(return_value={"_id": leave_id, "status": "New"}),
            update_one=AsyncMock(
                return_value=SimpleNamespace(matched_count=1),
            ),
        )

        with patch.object(
            employees,
            "employees_leaves_collection",
            collection,
        ), patch.object(
            employees,
            "get_employee_leave_details",
            AsyncMock(
                return_value={"_id": str(leave_id), "status": "Cancelled"},
            ),
        ):
            response = await employees.update_employee_leave(
                str(leave_id),
                self._leave_model("Cancelled"),
                {"company_id": str(company_id)},
            )

        self.assertEqual(response["update_leave"]["status"], "Cancelled")
        update_filter, update_document = collection.update_one.await_args.args
        self.assertEqual(update_filter["_id"], leave_id)
        self.assertEqual(update_filter["company_id"], company_id)
        self.assertEqual(update_filter["status"], "New")
        self.assertEqual(update_document["$set"]["status"], "Cancelled")

    async def test_posted_leave_cannot_be_changed(self):
        leave_id = ObjectId()
        collection = SimpleNamespace(
            find_one=AsyncMock(
                return_value={"_id": leave_id, "status": "Posted"},
            ),
            update_one=AsyncMock(),
        )

        with patch.object(
            employees,
            "employees_leaves_collection",
            collection,
        ):
            with self.assertRaises(HTTPException) as context:
                await employees.update_employee_leave(
                    str(leave_id),
                    self._leave_model("Cancelled"),
                    {"company_id": str(ObjectId())},
                )

        self.assertEqual(context.exception.status_code, 409)
        collection.update_one.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()

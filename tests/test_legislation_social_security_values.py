import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

from bson import ObjectId
from fastapi import HTTPException

from app.routes import legislation


class LegislationSocialSecurityValuesTests(unittest.IsolatedAsyncioTestCase):
    async def test_lists_company_and_legislation_scoped_employee_values(self):
        company_id = ObjectId()
        legislation_id = ObjectId()
        element_id = ObjectId()
        assignment_id = ObjectId()
        employee_id = ObjectId()
        expected_row = {
            "_id": str(assignment_id),
            "employee_id": str(employee_id),
            "employee_name": "Eman Jaradat",
            "employee_number": "PE-0012",
            "social_security_registration_number": "SS-123",
            "payroll_element_name": "Social Security Employee",
            "value": 3612,
            "has_override": True,
        }
        element_cursor = SimpleNamespace(
            to_list=AsyncMock(return_value=[{"_id": element_id}]),
        )
        value_cursor = SimpleNamespace(
            to_list=AsyncMock(return_value=[expected_row]),
        )
        legislation_collection = SimpleNamespace(
            find_one=AsyncMock(return_value={"_id": legislation_id}),
        )
        element_collection = SimpleNamespace(
            find=MagicMock(return_value=element_cursor),
        )
        assignment_collection = SimpleNamespace(
            aggregate=AsyncMock(return_value=value_cursor),
        )

        with patch.object(
            legislation,
            "legislations_collection",
            legislation_collection,
        ), patch.object(
            legislation,
            "payroll_elements_collection",
            element_collection,
        ), patch.object(
            legislation,
            "employees_payrolls_collection",
            assignment_collection,
        ):
            response = await legislation.get_social_security_employee_values(
                str(legislation_id),
                {"company_id": str(company_id)},
            )

        self.assertEqual(response, {"employee_values": [expected_row]})
        legislation_collection.find_one.assert_awaited_once_with(
            {"_id": legislation_id, "company_id": company_id},
            {"_id": 1},
        )
        element_filter = element_collection.find.call_args.args[0]
        self.assertEqual(element_filter["company_id"], company_id)
        self.assertEqual(
            element_filter["function"]["$regex"],
            "^PY_SOCIAL_SECURITY_EMPLOYEE_FF$",
        )

        pipeline = assignment_collection.aggregate.await_args.args[0]
        self.assertEqual(pipeline[0]["$match"], {
            "company_id": company_id,
            "name": {"$in": [element_id]},
        })
        self.assertIn("has_override", pipeline[1]["$set"])
        employee_lookup = pipeline[2]["$lookup"]
        employee_scope = employee_lookup["pipeline"][0]["$match"]["$expr"]["$and"]
        self.assertIn({"$eq": ["$company_id", company_id]}, employee_scope)
        self.assertIn(
            {"$eq": ["$legislation", legislation_id]},
            employee_scope,
        )
        self.assertEqual(
            pipeline[-1]["$sort"],
            {"has_override": -1, "employee_name": 1, "start_date": -1},
        )

    async def test_returns_empty_when_company_has_no_social_security_element(self):
        company_id = ObjectId()
        legislation_id = ObjectId()
        element_cursor = SimpleNamespace(to_list=AsyncMock(return_value=[]))
        legislation_collection = SimpleNamespace(
            find_one=AsyncMock(return_value={"_id": legislation_id}),
        )
        element_collection = SimpleNamespace(
            find=MagicMock(return_value=element_cursor),
        )
        assignment_collection = SimpleNamespace(aggregate=AsyncMock())

        with patch.object(
            legislation,
            "legislations_collection",
            legislation_collection,
        ), patch.object(
            legislation,
            "payroll_elements_collection",
            element_collection,
        ), patch.object(
            legislation,
            "employees_payrolls_collection",
            assignment_collection,
        ):
            response = await legislation.get_social_security_employee_values(
                str(legislation_id),
                {"company_id": str(company_id)},
            )

        self.assertEqual(response, {"employee_values": []})
        assignment_collection.aggregate.assert_not_awaited()

    async def test_rejects_an_invalid_legislation_id(self):
        with self.assertRaises(HTTPException) as context:
            await legislation.get_social_security_employee_values(
                "not-an-object-id",
                {"company_id": str(ObjectId())},
            )

        self.assertEqual(context.exception.status_code, 400)
        self.assertEqual(context.exception.detail, "Invalid legislation")

    async def test_clear_override_unsets_only_the_scoped_assignment_value(self):
        company_id = ObjectId()
        legislation_id = ObjectId()
        assignment_id = ObjectId()
        employee_id = ObjectId()
        element_id = ObjectId()
        legislation_collection = SimpleNamespace(
            find_one=AsyncMock(return_value={"_id": legislation_id}),
        )
        assignment_collection = SimpleNamespace(
            find_one=AsyncMock(return_value={
                "_id": assignment_id,
                "employee_id": employee_id,
                "name": element_id,
            }),
            update_one=AsyncMock(
                return_value=SimpleNamespace(matched_count=1),
            ),
        )
        element_collection = SimpleNamespace(
            find_one=AsyncMock(return_value={"_id": element_id}),
        )
        employee_collection = SimpleNamespace(
            find_one=AsyncMock(return_value={"_id": employee_id}),
        )

        with patch.object(
            legislation,
            "legislations_collection",
            legislation_collection,
        ), patch.object(
            legislation,
            "employees_payrolls_collection",
            assignment_collection,
        ), patch.object(
            legislation,
            "payroll_elements_collection",
            element_collection,
        ), patch.object(
            legislation,
            "employees_collection",
            employee_collection,
        ):
            response = await legislation.clear_social_security_employee_override(
                str(legislation_id),
                str(assignment_id),
                {"company_id": str(company_id)},
            )

        self.assertEqual(response, {
            "cleared_assignment_id": str(assignment_id),
        })
        employee_collection.find_one.assert_awaited_once_with({
            "_id": employee_id,
            "company_id": company_id,
            "legislation": legislation_id,
        }, {"_id": 1})
        update_filter, update = assignment_collection.update_one.await_args.args
        self.assertEqual(update_filter, {
            "_id": assignment_id,
            "company_id": company_id,
            "employee_id": employee_id,
            "name": element_id,
        })
        self.assertEqual(update["$unset"], {"value": ""})
        self.assertIn("updatedAt", update["$set"])


if __name__ == "__main__":
    unittest.main()

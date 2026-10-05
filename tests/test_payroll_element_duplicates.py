import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from bson import ObjectId
from fastapi import HTTPException

from app.routes import payroll_elements


class PayrollElementDuplicateTests(unittest.IsolatedAsyncioTestCase):
    async def test_create_rejects_duplicate_based_element(self):
        company_id = ObjectId()
        payroll_element_id = ObjectId()
        based_element_id = ObjectId()
        collection = SimpleNamespace(
            find_one=AsyncMock(return_value={"_id": ObjectId()}),
            insert_one=AsyncMock(),
        )

        with patch.object(
            payroll_elements,
            "payroll_elements_based_elements_collection",
            collection,
        ):
            with self.assertRaises(HTTPException) as context:
                await payroll_elements.add_new_based_element(
                    str(payroll_element_id),
                    payroll_elements.BasedElementsModel(
                        name=str(based_element_id),
                        type="Add",
                    ),
                    {"company_id": str(company_id)},
                )

        self.assertEqual(context.exception.status_code, 409)
        self.assertEqual(
            context.exception.detail,
            "This based element is already added.",
        )
        collection.insert_one.assert_not_awaited()

    async def test_update_excludes_current_row_when_checking_duplicates(self):
        company_id = ObjectId()
        payroll_element_id = ObjectId()
        based_element_id = ObjectId()
        current_link_id = ObjectId()
        collection = SimpleNamespace(
            find_one=AsyncMock(
                side_effect=[
                    {"payroll_element_id": payroll_element_id},
                    {"_id": ObjectId()},
                ],
            ),
            update_one=AsyncMock(),
        )

        with patch.object(
            payroll_elements,
            "payroll_elements_based_elements_collection",
            collection,
        ):
            with self.assertRaises(HTTPException) as context:
                await payroll_elements.update_based_element(
                    str(current_link_id),
                    payroll_elements.BasedElementsModel(
                        name=str(based_element_id),
                        type="Subtract",
                    ),
                    {"company_id": str(company_id)},
                )

        self.assertEqual(context.exception.status_code, 409)
        duplicate_filter = collection.find_one.await_args_list[1].args[0]
        self.assertEqual(duplicate_filter["payroll_element_id"], payroll_element_id)
        self.assertEqual(duplicate_filter["name"], based_element_id)
        self.assertEqual(duplicate_filter["_id"], {"$ne": current_link_id})
        collection.update_one.assert_not_awaited()

    async def test_create_saves_a_unique_based_element(self):
        company_id = ObjectId()
        payroll_element_id = ObjectId()
        based_element_id = ObjectId()
        inserted_id = ObjectId()
        collection = SimpleNamespace(
            find_one=AsyncMock(return_value=None),
            insert_one=AsyncMock(
                return_value=SimpleNamespace(inserted_id=inserted_id),
            ),
        )

        with patch.object(
            payroll_elements,
            "payroll_elements_based_elements_collection",
            collection,
        ):
            response = await payroll_elements.add_new_based_element(
                str(payroll_element_id),
                payroll_elements.BasedElementsModel(
                    name=str(based_element_id),
                    type="Add",
                ),
                {"company_id": str(company_id)},
            )

        self.assertEqual(response, {"added_based_element_id": str(inserted_id)})
        saved_document = collection.insert_one.await_args.args[0]
        self.assertEqual(saved_document["company_id"], company_id)
        self.assertEqual(saved_document["payroll_element_id"], payroll_element_id)
        self.assertEqual(saved_document["name"], based_element_id)


if __name__ == "__main__":
    unittest.main()

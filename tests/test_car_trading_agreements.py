"""Offline contract tests. No MongoDB connection, lifespan, or writes are used.

Run from the backend root with: python -m unittest discover -s tests -v
"""

import copy
import unittest
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

from bson import ObjectId
from fastapi import FastAPI, HTTPException
from pydantic import ValidationError

# Prevent application imports from ever constructing a real database client.
with patch.dict("os.environ", {"MONGO_URI": "mongodb://localhost:27017", "DATABASE_NAME": "offline_tests"}):
    with patch("pymongo.AsyncMongoClient", return_value=MagicMock()):
        from app.routes.car_trading import router
        from app.routes.car_trading import car_trading, sales_agreements
        from app.routes.car_trading.models import CarTradingModel, CarTradingSearch, PurchaseAgreementModel
        from app.routes import companies


COMPANY_ID = ObjectId()
TRADE_ID = ObjectId()
AGREEMENT_ID = ObjectId()
USER = {"company_id": str(COMPANY_ID)}


def agreement_values(**overrides):
    return {
        "trade_id": str(TRADE_ID),
        "agreement_date": datetime(2026, 9, 15),
        "seller_name": "Test Seller",
        "buyer_name": "Test Buyer",
        "agreement_amount": 1000.0,
        **overrides,
    }


class AgreementModelTests(unittest.TestCase):
    def test_defaults_do_not_become_patch_fields(self):
        model = PurchaseAgreementModel(agreement_note="Updated note")
        self.assertEqual(model.agreement_type, "sell")
        self.assertEqual(model.payment_method, "")
        self.assertEqual(model.model_dump(exclude_unset=True), {"agreement_note": "Updated note"})

    def test_invalid_enums_and_nonfinite_or_negative_money_are_rejected(self):
        cases = [
            {"agreement_type": "purchase"},
            {"agreement_type": None},
            {"payment_method": "card"},
        ]
        for field in ("agreement_amount", "agreement_down_payment"):
            for value in (-1, float("nan"), float("inf"), float("-inf")):
                cases.append({field: value})
        for values in cases:
            with self.subTest(values=values), self.assertRaises(ValidationError):
                PurchaseAgreementModel(**values)

    def test_openapi_retains_agreement_routes_and_exposes_new_fields(self):
        app = FastAPI()
        app.include_router(router, prefix="/car_trading")
        schema = app.openapi()
        for path, method in (
            ("/get_purchase_agreement_for_current_trade/{trade_id}", "get"),
            ("/add_purchase_agreement_item", "post"),
            ("/update_purchase_agreement_item/{purchase_item_id}", "patch"),
            ("/delete_purchase_agreement_item/{purchase_id}", "delete"),
        ):
            self.assertIn(method, schema["paths"]["/car_trading" + path])
        properties = schema["components"]["schemas"]["PurchaseAgreementModel"]["properties"]
        self.assertEqual(properties["agreement_type"]["enum"], ["buy", "sell"])
        self.assertEqual(properties["agreement_type"]["default"], "sell")
        self.assertEqual(properties["payment_method"]["enum"], ["", "cash", "bank_transfer", "cheque", "other"])
        self.assertNotIn("profit_margin_scheme", properties)
        self.assertIn("engine_number", schema["components"]["schemas"]["CarTradingModel"]["properties"])


class AgreementRouteTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.collection = SimpleNamespace(
            insert_one=AsyncMock(return_value=SimpleNamespace(inserted_id=AGREEMENT_ID)),
            find_one=AsyncMock(),
            update_one=AsyncMock(return_value=SimpleNamespace(matched_count=1)),
            aggregate=AsyncMock(),
        )
        self.broadcast = AsyncMock()
        self.ensure_trade = AsyncMock()
        self.counter = AsyncMock(return_value={"success": True, "final_counter": "CM-0001"})
        for target, value in (
            ("all_trades_purchase_agreement_items_collection", self.collection),
            ("ensure_trade_belongs_to_company", self.ensure_trade),
            ("create_custom_counter", self.counter),
        ):
            patcher = patch.object(sales_agreements, target, value)
            patcher.start()
            self.addCleanup(patcher.stop)
        patcher = patch.object(sales_agreements.manager, "send_to_company", self.broadcast)
        patcher.start()
        self.addCleanup(patcher.stop)

    def prepare_existing(self, **values):
        existing = {
            **agreement_values(**values),
            "_id": AGREEMENT_ID,
            "trade_id": TRADE_ID,
            "company_id": COMPANY_ID,
        }
        self.collection.find_one.return_value = existing

        async def update_one(query, update):
            existing.update(copy.deepcopy(update["$set"]))
            return SimpleNamespace(matched_count=1)

        async def aggregate(pipeline):
            serialized = {**existing, "_id": str(AGREEMENT_ID), "trade_id": str(TRADE_ID), "company_id": str(COMPANY_ID)}
            return SimpleNamespace(to_list=AsyncMock(return_value=[serialized]))

        self.collection.update_one.side_effect = update_one
        self.collection.aggregate.side_effect = aggregate
        return existing

    async def test_buy_creation_retains_actual_zero_partial_and_full_payment(self):
        for paid in (0, 300, 1000):
            with self.subTest(paid=paid):
                response = await sales_agreements.add_purchase_agreement_item(
                    PurchaseAgreementModel(**agreement_values(
                        agreement_type="buy", agreement_down_payment=paid,
                        payment_method="bank_transfer",
                    )), USER,
                )
                self.assertEqual(response["data"]["agreement_type"], "buy")
                self.assertEqual(response["data"]["agreement_down_payment"], paid)
                self.assertEqual(response["data"]["payment_method"], "bank_transfer")
                self.assertNotIn("profit_margin_scheme", response["data"])
                self.assertEqual(self.broadcast.call_args.args[1]["type"], "purchase_agreement_item_created")
                self.ensure_trade.assert_awaited_with(TRADE_ID, COMPANY_ID)

    async def test_omitted_type_defaults_to_sell_and_null_paid_to_zero(self):
        response = await sales_agreements.add_purchase_agreement_item(
            PurchaseAgreementModel(**agreement_values(agreement_down_payment=None)), USER,
        )
        self.assertEqual(response["data"]["agreement_type"], "sell")
        self.assertEqual(response["data"]["agreement_down_payment"], 0)
        self.assertEqual(response["data"]["payment_method"], "")
        self.assertNotIn("profit_margin_scheme", response["data"])

    async def test_create_rejects_missing_zero_and_overpaid_amounts_before_writing(self):
        for values in (
            {"agreement_amount": None}, {"agreement_amount": 0},
            {"agreement_down_payment": 1001},
        ):
            with self.subTest(values=values), self.assertRaises(HTTPException) as error:
                await sales_agreements.add_purchase_agreement_item(
                    PurchaseAgreementModel(**agreement_values(**values)), USER,
                )
            self.assertEqual(error.exception.status_code, 400)
        self.counter.assert_not_awaited()
        self.collection.insert_one.assert_not_awaited()
        self.broadcast.assert_not_awaited()

    async def test_get_normalizes_missing_and_null_fields_without_database_updates(self):
        legacy = [
            {"_id": "legacy", "profit_margin_scheme": True},
            {"agreement_type": None, "agreement_down_payment": None},
        ]
        self.collection.aggregate.return_value = SimpleNamespace(to_list=AsyncMock(return_value=legacy))
        response = await sales_agreements.get_purchase_agreement_for_current_trade(str(TRADE_ID), USER)
        for item in response["purchase_agreement_items"]:
            self.assertEqual(item["agreement_type"], "sell")
            self.assertEqual(item["agreement_down_payment"], 0)
            self.assertEqual(item["payment_method"], "")
            self.assertNotIn("profit_margin_scheme", item)
        self.assertNotIn("agreement_type", legacy[0])
        self.collection.update_one.assert_not_awaited()

    async def test_partial_patch_keeps_saved_buy_type_and_money(self):
        self.prepare_existing(agreement_type="buy", agreement_down_payment=300,
                              payment_method="cash", profit_margin_scheme=True)
        response = await sales_agreements.update_purchase_agreement_item(
            str(AGREEMENT_ID), PurchaseAgreementModel(agreement_note="A new note"), USER,
        )
        item = response["data"]
        self.assertEqual(item["agreement_type"], "buy")
        self.assertEqual(item["agreement_amount"], 1000)
        self.assertEqual(item["agreement_down_payment"], 300)
        self.assertEqual(item["payment_method"], "cash")
        self.assertNotIn("profit_margin_scheme", item)
        self.assertEqual(self.broadcast.call_args.args[1]["type"], "purchase_agreement_item_updated")
        self.collection.find_one.assert_awaited_once_with({"_id": AGREEMENT_ID, "company_id": COMPANY_ID})

    async def test_patch_validates_new_price_against_stored_payment(self):
        self.prepare_existing(agreement_type="buy", agreement_down_payment=300)
        with self.assertRaises(HTTPException) as error:
            await sales_agreements.update_purchase_agreement_item(
                str(AGREEMENT_ID), PurchaseAgreementModel(agreement_amount=200), USER,
            )
        self.assertEqual(error.exception.status_code, 400)
        self.collection.update_one.assert_not_awaited()
        self.broadcast.assert_not_awaited()

    async def test_patch_legacy_defaults_and_explicit_null_paid(self):
        self.prepare_existing(agreement_type=None, agreement_down_payment=300)
        response = await sales_agreements.update_purchase_agreement_item(
            str(AGREEMENT_ID), PurchaseAgreementModel(agreement_down_payment=None), USER,
        )
        self.assertEqual(response["data"]["agreement_type"], "sell")
        self.assertEqual(response["data"]["agreement_down_payment"], 0)

    async def test_patch_can_explicitly_change_type(self):
        self.prepare_existing(agreement_type="sell")
        response = await sales_agreements.update_purchase_agreement_item(
            str(AGREEMENT_ID), PurchaseAgreementModel(agreement_type="buy"), USER,
        )
        self.assertEqual(response["data"]["agreement_type"], "buy")

    async def test_patch_missing_or_other_company_record_cannot_write(self):
        self.collection.find_one.return_value = None
        with self.assertRaises(HTTPException) as error:
            await sales_agreements.update_purchase_agreement_item(
                str(AGREEMENT_ID), PurchaseAgreementModel(agreement_type="buy"), USER,
            )
        self.assertEqual(error.exception.status_code, 404)
        self.collection.update_one.assert_not_awaited()


class EngineNumberTests(unittest.IsolatedAsyncioTestCase):
    async def test_engine_number_saved_on_create_and_partial_update(self):
        collection = SimpleNamespace(
            insert_one=AsyncMock(return_value=SimpleNamespace(inserted_id=TRADE_ID)),
            update_one=AsyncMock(return_value=SimpleNamespace(matched_count=1)),
        )
        with patch.object(car_trading, "all_trades_collection", collection):
            await car_trading.add_new_trade(CarTradingModel(
                date=datetime(2026, 9, 15), car_brand=str(ObjectId()), car_model=str(ObjectId()),
                engine_number=" ENGINE-123 ",
            ), USER)
            self.assertEqual(collection.insert_one.call_args.args[0]["engine_number"], "ENGINE-123")
            await car_trading.update_trade(str(TRADE_ID), CarTradingModel(engine_number=" ENGINE-456 "), USER)
            self.assertEqual(collection.update_one.call_args.args[1]["$set"]["engine_number"], "ENGINE-456")
            await car_trading.update_trade(str(TRADE_ID), CarTradingModel(note="New note"), USER)
            self.assertNotIn("engine_number", collection.update_one.call_args.args[1]["$set"])

    async def test_both_trade_read_projections_include_engine_number(self):
        collection = SimpleNamespace(aggregate=AsyncMock(return_value=SimpleNamespace(to_list=AsyncMock(return_value=[]))))
        with patch.object(car_trading, "all_trades_collection", collection), patch.object(car_trading, "ensure_car_trading_indexes", AsyncMock()):
            await car_trading.get_all_cars(USER)
            get_pipeline = collection.aggregate.call_args.args[0]
            self.assertEqual(get_pipeline[-1]["$project"]["engine_number"], {"$ifNull": ["$engine_number", ""]})
            await car_trading.search_engine_for_car_trading(CarTradingSearch(all=True), USER)
            search_pipeline = collection.aggregate.call_args.args[0]
            projections = [stage["$project"] for stage in search_pipeline if "$project" in stage]
            self.assertTrue(any("engine_number" in projection for projection in projections))


class CompanyDetailsTests(unittest.IsolatedAsyncioTestCase):
    async def test_owner_contact_fields_are_exposed_for_agreement_prefill(self):
        collection = SimpleNamespace(
            aggregate=AsyncMock(
                return_value=SimpleNamespace(
                    to_list=AsyncMock(return_value=[{"_id": COMPANY_ID}]),
                ),
            ),
        )
        with patch.object(companies, "companies_collection", collection):
            await companies.get_current_company_details(
                {"company_id": str(COMPANY_ID), "sub": str(ObjectId())},
            )

        pipeline = collection.aggregate.call_args.args[0]
        owner_lookup = next(
            stage["$lookup"]
            for stage in pipeline
            if stage.get("$lookup", {}).get("as") == "user_details"
        )
        owner_projection = owner_lookup["pipeline"][-1]["$project"]
        self.assertEqual(owner_projection["email"], 1)
        self.assertEqual(owner_projection["phone_number"], 1)
        self.assertEqual(owner_projection["address"], 1)

        enriched = next(
            stage["$addFields"]
            for stage in reversed(pipeline)
            if "owner_email" in stage.get("$addFields", {})
        )
        self.assertEqual(enriched["owner_email"]["$ifNull"][0], "$user_details.email")
        self.assertEqual(
            enriched["owner_phone"]["$ifNull"][0],
            "$user_details.phone_number",
        )
        self.assertEqual(
            enriched["owner_address"]["$ifNull"][0],
            "$user_details.address",
        )


if __name__ == "__main__":
    unittest.main()

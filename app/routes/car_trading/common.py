from datetime import datetime, timedelta
from typing import Any, Optional

from bson import ObjectId
from fastapi import HTTPException

from app.database import get_collection


all_trades_collection = get_collection("all_trades")
all_trades_items_collection = get_collection("all_trades_items")
all_trades_purchase_agreement_items_collection = get_collection(
    "all_trades_purchase_agreement_items"
)
all_capitals_collection = get_collection("all_capitals")
all_outstanding_collection = get_collection("all_outstanding")
all_general_expenses_collection = get_collection("all_general_expenses")
all_trades_transfers_collection = get_collection("all_trades_transfers")
car_trading_indexes_ready = False


async def ensure_car_trading_indexes():
    global car_trading_indexes_ready
    if car_trading_indexes_ready:
        return

    trade_filter_indexes = [
        [("company_id", 1), ("status", 1)],
        [("company_id", 1), ("car_brand", 1)],
        [("company_id", 1), ("car_model", 1)],
        [("company_id", 1), ("specification", 1)],
        [("company_id", 1), ("engine_size", 1)],
        [("company_id", 1), ("bought_from", 1)],
        [("company_id", 1), ("bought_by", 1)],
        [("company_id", 1), ("sold_by", 1)],
        [("company_id", 1), ("sold_to", 1)],
        [("company_id", 1), ("invested_by", 1)],
        [("company_id", 1), ("consignment_for", 1)],
    ]
    for index in trade_filter_indexes:
        await all_trades_collection.create_index(index)

    await all_trades_items_collection.create_index([("trade_id", 1), ("company_id", 1)])
    await all_trades_purchase_agreement_items_collection.create_index([("trade_id", 1), ("company_id", 1)])
    car_trading_indexes_ready = True


def bson_serializer(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    elif isinstance(obj, ObjectId):
        return str(obj)
    elif isinstance(obj, list):
        return [bson_serializer(item) for item in obj]
    elif isinstance(obj, dict):
        return {k: bson_serializer(v) for k, v in obj.items()}
    return obj


def car_trade_search_serializer(trade: dict) -> dict:
    return bson_serializer(trade)


def parse_object_id(value: Any, field_name: str = "id") -> ObjectId:
    if value in (None, ""):
        raise HTTPException(status_code=400, detail=f"{field_name} is required")
    if isinstance(value, ObjectId):
        return value
    value = str(value)
    if not ObjectId.is_valid(value):
        raise HTTPException(status_code=400, detail=f"Invalid {field_name}")
    return ObjectId(value)


def optional_object_id(value: Any, field_name: str) -> Optional[ObjectId]:
    if value in (None, ""):
        return None
    return parse_object_id(value, field_name)


def require_payload_field(payload: dict, field_name: str, label: Optional[str] = None) -> Any:
    value = payload.get(field_name)
    if value in (None, ""):
        raise HTTPException(status_code=400, detail=f"{label or field_name} is required")
    return value


def zero_if_none(value: Optional[float]) -> float:
    return value if value is not None else 0


def exclusive_date_end(value: datetime) -> datetime:
    if (
            value.hour == 0
            and value.minute == 0
            and value.second == 0
            and value.microsecond == 0
    ):
        return value + timedelta(days=1)
    return value


async def ensure_trade_belongs_to_company(trade_id: ObjectId, company_id: ObjectId):
    trade = await all_trades_collection.find_one(
        {"_id": trade_id, "company_id": company_id},
        {"_id": 1},
    )
    if not trade:
        raise HTTPException(status_code=404, detail="Trade not found")
    return trade


def serialize(document: dict) -> dict:
    document["_id"] = str(document["_id"])
    document["company_id"] = str(document["company_id"])
    if document.get("name"):
        document["name"] = str(document["name"])
        document["name_id"] = str(document["name_id"])
    if document.get("account_name"):
        document["account_name"] = str(document["account_name"])
        document["account_name_id"] = str(document["account_name_id"])
    for key, value in list(document.items()):
        document[key] = bson_serializer(value)
    return document


def general_expenses_serialize(document: dict) -> dict:
    document["_id"] = str(document["_id"])
    document["company_id"] = str(document["company_id"])
    if document.get("trade_id"):
        document["trade_id"] = str(document["trade_id"])
    if document.get("item"):
        document["item"] = str(document["item"])
    if document.get("item_id"):
        document["item_id"] = str(document["item_id"])
    if document.get("account_name"):
        document["account_name"] = str(document["account_name"])
        document["account_name_id"] = str(document["account_name_id"])
    for key, value in list(document.items()):
        document[key] = bson_serializer(value)
    return document

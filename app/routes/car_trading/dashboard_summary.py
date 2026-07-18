import asyncio
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from app.core import security

from .bank_accounts import get_cash_on_hand_or_bank_balance
from .common import (
    all_capitals_collection,
    all_outstanding_collection,
    all_trades_collection,
    all_trades_items_collection,
)
from .last_changes import get_last_changes
from .models import LastChangesFilter

router = APIRouter()


class DashboardSummaryFilter(BaseModel):
    range: str = "month"
    from_date: Optional[datetime] = None
    to_date: Optional[datetime] = None
    compare_previous: bool = True
    status: Optional[str] = None
    timezone_offset_minutes: int = Field(default=0, ge=-720, le=840)


def _utc(value: datetime, local_timezone: timezone) -> datetime:
    if value.tzinfo is None:
        value = value.replace(tzinfo=local_timezone)
    return value.astimezone(timezone.utc)


def _resolve_period(filters: DashboardSummaryFilter) -> dict[str, Any]:
    offset = filters.timezone_offset_minutes
    local_timezone = timezone(timedelta(minutes=offset))
    # `security.now_utc()` is intentionally offset elsewhere in the legacy app.
    # The summary needs a real UTC instant because the client applies its own
    # local timezone when displaying the refresh time and calculating periods.
    now_utc = datetime.now(timezone.utc)
    local_now = now_utc.astimezone(local_timezone)
    selected_range = filters.range.strip().lower()

    if selected_range == "today":
        local_start = local_now.replace(hour=0, minute=0, second=0, microsecond=0)
        local_end = local_start + timedelta(days=1)
        label = "Today"
    elif selected_range == "year":
        local_start = local_now.replace(
            month=1,
            day=1,
            hour=0,
            minute=0,
            second=0,
            microsecond=0,
        )
        local_end = local_start.replace(year=local_start.year + 1)
        label = "This Year"
    elif selected_range == "custom" and filters.from_date is not None:
        local_start = filters.from_date
        if local_start.tzinfo is None:
            local_start = local_start.replace(tzinfo=local_timezone)
        else:
            local_start = local_start.astimezone(local_timezone)
        local_start = local_start.replace(hour=0, minute=0, second=0, microsecond=0)

        local_end = filters.to_date or filters.from_date
        if local_end.tzinfo is None:
            local_end = local_end.replace(tzinfo=local_timezone)
        else:
            local_end = local_end.astimezone(local_timezone)
        if (
            local_end.hour == 0
            and local_end.minute == 0
            and local_end.second == 0
            and local_end.microsecond == 0
        ):
            local_end += timedelta(days=1)
        if local_end <= local_start:
            local_end = local_start + timedelta(days=1)
        label = "Custom Range"
    elif selected_range == "all":
        return {
            "label": "All Time",
            "range": "all",
            "start": None,
            "end": None,
            "previous_start": None,
            "previous_end": None,
            "bucket": "month",
            "timezone": _timezone_string(offset),
            "now": now_utc,
        }
    else:
        local_start = local_now.replace(
            day=1,
            hour=0,
            minute=0,
            second=0,
            microsecond=0,
        )
        if local_start.month == 12:
            local_end = local_start.replace(year=local_start.year + 1, month=1)
        else:
            local_end = local_start.replace(month=local_start.month + 1)
        label = "This Month"
        selected_range = "month"

    start = _utc(local_start, local_timezone)
    end = _utc(local_end, local_timezone)
    duration = end - start
    previous_start = start - duration if filters.compare_previous else None
    previous_end = start if filters.compare_previous else None
    bucket = "day" if duration <= timedelta(days=62) else "month"

    return {
        "label": label,
        "range": selected_range,
        "start": start,
        "end": end,
        "previous_start": previous_start,
        "previous_end": previous_end,
        "bucket": bucket,
        "timezone": _timezone_string(offset),
        "now": now_utc,
    }


def _timezone_string(offset_minutes: int) -> str:
    sign = "+" if offset_minutes >= 0 else "-"
    absolute = abs(offset_minutes)
    return f"{sign}{absolute // 60:02d}:{absolute % 60:02d}"


def _date_match(field: str, start: Optional[datetime], end: Optional[datetime]) -> dict:
    if start is None and end is None:
        return {}
    limits: dict[str, datetime] = {}
    if start is not None:
        limits["$gte"] = start
    if end is not None:
        limits["$lt"] = end
    return {field: limits}


def _required_date_match(
    field: str,
    start: Optional[datetime],
    end: Optional[datetime],
) -> dict:
    limits: dict[str, Any] = {"$ne": None}
    if start is not None:
        limits["$gte"] = start
    if end is not None:
        limits["$lt"] = end
    return {field: limits}


def _first(rows: list[dict], default: Optional[dict] = None) -> dict:
    return rows[0] if rows else (default or {})


def _number(value: Any) -> float:
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def _integer(value: Any) -> int:
    return int(_number(value))


def _percentage_change(current: float, previous: float) -> Optional[float]:
    if previous == 0:
        return None
    return ((current - previous) / abs(previous)) * 100


def _date_bucket(field: str, period: dict[str, Any]) -> dict:
    date_format = "%Y-%m-%d" if period["bucket"] == "day" else "%Y-%m"
    return {
        "$dateToString": {
            "date": f"${field}",
            "format": date_format,
            "timezone": period["timezone"],
        }
    }


def _responsibility_name(id_field: str) -> dict:
    return {
        "$let": {
            "vars": {
                "person": {
                    "$arrayElemAt": [
                        {
                            "$filter": {
                                "input": "$responsibility_details",
                                "as": "detail",
                                "cond": {"$eq": ["$$detail._id", f"${id_field}"]},
                            }
                        },
                        0,
                    ]
                }
            },
            "in": {"$ifNull": ["$$person.name", "Unassigned"]},
        }
    }


def _vehicle_base_pipeline(company_id: ObjectId, status: Optional[str]) -> list[dict]:
    trade_match: dict[str, Any] = {"company_id": company_id}
    if status and status.strip().lower() in {"new", "sold"}:
        trade_match["status"] = status.strip().capitalize()

    return [
        {"$match": trade_match},
        {
            "$lookup": {
                "from": "all_trades_items",
                "let": {"trade_id": "$_id"},
                "pipeline": [
                    {"$match": {"$expr": {"$eq": ["$trade_id", "$$trade_id"]}}},
                    {"$sort": {"date": 1}},
                    {
                        "$lookup": {
                            "from": "all_lists_values",
                            "localField": "item",
                            "foreignField": "_id",
                            "pipeline": [{"$project": {"_id": 0, "name": 1}}],
                            "as": "item_detail",
                        }
                    },
                    {
                        "$set": {
                            "item_name": {
                                "$toUpper": {
                                    "$trim": {
                                        "input": {
                                            "$convert": {
                                                "input": {"$arrayElemAt": ["$item_detail.name", 0]},
                                                "to": "string",
                                                "onError": "",
                                                "onNull": "",
                                            }
                                        }
                                    }
                                }
                            },
                            "pay_value": {
                                "$convert": {
                                    "input": "$pay",
                                    "to": "double",
                                    "onError": 0,
                                    "onNull": 0,
                                }
                            },
                            "receive_value": {
                                "$convert": {
                                    "input": "$receive",
                                    "to": "double",
                                    "onError": 0,
                                    "onNull": 0,
                                }
                            },
                        }
                    },
                    {"$project": {"item_name": 1, "pay_value": 1, "receive_value": 1, "date": 1}},
                ],
                "as": "financial_items",
            }
        },
        {
            "$set": {
                "buy_items": {
                    "$filter": {
                        "input": "$financial_items",
                        "as": "item",
                        "cond": {"$eq": ["$$item.item_name", "BUY"]},
                    }
                },
                "sell_items": {
                    "$filter": {
                        "input": "$financial_items",
                        "as": "item",
                        "cond": {"$eq": ["$$item.item_name", "SELL"]},
                    }
                },
                "other_items": {
                    "$filter": {
                        "input": "$financial_items",
                        "as": "item",
                        "cond": {"$not": [{"$in": ["$$item.item_name", ["BUY", "SELL"]]}]},
                    }
                },
            }
        },
        {
            "$set": {
                "buy_date": {"$arrayElemAt": ["$buy_items.date", -1]},
                "sell_date": {"$arrayElemAt": ["$sell_items.date", -1]},
                "buy_price": {"$sum": "$buy_items.pay_value"},
                "sell_price": {"$sum": "$sell_items.receive_value"},
                "total_paid": {"$sum": "$financial_items.pay_value"},
                "total_received": {"$sum": "$financial_items.receive_value"},
                "vehicle_expenses": {"$sum": "$other_items.pay_value"},
                "vehicle_revenue": {"$sum": "$other_items.receive_value"},
            }
        },
        {
            "$set": {
                "total_net": {"$subtract": ["$total_received", "$total_paid"]},
                "inventory_investment": {"$subtract": ["$total_paid", "$total_received"]},
            }
        },
        {
            "$lookup": {
                "from": "all_brands",
                "localField": "car_brand",
                "foreignField": "_id",
                "pipeline": [{"$project": {"_id": 0, "name": 1}}],
                "as": "brand_detail",
            }
        },
        {
            "$lookup": {
                "from": "all_brand_models",
                "localField": "car_model",
                "foreignField": "_id",
                "pipeline": [{"$project": {"_id": 0, "name": 1, "model": 1}}],
                "as": "model_detail",
            }
        },
        {
            "$lookup": {
                "from": "all_lists_values",
                "let": {
                    "ids": ["$invested_by", "$bought_by", "$sold_by"],
                },
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {"$in": ["$_id", "$$ids"]},
                        }
                    },
                    {"$project": {"name": 1}},
                ],
                "as": "responsibility_details",
            }
        },
        {
            "$set": {
                "brand_name": {"$ifNull": [{"$arrayElemAt": ["$brand_detail.name", 0]}, "Unknown"]},
                "model_name": {
                    "$ifNull": [
                        {"$arrayElemAt": ["$model_detail.name", 0]},
                        {"$ifNull": [{"$arrayElemAt": ["$model_detail.model", 0]}, "Unknown"]},
                    ]
                },
                "status": {"$ifNull": ["$status", ""]},
                "vin": {"$ifNull": ["$vin", ""]},
                "trim": {"$ifNull": ["$trim", ""]},
                "capital_by_name": _responsibility_name("invested_by"),
                "bought_by_name": _responsibility_name("bought_by"),
                "sold_by_name": _responsibility_name("sold_by"),
            }
        },
    ]


def _people_financial_summary(
    id_field: str,
    name_field: str,
    item_match: dict,
) -> list[dict]:
    return [
        {
            "$match": {
                id_field: {"$nin": [None, ""]},
                name_field: {"$ne": "Unassigned"},
            }
        },
        {"$unwind": "$financial_items"},
        {"$match": item_match},
        {
            "$group": {
                "_id": {
                    "id": f"${id_field}",
                    "name": f"${name_field}",
                },
                "paid": {"$sum": "$financial_items.pay_value"},
                "received": {"$sum": "$financial_items.receive_value"},
                "items": {"$sum": 1},
                "cars": {"$addToSet": "$_id"},
            }
        },
        {
            "$set": {
                "net": {"$subtract": ["$received", "$paid"]},
                "car_count": {"$size": "$cars"},
            }
        },
        {"$sort": {"_id.name": 1}},
        {
            "$project": {
                "_id": 0,
                "id": {
                    "$convert": {
                        "input": "$_id.id",
                        "to": "string",
                        "onError": "",
                        "onNull": "",
                    }
                },
                "name": "$_id.name",
                "paid": 1,
                "received": 1,
                "net": 1,
                "items": 1,
                "car_count": 1,
            }
        },
    ]


def _vehicle_facet(period: dict[str, Any]) -> dict[str, list[dict]]:
    current_sold = {
        "status": "Sold",
        **_required_date_match("sell_date", period["start"], period["end"]),
    }
    current_bought = _required_date_match("buy_date", period["start"], period["end"])
    previous_sold = {
        "status": "Sold",
        **_required_date_match(
            "sell_date",
            period["previous_start"],
            period["previous_end"],
        ),
    }
    previous_bought = _required_date_match(
        "buy_date",
        period["previous_start"],
        period["previous_end"],
    )
    capital_item_match = _date_match(
        "financial_items.date",
        period["start"],
        period["end"],
    )
    now = period["now"]
    soon = now + timedelta(days=30)

    sold_group = {
        "$group": {
            "_id": None,
            "cars_sold": {"$sum": 1},
            "sales_value": {"$sum": "$sell_price"},
            "purchase_cost": {"$sum": "$buy_price"},
            "vehicle_expenses": {"$sum": "$vehicle_expenses"},
            "vehicle_revenue": {"$sum": "$vehicle_revenue"},
            "total_paid": {"$sum": "$total_paid"},
            "total_received": {"$sum": "$total_received"},
            "vehicle_profit": {"$sum": "$total_net"},
        }
    }
    bought_group = {"$group": {"_id": None, "cars_bought": {"$sum": 1}}}
    trend_group = {
        "$group": {
            "_id": _date_bucket("sell_date", period),
            "paid": {"$sum": "$total_paid"},
            "received": {"$sum": "$total_received"},
            "net": {"$sum": "$total_net"},
            "sold": {"$sum": 1},
        }
    }

    return {
        "current_sold": [{"$match": current_sold}, sold_group, {"$project": {"_id": 0}}],
        "current_bought": [{"$match": current_bought}, bought_group, {"$project": {"_id": 0}}],
        "previous_sold": [{"$match": previous_sold}, sold_group, {"$project": {"_id": 0}}]
        if period["previous_start"] is not None
        else [{"$match": {"$expr": {"$eq": [1, 0]}}}],
        "previous_bought": [{"$match": previous_bought}, bought_group, {"$project": {"_id": 0}}]
        if period["previous_start"] is not None
        else [{"$match": {"$expr": {"$eq": [1, 0]}}}],
        "financial_trend": [
            {"$match": current_sold},
            trend_group,
            {"$sort": {"_id": 1}},
            {"$project": {"_id": 0, "label": "$_id", "paid": 1, "received": 1, "net": 1, "sold": 1}},
        ],
        "bought_trend": [
            {"$match": current_bought},
            {"$group": {"_id": _date_bucket("buy_date", period), "bought": {"$sum": 1}}},
            {"$sort": {"_id": 1}},
            {"$project": {"_id": 0, "label": "$_id", "bought": 1}},
        ],
        "brand_performance": [
            {"$match": current_sold},
            {
                "$group": {
                    "_id": "$brand_name",
                    "cars": {"$sum": 1},
                    "sales": {"$sum": "$sell_price"},
                    "profit": {"$sum": "$total_net"},
                }
            },
            {"$sort": {"profit": -1}},
            {"$limit": 8},
            {"$project": {"_id": 0, "name": "$_id", "cars": 1, "sales": 1, "profit": 1}},
        ],
        "capital_by_status_summary": [
            {
                "$match": {
                    "invested_by": {"$nin": [None, ""]},
                    "capital_by_name": {"$ne": "Unassigned"},
                }
            },
            {"$unwind": "$financial_items"},
            {"$match": capital_item_match},
            {
                "$group": {
                    "_id": {
                        "id": "$invested_by",
                        "name": "$capital_by_name",
                        "status": "$status",
                    },
                    "paid": {"$sum": "$financial_items.pay_value"},
                    "received": {"$sum": "$financial_items.receive_value"},
                    "items": {"$sum": 1},
                    "cars": {"$addToSet": "$_id"},
                }
            },
            {
                "$set": {
                    "net": {"$subtract": ["$received", "$paid"]},
                    "car_count": {"$size": "$cars"},
                }
            },
            {"$sort": {"_id.name": 1}},
            {
                "$project": {
                    "_id": 0,
                    "id": {
                        "$convert": {
                            "input": "$_id.id",
                            "to": "string",
                            "onError": "",
                            "onNull": "",
                        }
                    },
                    "name": "$_id.name",
                    "status": "$_id.status",
                    "paid": 1,
                    "received": 1,
                    "net": 1,
                    "items": 1,
                    "car_count": 1,
                }
            },
        ],
        "bought_by_summary": _people_financial_summary(
            "bought_by",
            "bought_by_name",
            capital_item_match,
        ),
        "sold_by_summary": _people_financial_summary(
            "sold_by",
            "sold_by_name",
            capital_item_match,
        ),
        "inventory_summary": [
            {"$match": {"status": "New"}},
            {
                "$group": {
                    "_id": None,
                    "stock_count": {"$sum": 1},
                    "inventory_investment": {"$sum": "$inventory_investment"},
                    "inventory_purchase_cost": {"$sum": "$buy_price"},
                }
            },
            {"$project": {"_id": 0}},
        ],
        "inventory_aging": [
            {"$match": {"status": "New", "buy_date": {"$ne": None}}},
            {
                "$set": {
                    "age_days": {
                        "$floor": {"$divide": [{"$subtract": [now, "$buy_date"]}, 86400000]}
                    }
                }
            },
            {
                "$set": {
                    "bucket": {
                        "$switch": {
                            "branches": [
                                {"case": {"$lte": ["$age_days", 30]}, "then": "0–30 days"},
                                {"case": {"$lte": ["$age_days", 60]}, "then": "31–60 days"},
                                {"case": {"$lte": ["$age_days", 90]}, "then": "61–90 days"},
                            ],
                            "default": "90+ days",
                        }
                    }
                }
            },
            {
                "$group": {
                    "_id": "$bucket",
                    "count": {"$sum": 1},
                    "amount": {"$sum": "$inventory_investment"},
                }
            },
            {"$project": {"_id": 0, "label": "$_id", "count": 1, "amount": 1}},
        ],
        "top_vehicles": [
            {"$match": current_sold},
            {"$sort": {"total_net": -1}},
            {"$limit": 5},
            {
                "$project": {
                    "_id": 0,
                    "trade_id": {"$toString": "$_id"},
                    "brand": "$brand_name",
                    "model": "$model_name",
                    "trim": 1,
                    "buy_price": 1,
                    "sell_price": 1,
                    "expenses": "$vehicle_expenses",
                    "net": "$total_net",
                }
            },
        ],
        "loss_vehicles": [
            {"$match": {**current_sold, "total_net": {"$lt": 0}}},
            {"$sort": {"total_net": 1}},
            {"$limit": 5},
            {
                "$project": {
                    "_id": 0,
                    "trade_id": {"$toString": "$_id"},
                    "brand": "$brand_name",
                    "model": "$model_name",
                    "trim": 1,
                    "net": "$total_net",
                }
            },
        ],
        "stale_inventory": [
            {"$match": {"status": "New", "buy_date": {"$lt": now - timedelta(days=90)}}},
            {"$sort": {"buy_date": 1}},
            {"$limit": 6},
            {
                "$project": {
                    "_id": 0,
                    "trade_id": {"$toString": "$_id"},
                    "brand": "$brand_name",
                    "model": "$model_name",
                    "trim": 1,
                    "date": "$buy_date",
                    "amount": "$inventory_investment",
                    "age_days": {
                        "$floor": {"$divide": [{"$subtract": [now, "$buy_date"]}, 86400000]}
                    },
                }
            },
        ],
        "negative_margin": [
            {"$match": {"status": "Sold", "total_net": {"$lt": 0}}},
            {"$sort": {"total_net": 1}},
            {"$limit": 6},
            {
                "$project": {
                    "_id": 0,
                    "trade_id": {"$toString": "$_id"},
                    "brand": "$brand_name",
                    "model": "$model_name",
                    "trim": 1,
                    "amount": "$total_net",
                }
            },
        ],
        "data_quality": [
            {
                "$match": {
                    "$or": [
                        {"vin": ""},
                        {"buy_date": None},
                        {"$and": [{"status": "Sold"}, {"sell_date": None}]},
                    ]
                }
            },
            {"$limit": 6},
            {
                "$project": {
                    "_id": 0,
                    "trade_id": {"$toString": "$_id"},
                    "brand": "$brand_name",
                    "model": "$model_name",
                    "issue": {
                        "$switch": {
                            "branches": [
                                {"case": {"$eq": ["$vin", ""]}, "then": "Missing VIN"},
                                {"case": {"$eq": ["$buy_date", None]}, "then": "Missing BUY item"},
                            ],
                            "default": "Sold car is missing a SELL item",
                        }
                    },
                }
            },
        ],
        "expiry_alerts": [
            {
                "$match": {
                    "status": "New",
                    "$or": [
                        {"warranty_end_date": {"$gte": now, "$lt": soon}},
                        {"service_contract_end_date": {"$gte": now, "$lt": soon}},
                    ],
                }
            },
            {"$limit": 6},
            {
                "$project": {
                    "_id": 0,
                    "trade_id": {"$toString": "$_id"},
                    "brand": "$brand_name",
                    "model": "$model_name",
                    "warranty_end_date": 1,
                    "service_contract_end_date": 1,
                }
            },
        ],
    }


async def _vehicle_summary(company_id: ObjectId, filters: DashboardSummaryFilter, period: dict) -> dict:
    pipeline = _vehicle_base_pipeline(company_id, filters.status)
    pipeline.append({"$facet": _vehicle_facet(period)})
    cursor = await all_trades_collection.aggregate(pipeline, allowDiskUse=True)
    rows = await cursor.to_list(length=1)
    return rows[0] if rows else {}


def _capital_by_status_breakdown(rows: list[dict]) -> dict[str, list[dict]]:
    grouped: dict[str, dict[str, dict[str, Any]]] = {
        "all": {},
        "new": {},
        "sold": {},
    }

    def add_row(bucket: str, row: dict) -> None:
        capital_id = str(row.get("id") or "").strip()
        name = str(row.get("name") or "").strip()
        if not capital_id or not name:
            return
        current = grouped[bucket].setdefault(
            capital_id,
            {
                "id": capital_id,
                "name": name,
                "paid": 0.0,
                "received": 0.0,
                "net": 0.0,
                "items": 0,
                "car_count": 0,
            },
        )
        current["paid"] += _number(row.get("paid"))
        current["received"] += _number(row.get("received"))
        current["net"] = current["received"] - current["paid"]
        current["items"] += _integer(row.get("items"))
        current["car_count"] += _integer(row.get("car_count"))

    for row in rows:
        add_row("all", row)
        status = str(row.get("status") or "").strip().lower()
        if status in {"new", "sold"}:
            add_row(status, row)

    return {
        bucket: sorted(values.values(), key=lambda row: row["name"].lower())
        for bucket, values in grouped.items()
    }


async def _general_expenses_summary(company_id: ObjectId, period: dict) -> dict:
    current_match = _date_match("date", period["start"], period["end"])
    previous_match = _date_match("date", period["previous_start"], period["previous_end"])
    pipeline: list[dict] = [
        {"$match": {"company_id": company_id, "trade_id": None}},
        {
            "$lookup": {
                "from": "all_lists_values",
                "localField": "item",
                "foreignField": "_id",
                "pipeline": [{"$project": {"_id": 0, "name": 1}}],
                "as": "item_detail",
            }
        },
        {
            "$set": {
                "item_name": {"$ifNull": [{"$arrayElemAt": ["$item_detail.name", 0]}, "Uncategorized"]},
                "pay_value": {"$convert": {"input": "$pay", "to": "double", "onError": 0, "onNull": 0}},
                "receive_value": {
                    "$convert": {"input": "$receive", "to": "double", "onError": 0, "onNull": 0}
                },
            }
        },
        {
            "$facet": {
                "current": [
                    {"$match": current_match},
                    {
                        "$group": {
                            "_id": None,
                            "count": {"$sum": 1},
                            "paid": {"$sum": "$pay_value"},
                            "received": {"$sum": "$receive_value"},
                        }
                    },
                    {"$set": {"net": {"$subtract": ["$received", "$paid"]}}},
                    {"$project": {"_id": 0}},
                ],
                "previous": [
                    {"$match": previous_match},
                    {
                        "$group": {
                            "_id": None,
                            "count": {"$sum": 1},
                            "paid": {"$sum": "$pay_value"},
                            "received": {"$sum": "$receive_value"},
                        }
                    },
                    {"$set": {"net": {"$subtract": ["$received", "$paid"]}}},
                    {"$project": {"_id": 0}},
                ]
                if period["previous_start"] is not None
                else [{"$match": {"$expr": {"$eq": [1, 0]}}}],
                "breakdown": [
                    {"$match": current_match},
                    {
                        "$group": {
                            "_id": "$item_name",
                            "paid": {"$sum": "$pay_value"},
                            "received": {"$sum": "$receive_value"},
                            "count": {"$sum": 1},
                        }
                    },
                    {"$sort": {"paid": -1}},
                    {"$limit": 8},
                    {"$project": {"_id": 0, "name": "$_id", "paid": 1, "received": 1, "count": 1}},
                ],
                "trend": [
                    {"$match": current_match},
                    {
                        "$group": {
                            "_id": _date_bucket("date", period),
                            "paid": {"$sum": "$pay_value"},
                            "received": {"$sum": "$receive_value"},
                        }
                    },
                    {"$sort": {"_id": 1}},
                    {"$project": {"_id": 0, "label": "$_id", "paid": 1, "received": 1}},
                ],
            }
        },
    ]
    cursor = await all_trades_items_collection.aggregate(pipeline, allowDiskUse=True)
    rows = await cursor.to_list(length=1)
    return rows[0] if rows else {}


async def _money_collection_summary(collection: Any, company_id: ObjectId, period: dict, aging: bool) -> dict:
    current_match = _date_match("date", period["start"], period["end"])
    now = period["now"]
    facet: dict[str, list[dict]] = {
        "snapshot": [
            {
                "$group": {
                    "_id": None,
                    "count": {"$sum": 1},
                    "paid": {"$sum": {"$ifNull": ["$pay", 0]}},
                    "received": {"$sum": {"$ifNull": ["$receive", 0]}},
                }
            },
            {"$set": {"net": {"$subtract": ["$received", "$paid"]}}},
            {"$project": {"_id": 0}},
        ],
        "period": [
            {"$match": current_match},
            {
                "$group": {
                    "_id": None,
                    "count": {"$sum": 1},
                    "paid": {"$sum": {"$ifNull": ["$pay", 0]}},
                    "received": {"$sum": {"$ifNull": ["$receive", 0]}},
                }
            },
            {"$set": {"net": {"$subtract": ["$received", "$paid"]}}},
            {"$project": {"_id": 0}},
        ],
    }
    if aging:
        facet["aging"] = [
            {"$match": {"date": {"$ne": None}}},
            {
                "$set": {
                    "age_days": {"$floor": {"$divide": [{"$subtract": [now, "$date"]}, 86400000]}}
                }
            },
            {
                "$set": {
                    "bucket": {
                        "$switch": {
                            "branches": [
                                {"case": {"$lte": ["$age_days", 30]}, "then": "0–30 days"},
                                {"case": {"$lte": ["$age_days", 60]}, "then": "31–60 days"},
                                {"case": {"$lte": ["$age_days", 90]}, "then": "61–90 days"},
                            ],
                            "default": "90+ days",
                        }
                    }
                }
            },
            {
                "$group": {
                    "_id": "$bucket",
                    "count": {"$sum": 1},
                    "paid": {"$sum": {"$ifNull": ["$pay", 0]}},
                    "received": {"$sum": {"$ifNull": ["$receive", 0]}},
                }
            },
            {"$project": {"_id": 0, "label": "$_id", "count": 1, "paid": 1, "received": 1}},
        ]
        facet["old_items"] = [
            {"$match": {"date": {"$lt": now - timedelta(days=90)}}},
            {"$sort": {"date": 1}},
            {"$limit": 5},
            {
                "$lookup": {
                    "from": "all_lists_values",
                    "localField": "name",
                    "foreignField": "_id",
                    "pipeline": [{"$project": {"_id": 0, "name": 1}}],
                    "as": "name_detail",
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "id": {"$toString": "$_id"},
                    "name": {"$ifNull": [{"$arrayElemAt": ["$name_detail.name", 0]}, "Unknown"]},
                    "paid": {"$ifNull": ["$pay", 0]},
                    "received": {"$ifNull": ["$receive", 0]},
                    "date": 1,
                    "age_days": {"$floor": {"$divide": [{"$subtract": [now, "$date"]}, 86400000]}},
                }
            },
        ]

    cursor = await collection.aggregate([{"$match": {"company_id": company_id}}, {"$facet": facet}])
    rows = await cursor.to_list(length=1)
    return rows[0] if rows else {}


async def _recent_changes(data: dict, period: dict) -> dict:
    start = period["start"] or (period["now"] - timedelta(days=30))
    result = await get_last_changes(
        LastChangesFilter(from_date=start, to_date=period["now"]),
        data,
    )
    changes = result.get("last_changes", []) if isinstance(result, dict) else []
    return {"last_changes": changes[:8]}


async def _safe(coroutine: Any, fallback: dict) -> dict:
    try:
        result = await coroutine
        return result if isinstance(result, dict) else fallback
    except Exception:
        return fallback


def _combine_trends(vehicle: dict, expenses: dict) -> list[dict]:
    points: dict[str, dict[str, Any]] = {}
    for row in vehicle.get("financial_trend", []):
        label = str(row.get("label", ""))
        if not label:
            continue
        points[label] = {
            "label": label,
            "paid": _number(row.get("paid")),
            "received": _number(row.get("received")),
            "net": _number(row.get("net")),
            "sold": _integer(row.get("sold")),
            "bought": 0,
        }
    for row in vehicle.get("bought_trend", []):
        label = str(row.get("label", ""))
        if not label:
            continue
        point = points.setdefault(
            label,
            {"label": label, "paid": 0.0, "received": 0.0, "net": 0.0, "sold": 0, "bought": 0},
        )
        point["bought"] = _integer(row.get("bought"))
    for row in expenses.get("trend", []):
        label = str(row.get("label", ""))
        if not label:
            continue
        point = points.setdefault(
            label,
            {"label": label, "paid": 0.0, "received": 0.0, "net": 0.0, "sold": 0, "bought": 0},
        )
        paid = _number(row.get("paid"))
        received = _number(row.get("received"))
        point["paid"] += paid
        point["received"] += received
        point["net"] += received - paid
    return [points[key] for key in sorted(points)]


def _build_alerts(vehicle: dict, outstanding: dict, accounts: list[dict]) -> list[dict]:
    alerts: list[dict] = []
    for row in vehicle.get("negative_margin", []):
        alerts.append(
            {
                "severity": "high",
                "type": "vehicle",
                "title": "Vehicle sold at a loss",
                "detail": _vehicle_name(row),
                "amount": _number(row.get("amount")),
                "tab": "cars",
            }
        )
    for row in vehicle.get("stale_inventory", []):
        alerts.append(
            {
                "severity": "medium",
                "type": "inventory",
                "title": "Vehicle in stock for more than 90 days",
                "detail": f"{_vehicle_name(row)} · {_integer(row.get('age_days'))} days",
                "amount": _number(row.get("amount")),
                "tab": "cars",
            }
        )
    for row in vehicle.get("data_quality", []):
        alerts.append(
            {
                "severity": "low",
                "type": "data",
                "title": str(row.get("issue", "Vehicle information needs attention")),
                "detail": _vehicle_name(row),
                "amount": 0,
                "tab": "cars",
            }
        )
    for row in vehicle.get("expiry_alerts", []):
        alerts.append(
            {
                "severity": "medium",
                "type": "expiry",
                "title": "Warranty or service contract expires soon",
                "detail": _vehicle_name(row),
                "amount": 0,
                "tab": "cars",
            }
        )
    for row in outstanding.get("old_items", []):
        amount = _number(row.get("received")) - _number(row.get("paid"))
        alerts.append(
            {
                "severity": "high" if _integer(row.get("age_days")) > 180 else "medium",
                "type": "outstanding",
                "title": "Old outstanding entry",
                "detail": f"{row.get('name', 'Unknown')} · {_integer(row.get('age_days'))} days",
                "amount": amount,
                "tab": "outstanding",
            }
        )
    for account in accounts:
        balance = _number(account.get("final_net"))
        if balance < 0:
            alerts.append(
                {
                    "severity": "high",
                    "type": "account",
                    "title": "Negative account balance",
                    "detail": str(account.get("account_name") or account.get("account_display") or "Account"),
                    "amount": balance,
                    "tab": "bank_accounts",
                }
            )
    order = {"high": 0, "medium": 1, "low": 2}
    alerts.sort(key=lambda row: (order.get(row["severity"], 3), -abs(_number(row.get("amount")))))
    return alerts[:12]


def _vehicle_name(row: dict) -> str:
    return " ".join(
        part.strip()
        for part in [str(row.get("brand", "")), str(row.get("model", "")), str(row.get("trim", ""))]
        if part and part.strip()
    ) or "Unknown vehicle"


def _insight(current: dict, previous: dict, brand_rows: list[dict]) -> str:
    current_net = _number(current.get("operating_net"))
    previous_net = _number(previous.get("operating_net"))
    change = _percentage_change(current_net, previous_net)
    leader = brand_rows[0].get("name") if brand_rows else None
    if change is None:
        movement = "There is not enough previous-period data for a reliable comparison."
    elif change >= 0:
        movement = f"Operating net improved by {abs(change):.1f}% compared with the previous period."
    else:
        movement = f"Operating net decreased by {abs(change):.1f}% compared with the previous period."
    if leader:
        movement += f" {leader} is currently the strongest brand by realized net."
    return movement


@router.post("/get_dashboard_summary")
async def get_dashboard_summary(
    filters: DashboardSummaryFilter,
    data: dict = Depends(security.get_current_user),
):
    try:
        company_value = data.get("company_id")
        if not company_value or not ObjectId.is_valid(str(company_value)):
            raise HTTPException(status_code=400, detail="Invalid company id")
        company_id = ObjectId(str(company_value))
        period = _resolve_period(filters)

        vehicle, expenses, capital, outstanding, accounts_result, changes = await asyncio.gather(
            _safe(_vehicle_summary(company_id, filters, period), {}),
            _safe(_general_expenses_summary(company_id, period), {}),
            _safe(_money_collection_summary(all_capitals_collection, company_id, period, False), {}),
            _safe(_money_collection_summary(all_outstanding_collection, company_id, period, True), {}),
            _safe(get_cash_on_hand_or_bank_balance(data), {"totals": {}}),
            _safe(_recent_changes(data, period), {"last_changes": []}),
        )

        vehicle_current = _first(vehicle.get("current_sold", []))
        vehicle_current.update(_first(vehicle.get("current_bought", [])))
        vehicle_previous = _first(vehicle.get("previous_sold", []))
        vehicle_previous.update(_first(vehicle.get("previous_bought", [])))
        expense_current = _first(expenses.get("current", []))
        expense_previous = _first(expenses.get("previous", []))

        current = {
            "cars_bought": _integer(vehicle_current.get("cars_bought")),
            "cars_sold": _integer(vehicle_current.get("cars_sold")),
            "sales_value": _number(vehicle_current.get("sales_value")),
            "vehicle_profit": _number(vehicle_current.get("vehicle_profit")),
            "general_expenses": _number(expense_current.get("paid")),
            "general_income": _number(expense_current.get("received")),
        }
        current["operating_net"] = (
            current["vehicle_profit"]
            + current["general_income"]
            - current["general_expenses"]
        )
        current["gross_margin"] = (
            current["vehicle_profit"] / current["sales_value"] * 100
            if current["sales_value"]
            else 0
        )

        previous = {
            "cars_bought": _integer(vehicle_previous.get("cars_bought")),
            "cars_sold": _integer(vehicle_previous.get("cars_sold")),
            "sales_value": _number(vehicle_previous.get("sales_value")),
            "vehicle_profit": _number(vehicle_previous.get("vehicle_profit")),
            "general_expenses": _number(expense_previous.get("paid")),
            "general_income": _number(expense_previous.get("received")),
        }
        previous["operating_net"] = (
            previous["vehicle_profit"]
            + previous["general_income"]
            - previous["general_expenses"]
        )
        previous["gross_margin"] = (
            previous["vehicle_profit"] / previous["sales_value"] * 100
            if previous["sales_value"]
            else 0
        )

        deltas = {
            key: _percentage_change(_number(current.get(key)), _number(previous.get(key)))
            for key in current
        }

        account_totals = accounts_result.get("totals", {})
        accounts = account_totals.get("all_accounts", []) if isinstance(account_totals, dict) else []
        inventory = _first(vehicle.get("inventory_summary", []))
        capital_snapshot = _first(capital.get("snapshot", []))
        outstanding_snapshot = _first(outstanding.get("snapshot", []))
        position = {
            "cash_balance": _number(account_totals.get("total_final_net")),
            "stock_count": _integer(inventory.get("stock_count")),
            "inventory_investment": _number(inventory.get("inventory_investment")),
            "outstanding_receive": _number(outstanding_snapshot.get("received")),
            "outstanding_pay": _number(outstanding_snapshot.get("paid")),
            "outstanding_net": _number(outstanding_snapshot.get("net")),
            "capital_net": _number(capital_snapshot.get("net")),
        }

        brand_rows = vehicle.get("brand_performance", [])
        capital_by_status = _capital_by_status_breakdown(
            vehicle.get("capital_by_status_summary", [])
        )
        response_period = {
            "label": period["label"],
            "range": period["range"],
            "from": period["start"].isoformat() if period["start"] else None,
            "to": period["end"].isoformat() if period["end"] else None,
            "previous_from": period["previous_start"].isoformat()
            if period["previous_start"]
            else None,
            "previous_to": period["previous_end"].isoformat()
            if period["previous_end"]
            else None,
        }

        return {
            "generated_at": period["now"].isoformat(),
            "period": response_period,
            "performance": {"current": current, "previous": previous, "deltas": deltas},
            "position": position,
            "trends": _combine_trends(vehicle, expenses),
            "brand_performance": brand_rows,
            "capital_by_summary": capital_by_status["all"],
            "capital_by_status_summary": capital_by_status,
            "bought_by_summary": vehicle.get("bought_by_summary", []),
            "sold_by_summary": vehicle.get("sold_by_summary", []),
            "expense_breakdown": expenses.get("breakdown", []),
            "accounts": accounts,
            "inventory_aging": vehicle.get("inventory_aging", []),
            "outstanding_aging": outstanding.get("aging", []),
            "top_vehicles": vehicle.get("top_vehicles", []),
            "loss_vehicles": vehicle.get("loss_vehicles", []),
            "alerts": _build_alerts(vehicle, outstanding, accounts),
            "recent_changes": changes.get("last_changes", []),
            "insight": _insight(current, previous, brand_rows),
        }
    except HTTPException:
        raise
    except Exception as error:
        raise HTTPException(status_code=500, detail=str(error))

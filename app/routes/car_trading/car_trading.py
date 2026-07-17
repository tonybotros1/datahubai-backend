import copy
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException
from fastapi.encoders import jsonable_encoder
from pymongo.errors import PyMongoError

from app import database
from app.core import security
from app.routes.counters import create_custom_counter
from app.websocket_config import manager

from .common import (
    all_trades_collection,
    all_trades_items_collection,
    all_trades_purchase_agreement_items_collection,
    car_trade_search_serializer,
    ensure_car_trading_indexes,
    ensure_trade_belongs_to_company,
    exclusive_date_end,
    parse_object_id,
    require_payload_field,
    zero_if_none,
)
from .models import CarTradingItemsModel, CarTradingModel, CarTradingSearch

router = APIRouter()


trade_item_details_pipeline = [
    {
        '$lookup': {
            'from': 'all_lists_values',
            'let': {
                'itemID': '$item',
                'accountID': '$account_name'
            },
            'pipeline': [
                {
                    '$match': {
                        '$expr': {
                            '$in': [
                                '$_id', [
                                    '$$itemID', '$$accountID'
                                ]
                            ]
                        }
                    }
                }, {
                    '$project': {
                        'name': 1
                    }
                }
            ],
            'as': 'details'
        }
    }, {
        '$addFields': {
            'account_name_id': {
                '$toString': '$account_name'
            },
            'item_id': {
                '$toString': '$item'
            },
            'item': {
                '$let': {
                    'vars': {
                        'match': {
                            '$first': {
                                '$filter': {
                                    'input': '$details',
                                    'cond': {
                                        '$eq': [
                                            '$$this._id', '$item'
                                        ]
                                    }
                                }
                            }
                        }
                    },
                    'in': '$$match.name'
                }
            },
            'account_name': {
                '$let': {
                    'vars': {
                        'match': {
                            '$first': {
                                '$filter': {
                                    'input': '$details',
                                    'cond': {
                                        '$eq': [
                                            '$$this._id', '$account_name'
                                        ]
                                    }
                                }
                            }
                        }
                    },
                    'in': '$$match.name'
                }
            },
            '_id': {
                '$toString': '$_id'
            },
            'company_id': {
                '$toString': '$company_id'
            },
            'trade_id': {
                '$toString': '$trade_id'
            }
        }
    }, {
        '$project': {
            'details': 0
        }
    }
]


@router.get("/get_all_cars")
async def get_all_cars(data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        cars_pipeline = [
            {
                '$match': {
                    'company_id': company_id
                }
            }, {
                '$set': {
                    '_list_value_ids': {
                        '$filter': {
                            'input': [
                                '$color_in', '$color_out', '$specification', '$engine_size', '$year'
                            ],
                            'as': 'value_id',
                            'cond': {
                                '$ne': [
                                    '$$value_id', None
                                ]
                            }
                        }
                    }
                }
            }, {
                '$lookup': {
                    'from': 'all_brands',
                    'localField': 'car_brand',
                    'foreignField': '_id',
                    'pipeline': [
                        {
                            '$project': {
                                '_id': 1,
                                'name': 1
                            }
                        }
                    ],
                    'as': 'brand_details'
                }
            }, {
                '$lookup': {
                    'from': 'all_brand_models',
                    'localField': 'car_model',
                    'foreignField': '_id',
                    'pipeline': [
                        {
                            '$project': {
                                '_id': 1,
                                'name': 1
                            }
                        }
                    ],
                    'as': 'model_details'
                }
            }, {
                '$lookup': {
                    'from': 'all_lists_values',
                    'localField': '_list_value_ids',
                    'foreignField': '_id',
                    'pipeline': [
                        {
                            '$project': {
                                '_id': 1,
                                'name': 1
                            }
                        }
                    ],
                    'as': 'list_values_details'
                }
            }, {
                '$set': {
                    'brand_details': {
                        '$arrayElemAt': [
                            '$brand_details', 0
                        ]
                    },
                    'model_details': {
                        '$arrayElemAt': [
                            '$model_details', 0
                        ]
                    }
                }
            }, {
                '$set': {
                    'color_in_details': {
                        '$arrayElemAt': [
                            {
                                '$filter': {
                                    'input': '$list_values_details',
                                    'as': 'value',
                                    'cond': {
                                        '$eq': [
                                            '$$value._id', '$color_in'
                                        ]
                                    }
                                }
                            }, 0
                        ]
                    },
                    'color_out_details': {
                        '$arrayElemAt': [
                            {
                                '$filter': {
                                    'input': '$list_values_details',
                                    'as': 'value',
                                    'cond': {
                                        '$eq': [
                                            '$$value._id', '$color_out'
                                        ]
                                    }
                                }
                            }, 0
                        ]
                    },
                    'specification_details': {
                        '$arrayElemAt': [
                            {
                                '$filter': {
                                    'input': '$list_values_details',
                                    'as': 'value',
                                    'cond': {
                                        '$eq': [
                                            '$$value._id', '$specification'
                                        ]
                                    }
                                }
                            }, 0
                        ]
                    },
                    'engine_size_details': {
                        '$arrayElemAt': [
                            {
                                '$filter': {
                                    'input': '$list_values_details',
                                    'as': 'value',
                                    'cond': {
                                        '$eq': [
                                            '$$value._id', '$engine_size'
                                        ]
                                    }
                                }
                            }, 0
                        ]
                    },
                    'year_details': {
                        '$arrayElemAt': [
                            {
                                '$filter': {
                                    'input': '$list_values_details',
                                    'as': 'value',
                                    'cond': {
                                        '$eq': [
                                            '$$value._id', '$year'
                                        ]
                                    }
                                }
                            }, 0
                        ]
                    },
                    'mileage_text': {
                        '$convert': {
                            'input': '$mileage',
                            'to': 'string',
                            'onError': '',
                            'onNull': ''
                        }
                    }
                }
            }, {
                '$set': {
                    'brand': {
                        '$ifNull': [
                            '$brand_details.name', ''
                        ]
                    },
                    'model': {
                        '$ifNull': [
                            '$model_details.name', ''
                        ]
                    },
                    'trim_name': {
                        '$ifNull': [
                            '$trim', ''
                        ]
                    },
                    'year_name': {
                        '$ifNull': [
                            '$year_details.name', ''
                        ]
                    },
                    'interior_color': {
                        '$ifNull': [
                            '$color_in_details.name', ''
                        ]
                    },
                    'exterior_color': {
                        '$ifNull': [
                            '$color_out_details.name', ''
                        ]
                    },
                    'specification_name': {
                        '$ifNull': [
                            '$specification_details.name', ''
                        ]
                    },
                    'engine_size_name': {
                        '$ifNull': [
                            '$engine_size_details.name', ''
                        ]
                    }
                }
            }, {
                '$set': {
                    'brand_and_model': {
                        '$trim': {
                            'input': {
                                '$concat': [
                                    '$brand', {
                                        '$cond': [
                                            {
                                                '$and': [
                                                    {
                                                        '$ne': [
                                                            '$brand', ''
                                                        ]
                                                    }, {
                                                        '$ne': [
                                                            '$model', ''
                                                        ]
                                                    }
                                                ]
                                            }, ' ', ''
                                        ]
                                    }, '$model', {
                                        '$cond': [
                                            {
                                                '$and': [
                                                    {
                                                        '$or': [
                                                            {
                                                                '$ne': [
                                                                    '$brand', ''
                                                                ]
                                                            },
                                                            {
                                                                '$ne': [
                                                                    '$model', ''
                                                                ]
                                                            }
                                                        ]
                                                    }, {
                                                        '$ne': [
                                                            '$trim_name', ''
                                                        ]
                                                    }
                                                ]
                                            }, ' ', ''
                                        ]
                                    }, '$trim_name'
                                ]
                            }
                        }
                    }
                }
            }, {
                '$set': {
                    'car_without_mileage': {
                        '$trim': {
                            'input': {
                                '$concat': [
                                    '$brand_and_model', {
                                        '$cond': [
                                            {
                                                '$and': [
                                                    {
                                                        '$ne': [
                                                            '$brand_and_model', ''
                                                        ]
                                                    }, {
                                                        '$ne': [
                                                            '$year_name', ''
                                                        ]
                                                    }
                                                ]
                                            }, ' - ', ''
                                        ]
                                    }, '$year_name'
                                ]
                            }
                        }
                    }
                }
            }, {
                '$set': {
                    'car': {
                        '$trim': {
                            'input': {
                                '$concat': [
                                    '$car_without_mileage', {
                                        '$cond': [
                                            {
                                                '$and': [
                                                    {
                                                        '$ne': [
                                                            '$car_without_mileage', ''
                                                        ]
                                                    }, {
                                                        '$ne': [
                                                            '$mileage_text', ''
                                                        ]
                                                    }
                                                ]
                                            }, {
                                                '$concat': [
                                                    ' - ', '$mileage_text', ' km'
                                                ]
                                            }, {
                                                '$cond': [
                                                    {
                                                        '$ne': [
                                                            '$mileage_text', ''
                                                        ]
                                                    }, {
                                                        '$concat': [
                                                            '$mileage_text', ' km'
                                                        ]
                                                    }, ''
                                                ]
                                            }
                                        ]
                                    }
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
                    'car': 1,
                    'brand': 1,
                    'model': 1,
                    'trim': {
                        '$ifNull': [
                            '$trim', ''
                        ]
                    },
                    'year': {
                        '$ifNull': [
                            '$year_name', ''
                        ]
                    },
                    'interior_color': 1,
                    'exterior_color': 1,
                    'specification': {
                        '$ifNull': [
                            '$specification_name', ''
                        ]
                    },
                    'engine_size': {
                        '$ifNull': [
                            '$engine_size_name', ''
                        ]
                    },
                    'mileage': {
                        '$ifNull': [
                            '$mileage', None
                        ]
                    },
                    'chassis_number': {
                        '$ifNull': [
                            '$chassis_number', ''
                        ]
                    },
                    'plate_number': {
                        '$ifNull': [
                            '$plate_number', ''
                        ]
                    },
                    'status': {
                        '$ifNull': [
                            '$status', ''
                        ]
                    }
                }
            }
        ]

        cursor = await all_trades_collection.aggregate(cars_pipeline)
        results = await cursor.to_list(None)
        return {"cars": results}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/add_new_trade")
async def add_new_trade(trade: CarTradingModel, data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        if trade.date is None:
            raise HTTPException(status_code=400, detail="Transaction date is required")
        if not trade.car_brand:
            raise HTTPException(status_code=400, detail="Car brand is required")
        if not trade.car_model:
            raise HTTPException(status_code=400, detail="Car model is required")
        trade_dict = {
            "company_id": company_id if company_id else "",
            "date": trade.date,
            "warranty_end_date": trade.warranty_end_date,
            "service_contract_end_date": trade.service_contract_end_date,
            "mileage": trade.mileage,
            "color_in": trade.color_in if trade.color_in else "",
            "color_out": trade.color_out if trade.color_out else "",
            "car_brand": trade.car_brand if trade.car_brand else "",
            "car_model": trade.car_model if trade.car_model else "",
            "trim": trade.trim.strip() if trade.trim else "",
            "specification": trade.specification if trade.specification else "",
            "engine_size": trade.engine_size if trade.engine_size else "",
            "year": trade.year if trade.year else "",
            "vin": trade.vin if trade.vin else "",
            "status": "New",
            "bought_from": trade.bought_from if trade.bought_from else "",
            "sold_to": trade.sold_to if trade.sold_to else "",
            "note": trade.note,
            "bought_by": trade.bought_by if trade.bought_by else "",
            "sold_by": trade.sold_by if trade.sold_by else "",
            "invested_by": trade.invested_by if trade.invested_by else "",
            "consignment_for": trade.consignment_for if trade.consignment_for else "",
            "createdAt": security.now_utc(),
            "updatedAt": security.now_utc(),
        }

        result = await all_trades_collection.insert_one(trade_dict)
        if not result.inserted_id:
            raise HTTPException(status_code=500, detail="Failed to insert trade")

        return {"message": "Trade added successfully", "trade_id": str(result.inserted_id)}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")


async def get_trade_item_details(item_id: ObjectId, company_id: Optional[ObjectId] = None):
    item_details_pipeline: Any = copy.deepcopy(trade_item_details_pipeline)
    match_stage = {"_id": ObjectId(item_id)}
    if company_id is not None:
        match_stage["company_id"] = company_id
    item_details_pipeline.insert(0, {"$match": match_stage})
    cursor = await all_trades_items_collection.aggregate(item_details_pipeline)
    result = await cursor.to_list(length=1)
    return result[0] if result else None


@router.post("/add_trade_item")
async def add_trade_item(item_model: CarTradingItemsModel, data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        item_model = item_model.model_dump(exclude_unset=True)
        trade_id = parse_object_id(
            require_payload_field(item_model, "trade_id", "Trade id"),
            "trade_id",
        )
        await ensure_trade_belongs_to_company(trade_id, company_id)
        item_id = parse_object_id(
            require_payload_field(item_model, "item", "Item"),
            "item",
        )
        account_name_id = parse_object_id(
            require_payload_field(item_model, "account_name", "Account name"),
            "account_name",
        )
        require_payload_field(item_model, "date", "Date")
        item_model.update({
            "item": item_id,
            "trade_id": trade_id,
            "account_name": account_name_id,
            "company_id": company_id,
            "pay": zero_if_none(item_model.get("pay")),
            "receive": zero_if_none(item_model.get("receive")),
            "createdAt": security.now_utc(),
            "updatedAt": security.now_utc()
        })
        result = await all_trades_items_collection.insert_one(item_model)
        added_item = await get_trade_item_details(result.inserted_id, company_id)
        encoded_data = jsonable_encoder(added_item)

        await manager.send_to_company(str(company_id), {
            "type": "trade_item_added",
            "data": encoded_data
        })
        return {"message": "Trade item added successfully", "data": encoded_data}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")


@router.patch("/update_trade_item/{item_id}")
async def update_trade_item(item_id: str, item_model: CarTradingItemsModel,
                            data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        item_id = parse_object_id(item_id, "item_id")
        item_model = item_model.model_dump(exclude_unset=True)
        item_model.pop("trade_id", None)
        parsed_item_id = parse_object_id(
            require_payload_field(item_model, "item", "Item"),
            "item",
        )
        account_name_id = parse_object_id(
            require_payload_field(item_model, "account_name", "Account name"),
            "account_name",
        )
        require_payload_field(item_model, "date", "Date")
        item_model.update({
            "item": parsed_item_id,
            "account_name": account_name_id,
            "pay": zero_if_none(item_model.get("pay")),
            "receive": zero_if_none(item_model.get("receive")),
            "updatedAt": security.now_utc()
        })
        result = await all_trades_items_collection.update_one(
            {"_id": item_id, "company_id": company_id},
            {"$set": item_model},
        )
        if result.matched_count == 0:
            raise HTTPException(status_code=404, detail="Trade item not found")
        added_item = await get_trade_item_details(item_id, company_id)
        encoded_data = jsonable_encoder(added_item)

        await manager.send_to_company(str(company_id), {
            "type": "trade_item_updated",
            "data": encoded_data
        })
        return {"message": "Trade item updated successfully", "data": encoded_data}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")


@router.delete("/delete_trade_item/{item_id}")
async def delete_trade_item(item_id: str, data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        item_id = parse_object_id(item_id, "item_id")
        result = await all_trades_items_collection.delete_one(
            {"_id": item_id, "company_id": company_id},
        )
        if result.deleted_count == 0:
            raise HTTPException(status_code=404, detail="Trade item not found")
        await manager.send_to_company(str(company_id), {
            "type": "trade_item_deleted",
            "data": {"_id": str(item_id)}
        })
        return {"message": "Trade item deleted successfully"}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")


@router.patch("/update_trade/{trade_id}")
async def update_trade(trade_id: str, trade: CarTradingModel,
                       data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        trade_object_id = parse_object_id(trade_id, "trade_id")
        updated_trade = trade.model_dump(exclude_unset=True)
        if "date" in updated_trade and updated_trade["date"] is None:
            raise HTTPException(status_code=400, detail="Transaction date is required")
        if "car_brand" in updated_trade and updated_trade["car_brand"] in (None, ""):
            raise HTTPException(status_code=400, detail="Car brand is required")
        if "car_model" in updated_trade and updated_trade["car_model"] in (None, ""):
            raise HTTPException(status_code=400, detail="Car model is required")
        if "trim" in updated_trade:
            updated_trade["trim"] = updated_trade["trim"].strip() if updated_trade["trim"] else ""

        updated_trade["updatedAt"] = security.now_utc()
        result = await all_trades_collection.update_one(
            {"_id": trade_object_id, "company_id": company_id},
            {"$set": updated_trade},
        )
        if result.matched_count == 0:
            raise HTTPException(status_code=404, detail="Trade not found")

        return {"message": "Trade updated successfully", "trade_id": trade_id}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")


@router.post("/search_engine_for_car_trading")
async def search_engine_for_car_trading(
        filter_trades: CarTradingSearch,
        data: dict = Depends(security.get_current_user)
):
    try:
        await ensure_car_trading_indexes()
        company_id = ObjectId(data.get("company_id"))
        match_stage: Any = {"company_id": company_id}
        if filter_trades.car_brand:
            match_stage["car_brand"] = filter_trades.car_brand
        if filter_trades.car_model:
            match_stage["car_model"] = filter_trades.car_model
        if filter_trades.specification:
            match_stage["specification"] = filter_trades.specification
        if filter_trades.engine_size:
            match_stage["engine_size"] = filter_trades.engine_size
        if filter_trades.bought_from:
            match_stage["bought_from"] = filter_trades.bought_from
        if filter_trades.bought_by:
            match_stage["bought_by"] = filter_trades.bought_by
        if filter_trades.sold_by:
            match_stage["sold_by"] = filter_trades.sold_by
        if filter_trades.invested_by:
            match_stage["invested_by"] = filter_trades.invested_by
        if filter_trades.consignment_for:
            match_stage["consignment_for"] = filter_trades.consignment_for
        if filter_trades.sold_to:
            match_stage["sold_to"] = filter_trades.sold_to
        if filter_trades.status:
            match_stage["status"] = filter_trades.status

        now = security.now_utc()
        date_field = "date_field_to_filter"
        if filter_trades.status and filter_trades.status.lower() == "sold":
            date_field = "sell_date"
        elif filter_trades.status and filter_trades.status.lower() == "buy":
            date_field = "buy_date"

        date_filter = {}
        if filter_trades.today:
            start = datetime(now.year, now.month, now.day, tzinfo=timezone.utc)
            end = start + timedelta(days=1)
            date_filter[date_field] = {"$gte": start, "$lt": end}

        elif filter_trades.this_month:
            start = datetime(now.year, now.month, 1, tzinfo=timezone.utc)
            end = datetime(now.year + (now.month // 12), ((now.month % 12) + 1), 1, tzinfo=timezone.utc)
            date_filter[date_field] = {"$gte": start, "$lt": end}

        elif filter_trades.this_year:
            start = datetime(now.year, 1, 1, tzinfo=timezone.utc)
            end = datetime(now.year + 1, 1, 1, tzinfo=timezone.utc)
            date_filter[date_field] = {"$gte": start, "$lt": end}

        elif filter_trades.from_date or filter_trades.to_date:
            date_filter[date_field] = {}
            if filter_trades.from_date:
                date_filter[date_field]["$gte"] = filter_trades.from_date
            if filter_trades.to_date:
                date_filter[date_field]["$lt"] = exclusive_date_end(filter_trades.to_date)

        pipeline: list[dict] = [
            {"$match": match_stage},
            {
                "$lookup": {
                    "from": "all_trades_items",
                    "let": {"trade_id": "$_id", "company_id": "$company_id"},
                    "pipeline": [
                        {
                            "$match": {
                                "$expr": {
                                    "$and": [
                                        {"$eq": ["$trade_id", "$$trade_id"]},
                                        {"$eq": ["$company_id", "$$company_id"]},
                                    ]
                                }
                            }
                        },
                        {"$sort": {"date": 1}},
                        {
                            "$lookup": {
                                "from": "all_lists_values",
                                "let": {"ids": ["$item", "$account_name"]},
                                "pipeline": [
                                    {"$match": {"$expr": {"$in": ["$_id", "$$ids"]}}},
                                    {"$project": {"name": 1}},
                                ],
                                "as": "item_details",
                            }
                        },
                        {
                            "$project": {
                                "_id": 1,
                                "company_id": 1,
                                "trade_id": 1,
                                "date": 1,
                                "item_id": "$item",
                                "item": {
                                    "$ifNull": [
                                        {
                                            "$let": {
                                                "vars": {
                                                    "match": {
                                                        "$first": {
                                                            "$filter": {
                                                                "input": "$item_details",
                                                                "as": "detail",
                                                                "cond": {"$eq": ["$$detail._id", "$item"]},
                                                            }
                                                        }
                                                    }
                                                },
                                                "in": "$$match.name",
                                            }
                                        },
                                        "",
                                    ]
                                },
                                "account_name_id": "$account_name",
                                "account_name": {
                                    "$ifNull": [
                                        {
                                            "$let": {
                                                "vars": {
                                                    "match": {
                                                        "$first": {
                                                            "$filter": {
                                                                "input": "$item_details",
                                                                "as": "detail",
                                                                "cond": {"$eq": ["$$detail._id", "$account_name"]},
                                                            }
                                                        }
                                                    }
                                                },
                                                "in": "$$match.name",
                                            }
                                        },
                                        "",
                                    ]
                                },
                                "pay": {"$ifNull": ["$pay", 0]},
                                "receive": {"$ifNull": ["$receive", 0]},
                                "comment": {"$ifNull": ["$comment", ""]},
                                "createdAt": 1,
                                "updatedAt": 1,
                            }
                        },
                    ],
                    "as": "trade_items",
                }
            },
            {
                "$set": {
                    "buy_date": {
                        "$min": {
                            "$map": {
                                "input": {
                                    "$filter": {
                                        "input": "$trade_items",
                                        "as": "item",
                                        "cond": {"$eq": ["$$item.item", "BUY"]},
                                    }
                                },
                                "as": "item",
                                "in": "$$item.date",
                            }
                        }
                    },
                    "sell_date": {
                        "$min": {
                            "$map": {
                                "input": {
                                    "$filter": {
                                        "input": "$trade_items",
                                        "as": "item",
                                        "cond": {"$eq": ["$$item.item", "SELL"]},
                                    }
                                },
                                "as": "item",
                                "in": "$$item.date",
                            }
                        }
                    },
                    "total_pay": {"$sum": "$trade_items.pay"},
                    "total_receive": {"$sum": "$trade_items.receive"},
                }
            },
            {
                "$set": {
                    "date_field_to_filter": {
                        "$cond": [
                            {"$eq": ["$status", "Sold"]},
                            "$sell_date",
                            "$buy_date",
                        ]
                    }
                }
            },
        ]

        if date_filter:
            pipeline.append({"$match": date_filter})

        entity_lookups = [
            ("car_brand", "all_brands", "car_brand_detail"),
            ("car_model", "all_brand_models", "car_model_detail"),
        ]
        for local_field, collection, as_field in entity_lookups:
            pipeline.append({
                "$lookup": {
                    "from": collection,
                    "localField": local_field,
                    "foreignField": "_id",
                    "as": as_field,
                }
            })

        pipeline.append({
            "$lookup": {
                "from": "all_lists_values",
                "let": {
                    "ids": [
                        "$color_in",
                        "$color_out",
                        "$specification",
                        "$engine_size",
                        "$year",
                        "$bought_from",
                        "$sold_to",
                        "$sold_by",
                        "$bought_by",
                        "$invested_by",
                        "$consignment_for",
                    ]
                },
                "pipeline": [
                    {"$match": {"$expr": {"$in": ["$_id", "$$ids"]}}},
                    {"$project": {"name": 1}},
                ],
                "as": "list_details",
            }
        })

        def list_name(field: str) -> dict:
            return {
                "$ifNull": [
                    {
                        "$let": {
                            "vars": {
                                "match": {
                                    "$first": {
                                        "$filter": {
                                            "input": "$list_details",
                                            "as": "detail",
                                            "cond": {"$eq": ["$$detail._id", f"${field}"]},
                                        }
                                    }
                                }
                            },
                            "in": "$$match.name",
                        }
                    },
                    "",
                ]
            }

        pipeline.append({
            "$project": {
                "_id": 1,
                "car_brand_id": {"$ifNull": ["$car_brand", ""]},
                "car_brand": {"$ifNull": [{"$arrayElemAt": ["$car_brand_detail.name", 0]}, ""]},
                "car_model_id": {"$ifNull": ["$car_model", ""]},
                "car_model": {"$ifNull": [{"$arrayElemAt": ["$car_model_detail.name", 0]}, ""]},
                "trim": {"$ifNull": ["$trim", ""]},
                "year_id": {"$ifNull": ["$year", ""]},
                "year": list_name("year"),
                "status": {"$ifNull": ["$status", ""]},
                "color_in_id": {"$ifNull": ["$color_in", ""]},
                "color_in": list_name("color_in"),
                "color_out_id": {"$ifNull": ["$color_out", ""]},
                "color_out": list_name("color_out"),
                "specification_id": {"$ifNull": ["$specification", ""]},
                "specification": list_name("specification"),
                "engine_size_id": {"$ifNull": ["$engine_size", ""]},
                "engine_size": list_name("engine_size"),
                "mileage": {"$ifNull": ["$mileage", 0]},
                "vin": {"$ifNull": ["$vin", ""]},
                "bought_from_id": {"$ifNull": ["$bought_from", ""]},
                "bought_from": list_name("bought_from"),
                "sold_to_id": {"$ifNull": ["$sold_to", ""]},
                "sold_to": list_name("sold_to"),
                "sold_by": list_name("sold_by"),
                "bought_by": list_name("bought_by"),
                "sold_by_id": {"$ifNull": ["$sold_by", ""]},
                "bought_by_id": {"$ifNull": ["$bought_by", ""]},
                "invested_by": list_name("invested_by"),
                "invested_by_id": {"$ifNull": ["$invested_by", ""]},
                "consignment_for": list_name("consignment_for"),
                "consignment_for_id": {"$ifNull": ["$consignment_for", ""]},
                "note": {"$ifNull": ["$note", ""]},
                "date": {"$ifNull": ["$date", ""]},
                "warranty_end_date": {"$ifNull": ["$warranty_end_date", ""]},
                "service_contract_end_date": {"$ifNull": ["$service_contract_end_date", ""]},
                "trade_items": {"$ifNull": ["$trade_items", []]},
                "buy_date": {"$ifNull": ["$buy_date", ""]},
                "sell_date": {"$ifNull": ["$sell_date", ""]},
                "total_pay": {"$toDouble": {"$ifNull": ["$total_pay", 0]}},
                "total_receive": {"$toDouble": {"$ifNull": ["$total_receive", 0]}},
                "net": {
                    "$subtract": [
                        {"$toDouble": {"$ifNull": ["$total_receive", 0]}},
                        {"$toDouble": {"$ifNull": ["$total_pay", 0]}}
                    ]
                }

            }
        })

        sort_field = "sell_date" if (filter_trades.status and filter_trades.status.lower() == "sold") else "buy_date"
        pipeline.append({
            "$facet": {
                "trades": [{"$sort": {sort_field: -1}}],
                "totals": [
                    {
                        "$group": {
                            "_id": None,
                            "grand_total_pay": {"$sum": "$total_pay"},
                            "grand_total_receive": {"$sum": "$total_receive"},
                            "grand_net": {"$sum": "$net"}
                        }
                    }
                ]
            }
        })
        pipeline.append({
            "$project": {
                "trades": 1,
                "grand_total_pay": {
                    "$ifNull": [{"$arrayElemAt": ["$totals.grand_total_pay", 0]}, 0]
                },
                "grand_total_receive": {
                    "$ifNull": [{"$arrayElemAt": ["$totals.grand_total_receive", 0]}, 0]
                },
                "grand_net": {
                    "$ifNull": [{"$arrayElemAt": ["$totals.grand_net", 0]}, 0]
                }
            }
        })

        cursor = await all_trades_collection.aggregate(pipeline, allowDiskUse=True)
        results = await cursor.to_list(None)

        if results:
            return [car_trade_search_serializer(r) for r in results]
        return [{"trades": [], "grand_total_pay": 0, "grand_total_receive": 0, "grand_net": 0}]

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")


@router.delete("/delete_trade/{trade_id}")
async def delete_trade(trade_id: str, data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        trade_object_id = parse_object_id(trade_id, "trade_id")

        async with database.client.start_session() as session:
            try:
                await session.start_transaction()
                current_trade = await all_trades_collection.find_one(
                    {"_id": trade_object_id, "company_id": company_id},
                    {"_id": 1},
                    session=session,
                )
                if not current_trade:
                    raise HTTPException(status_code=404, detail="Trade not found")
                result1 = await all_trades_collection.delete_one(
                    {"_id": trade_object_id, "company_id": company_id},
                    session=session,
                )
                if result1.deleted_count == 0:
                    raise HTTPException(status_code=404, detail="Trade not found")
                await all_trades_items_collection.delete_many(
                    {"trade_id": trade_object_id, "company_id": company_id},
                    session=session,
                )
                await all_trades_purchase_agreement_items_collection.delete_many(
                    {"trade_id": trade_object_id, "company_id": company_id},
                    session=session,
                )
                await session.commit_transaction()
            except Exception:
                await session.abort_transaction()
                raise

        return {"message": "Trade and its items deleted successfully"}

    except HTTPException:
        raise
    except PyMongoError as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")

from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException

from app.core import security
from app.websocket_config import manager

from .common import (
    all_trades_collection,
    all_trades_items_collection,
    ensure_trade_belongs_to_company,
    exclusive_date_end,
    general_expenses_serialize,
    optional_object_id,
    parse_object_id,
    zero_if_none,
)
from .models import ExpensesSearchModel, GeneralExpensesModel

router = APIRouter()


@router.get("/get_all_general_expenses")
async def get_all_general_expenses(data: dict = Depends(security.get_current_user)):
    company_id = ObjectId(data.get("company_id"))

    pipeline = [
        {
            '$match': {
                'company_id': company_id
            }
        }, {
            '$sort': {
                'date': -1
            }
        }, {
            '$lookup': {
                'from': 'all_lists_values',
                'localField': 'item',
                'foreignField': '_id',
                'as': 'items'
            }
        }, {
            '$unwind': {
                'path': '$items',
                'preserveNullAndEmptyArrays': True
            }
        }, {
            '$lookup': {
                'from': 'all_lists_values',
                'localField': 'account_name',
                'foreignField': '_id',
                'as': 'account_name_details'
            }
        }, {
            '$unwind': {
                'path': '$account_name_details',
                'preserveNullAndEmptyArrays': True
            }
        }, {
            '$lookup': {
                'from': 'all_trades',
                'localField': 'trade_id',
                'foreignField': '_id',
                'as': 'trade_details'
            }
        }, {
            '$unwind': {
                'path': '$trade_details',
                'preserveNullAndEmptyArrays': True
            }
        }, {
            '$lookup': {
                'from': 'all_brands',
                'localField': 'trade_details.car_brand',
                'foreignField': '_id',
                'as': 'car_brand_details'
            }
        }, {
            '$unwind': {
                'path': '$car_brand_details',
                'preserveNullAndEmptyArrays': True
            }
        }, {
            '$lookup': {
                'from': 'all_brand_models',
                'localField': 'trade_details.car_model',
                'foreignField': '_id',
                'as': 'car_model_details'
            }
        }, {
            '$unwind': {
                'path': '$car_model_details',
                'preserveNullAndEmptyArrays': True
            }
        }, {
            '$addFields': {
                'mileage_text': {
                    '$convert': {
                        'input': '$trade_details.mileage',
                        'to': 'string',
                        'onError': '',
                        'onNull': ''
                    }
                }
            }
        }, {
            '$addFields': {
                'car': {
                    '$trim': {
                        'input': {
                            '$concat': [
                                {
                                    '$ifNull': [
                                        '$car_brand_details.name', ''
                                    ]
                                }, {
                                    '$cond': [
                                        {
                                            '$and': [
                                                {
                                                    '$ne': [
                                                        {
                                                            '$ifNull': [
                                                                '$car_brand_details.name', ''
                                                            ]
                                                        }, ''
                                                    ]
                                                }, {
                                                    '$ne': [
                                                        {
                                                            '$ifNull': [
                                                                '$car_model_details.name', ''
                                                            ]
                                                        }, ''
                                                    ]
                                                }
                                            ]
                                        }, ' ', ''
                                    ]
                                }, {
                                    '$ifNull': [
                                        '$car_model_details.name', ''
                                    ]
                                }, {
                                    '$cond': [
                                        {
                                            '$and': [
                                                {
                                                    '$or': [
                                                        {
                                                            '$ne': [
                                                                {
                                                                    '$ifNull': [
                                                                        '$car_brand_details.name', ''
                                                                    ]
                                                                }, ''
                                                            ]
                                                        },
                                                        {
                                                            '$ne': [
                                                                {
                                                                    '$ifNull': [
                                                                        '$car_model_details.name', ''
                                                                    ]
                                                                }, ''
                                                            ]
                                                        }
                                                    ]
                                                }, {
                                                    '$ne': [
                                                        {
                                                            '$ifNull': [
                                                                '$trade_details.trim', ''
                                                            ]
                                                        }, ''
                                                    ]
                                                }
                                            ]
                                        }, ' ', ''
                                    ]
                                }, {
                                    '$ifNull': [
                                        '$trade_details.trim', ''
                                    ]
                                }, {
                                    '$cond': [
                                        {
                                            '$ne': [
                                                '$mileage_text', ''
                                            ]
                                        }, {
                                            '$concat': [
                                                ' - ', '$mileage_text', ' km'
                                            ]
                                        }, ''
                                    ]
                                }
                            ]
                        }
                    }
                }
            }
        }, {
            '$project': {
                '_id': 1,
                'item': {
                    '$ifNull': [
                        '$items.name', ''
                    ]
                },
                'item_id': {
                    '$ifNull': [
                        '$items._id', ''
                    ]
                },
                'account_name': {
                    '$ifNull': [
                        '$account_name_details.name', ''
                    ]
                },
                'account_name_id': {
                    '$ifNull': [
                        '$account_name_details._id', ''
                    ]
                },
                'company_id': 1,
                'comment': 1,
                'date': 1,
                'pay': 1,
                'car': 1,
                'trim': {
                    '$ifNull': [
                        '$trade_details.trim', ''
                    ]
                },
                'trade_id': 1,
                'receive': 1,
                'createdAt': 1,
                'updatedAt': 1
            }
        }, {
            '$facet': {
                'general_expenses': [
                    {
                        '$match': {}
                    }
                ],
                'totals': [
                    {
                        '$group': {
                            '_id': None,
                            'total_pay': {
                                '$sum': {
                                    '$ifNull': [
                                        '$pay', 0
                                    ]
                                }
                            },
                            'total_receive': {
                                '$sum': {
                                    '$ifNull': [
                                        '$receive', 0
                                    ]
                                }
                            }
                        }
                    }, {
                        '$addFields': {
                            'total_net': {
                                '$subtract': [
                                    '$total_receive', '$total_pay'
                                ]
                            }
                        }
                    }
                ]
            }
        }
    ]

    cursor = await all_trades_items_collection.aggregate(pipeline)
    results = await cursor.to_list(length=None)
    if not results:
        return {"capitals": [], "totals": {"total_pay": 0, "total_receive": 0, "total_net": 0}}

    general_expenses = results[0].get("general_expenses", [])
    totals = results[0].get("totals", [])
    totals = totals[0] if totals else {"total_pay": 0, "total_receive": 0, "total_net": 0}
    return {
        "data": [general_expenses_serialize(g) for g in general_expenses],
        "totals": totals
    }


async def get_general_expenses_details(type_id: ObjectId, company_id: Optional[ObjectId] = None):
    try:
        match_stage = {"_id": type_id}
        if company_id is not None:
            match_stage["company_id"] = company_id
        pipeline = [
            {
                "$match": match_stage,
            },
            {
                '$lookup': {
                    'from': 'all_lists_values',
                    'localField': 'item',
                    'foreignField': '_id',
                    'as': 'items'
                }
            }, {
                '$unwind': {
                    'path': '$items',
                    'preserveNullAndEmptyArrays': True
                }
            }, {
                '$lookup': {
                    'from': 'all_lists_values',
                    'localField': 'account_name',
                    'foreignField': '_id',
                    'as': 'account_name_details'
                }
            }, {
                '$unwind': {
                    'path': '$account_name_details',
                    'preserveNullAndEmptyArrays': True
                }
            }, {
                '$lookup': {
                    'from': 'all_trades',
                    'localField': 'trade_id',
                    'foreignField': '_id',
                    'as': 'trade_details'
                }
            }, {
                '$unwind': {
                    'path': '$trade_details',
                    'preserveNullAndEmptyArrays': True
                }
            }, {
                '$lookup': {
                    'from': 'all_brands',
                    'localField': 'trade_details.car_brand',
                    'foreignField': '_id',
                    'as': 'car_brand_details'
                }
            }, {
                '$unwind': {
                    'path': '$car_brand_details',
                    'preserveNullAndEmptyArrays': True
                }
            }, {
                '$lookup': {
                    'from': 'all_brand_models',
                    'localField': 'trade_details.car_model',
                    'foreignField': '_id',
                    'as': 'car_model_details'
                }
            }, {
                '$unwind': {
                    'path': '$car_model_details',
                    'preserveNullAndEmptyArrays': True
                }
            }, {
                '$addFields': {
                    'mileage_text': {
                        '$convert': {
                            'input': '$trade_details.mileage',
                            'to': 'string',
                            'onError': '',
                            'onNull': ''
                        }
                    }
                }
            }, {
                '$addFields': {
                    'car': {
                        '$trim': {
                            'input': {
                                '$concat': [
                                    {
                                        '$ifNull': [
                                            '$car_brand_details.name', ''
                                        ]
                                    }, {
                                        '$cond': [
                                            {
                                                '$and': [
                                                    {
                                                        '$ne': [
                                                            {
                                                                '$ifNull': [
                                                                    '$car_brand_details.name', ''
                                                                ]
                                                            }, ''
                                                        ]
                                                    }, {
                                                        '$ne': [
                                                            {
                                                                '$ifNull': [
                                                                    '$car_model_details.name', ''
                                                                ]
                                                            }, ''
                                                        ]
                                                    }
                                                ]
                                            }, ' ', ''
                                        ]
                                    }, {
                                        '$ifNull': [
                                            '$car_model_details.name', ''
                                        ]
                                    }, {
                                        '$cond': [
                                            {
                                                '$and': [
                                                    {
                                                        '$or': [
                                                            {
                                                                '$ne': [
                                                                    {
                                                                        '$ifNull': [
                                                                            '$car_brand_details.name', ''
                                                                        ]
                                                                    }, ''
                                                                ]
                                                            },
                                                            {
                                                                '$ne': [
                                                                    {
                                                                        '$ifNull': [
                                                                            '$car_model_details.name', ''
                                                                        ]
                                                                    }, ''
                                                                ]
                                                            }
                                                        ]
                                                    }, {
                                                        '$ne': [
                                                            {
                                                                '$ifNull': [
                                                                    '$trade_details.trim', ''
                                                                ]
                                                            }, ''
                                                        ]
                                                    }
                                                ]
                                            }, ' ', ''
                                        ]
                                    }, {
                                        '$ifNull': [
                                            '$trade_details.trim', ''
                                        ]
                                    }, {
                                        '$cond': [
                                            {
                                                '$ne': [
                                                    '$mileage_text', ''
                                                ]
                                            }, {
                                                '$concat': [
                                                    ' - ', '$mileage_text', ' km'
                                                ]
                                            }, ''
                                        ]
                                    }
                                ]
                            }
                        }
                    }
                }
            }, {
                '$project': {
                    '_id': 1,
                    'item': {
                        '$ifNull': [
                            '$items.name', ''
                        ]
                    },
                    'item_id': {
                        '$ifNull': [
                            '$items._id', None
                        ]
                    },
                    'account_name': {
                        '$ifNull': [
                            '$account_name_details.name', ''
                        ]
                    },
                    'account_name_id': {
                        '$ifNull': [
                            '$account_name_details._id', None
                        ]
                    },
                    'car': 1,
                    'car_brand': {
                        '$ifNull': [
                            '$car_brand_details.name', ''
                        ]
                    },
                    'car_brand_id': {
                        '$ifNull': [
                            '$car_brand_details._id', None
                        ]
                    },
                    'car_model': {
                        '$ifNull': [
                            '$car_model_details.name', ''
                        ]
                    },
                    'car_model_id': {
                        '$ifNull': [
                            '$car_model_details._id', None
                        ]
                    },
                    'mileage': {
                        '$ifNull': [
                            '$trade_details.mileage', None
                        ]
                    },
                    'trim': {
                        '$ifNull': [
                            '$trade_details.trim', ''
                        ]
                    },
                    'company_id': 1,
                    'comment': 1,
                    'date': 1,
                    'pay': 1,
                    'receive': 1,
                    'trade_id': 1,
                    'createdAt': 1,
                    'updatedAt': 1
                }
            }
        ]
        cursor = await all_trades_items_collection.aggregate(pipeline)
        result = await cursor.to_list(length=1)
        if not result:
            raise HTTPException(status_code=404, detail="General expenses not found")
        return result[0]

    except HTTPException:
        raise
    except Exception as e:
        raise e


@router.post("/get_general_expenses_summary")
async def get_general_expenses_summary(filter_expenses: ExpensesSearchModel,
                                       data: dict = Depends(security.get_current_user)):
    company_id = ObjectId(data.get("company_id"))

    expenses_search_pipeline = []
    expenses_search_pipeline.insert(0, {'$match': {'company_id': company_id, 'trade_id': None}})

    now = security.now_utc()
    date_field = "date"
    date_filter = {}
    if filter_expenses.today:
        start = datetime(now.year, now.month, now.day, tzinfo=timezone.utc)
        end = start + timedelta(days=1)
        date_filter[date_field] = {"$gte": start, "$lt": end}

    elif filter_expenses.this_month:
        start = datetime(now.year, now.month, 1, tzinfo=timezone.utc)
        end = datetime(now.year + (now.month // 12), ((now.month % 12) + 1), 1, tzinfo=timezone.utc)
        date_filter[date_field] = {"$gte": start, "$lt": end}

    elif filter_expenses.this_year:
        start = datetime(now.year, 1, 1, tzinfo=timezone.utc)
        end = datetime(now.year + 1, 1, 1, tzinfo=timezone.utc)
        date_filter[date_field] = {"$gte": start, "$lt": end}

    elif filter_expenses.from_date or filter_expenses.to_date:
        date_filter[date_field] = {}
        if filter_expenses.from_date:
            date_filter[date_field]["$gte"] = filter_expenses.from_date
        if filter_expenses.to_date:
            date_filter[date_field]["$lt"] = exclusive_date_end(filter_expenses.to_date)

    if date_filter:
        expenses_search_pipeline.append({"$match": date_filter})

    expenses_search_pipeline.append(
        {
            "$group": {
                "_id": None,
                "total_pay": {"$sum": {"$ifNull": ["$pay", 0]}},
                "total_receive": {"$sum": {"$ifNull": ["$receive", 0]}},
                "count": {"$sum": 1}  # count all documents
            }
        },
    )
    expenses_search_pipeline.append(
        {
            "$addFields": {
                "total_net": {"$subtract": ["$total_receive", "$total_pay"]}
            }
        }
    )

    expenses_search_pipeline.append(
        {
            '$lookup': {
                'from': 'all_trades',
                'let': {
                    'companyId': company_id
                },
                'pipeline': [
                    {
                        '$match': {
                            '$expr': {
                                '$and': [
                                    {
                                        '$eq': [
                                            '$company_id', '$$companyId'
                                        ]
                                    }, {
                                        '$eq': [
                                            '$status', 'Sold'
                                        ]
                                    }
                                ]
                            }
                        }
                    }, {
                        '$lookup': {
                            'from': 'all_trades_items',
                            'let': {
                                'trade_id': '$_id'
                            },
                            'pipeline': [
                                {
                                    '$match': {
                                        '$expr': {
                                            '$eq': [
                                                '$trade_id', '$$trade_id'
                                            ]
                                        }
                                    }
                                }, {
                                    '$lookup': {
                                        'from': 'all_lists_values',
                                        'let': {
                                            'item_id': '$item'
                                        },
                                        'pipeline': [
                                            {
                                                '$match': {
                                                    '$expr': {
                                                        '$eq': [
                                                            '$_id', '$$item_id'
                                                        ]
                                                    }
                                                }
                                            }, {
                                                '$project': {
                                                    'name': 1
                                                }
                                            }
                                        ],
                                        'as': 'item_detail'
                                    }
                                }, {
                                    '$unwind': {
                                        'path': '$item_detail',
                                        'preserveNullAndEmptyArrays': True
                                    }
                                }, {
                                    '$addFields': {
                                        'buy_date_tmp': {
                                            '$cond': [
                                                {
                                                    '$eq': [
                                                        '$item_detail.name', 'BUY'
                                                    ]
                                                }, '$date', None
                                            ]
                                        },
                                        'sell_date_tmp': {
                                            '$cond': [
                                                {
                                                    '$eq': [
                                                        '$item_detail.name', 'SELL'
                                                    ]
                                                }, '$date', None
                                            ]
                                        }
                                    }
                                }
                            ],
                            'as': 'trade_items'
                        }
                    },
                    {
                        '$match': {
                            '$expr': {
                                '$or': [
                                    # If no date filter was created, allow all trades (True)
                                    {'$eq': [len(date_filter), 0]},
                                    # If date filter exists, check if any item in trade_items matches the range
                                    {
                                        '$gt': [
                                            {
                                                '$size': {
                                                    '$filter': {
                                                        'input': '$trade_items',
                                                        'as': 'item',
                                                        'cond': {
                                                            '$and': [
                                                                {'$ne': ['$$item.sell_date_tmp', None]},
                                                                {'$gte': ['$$item.sell_date_tmp',
                                                                          date_filter.get("date", {}).get("$gte",
                                                                                                          datetime(1, 1,
                                                                                                                   1,
                                                                                                                   tzinfo=timezone.utc))]},
                                                                {'$lt': ['$$item.sell_date_tmp',
                                                                         date_filter.get("date", {}).get("$lt",
                                                                                                         datetime(9999,
                                                                                                                  12,
                                                                                                                  31,
                                                                                                                  tzinfo=timezone.utc)) if "$lt" in date_filter.get(
                                                                             "date", {}) else datetime(9999, 12, 31,
                                                                                                       tzinfo=timezone.utc)]},
                                                                # Handle $lte if you used it in from_date/to_date
                                                                {'$lte': ['$$item.sell_date_tmp',
                                                                          date_filter.get("date", {}).get("$lte",
                                                                                                          datetime(9999,
                                                                                                                   12,
                                                                                                                   31,
                                                                                                                   tzinfo=timezone.utc))]}
                                                            ]
                                                        }
                                                    }
                                                }
                                            }, 0
                                        ]
                                    }
                                ]
                            }
                        }
                    },
                    {
                        '$unwind': '$trade_items'
                    }, {
                        '$group': {
                            '_id': None,
                            'total_trades_pay': {
                                '$sum': {
                                    '$ifNull': [
                                        '$trade_items.pay', 0
                                    ]
                                }
                            },
                            'total_trades_receive': {
                                '$sum': {
                                    '$ifNull': [
                                        '$trade_items.receive', 0
                                    ]
                                }
                            }
                        }
                    }, {
                        '$addFields': {
                            'total_trades_net': {
                                '$subtract': [
                                    '$total_trades_receive', '$total_trades_pay'
                                ]
                            }
                        }
                    }
                ],
                'as': 'trades'
            }
        }
    )

    expenses_search_pipeline.append({
        '$addFields': {
            'total_trades_net': {
                '$ifNull': [
                    {
                        '$arrayElemAt': [
                            '$trades.total_trades_net', 0
                        ]
                    }, 0
                ]
            }
        }
    })
    expenses_search_pipeline.append({
        '$addFields': {
            'net_profit': {
                '$add': [
                    '$total_trades_net', '$total_net'
                ]
            }
        }
    })

    expenses_search_pipeline.append({
        '$project': {
            'trades': 0,
            'total_trades_net': 0
        }
    })

    cursor = await all_trades_items_collection.aggregate(expenses_search_pipeline)
    result = await cursor.to_list(None)

    summary = result[0] if result else {
        "total_pay": 0,
        "total_receive": 0,
        "total_net": 0,
        "count": 0,
        "net_profit": 0
    }
    sell_item_conditions = [{'$eq': ['$$item.item_detail.name', 'SELL']}]
    date_range = date_filter.get("date", {})
    if "$gte" in date_range:
        sell_item_conditions.append({'$gte': ['$$item.date', date_range["$gte"]]})
    if "$lt" in date_range:
        sell_item_conditions.append({'$lt': ['$$item.date', date_range["$lt"]]})
    if "$lte" in date_range:
        sell_item_conditions.append({'$lte': ['$$item.date', date_range["$lte"]]})
    trade_net_pipeline: Any = [
        {'$match': {'company_id': company_id, 'status': 'Sold'}},
        {
            '$lookup': {
                'from': 'all_trades_items',
                'let': {'trade_id': '$_id'},
                'pipeline': [
                    {'$match': {'$expr': {'$eq': ['$trade_id', '$$trade_id']}}},
                    {
                        '$lookup': {
                            'from': 'all_lists_values',
                            'localField': 'item',
                            'foreignField': '_id',
                            'as': 'item_detail',
                        }
                    },
                    {'$unwind': {'path': '$item_detail', 'preserveNullAndEmptyArrays': True}},
                ],
                'as': 'trade_items',
            }
        },
    ]
    if date_filter:
        trade_net_pipeline.append({
            '$match': {
                '$expr': {
                    '$gt': [
                        {
                            '$size': {
                                '$filter': {
                                    'input': '$trade_items',
                                    'as': 'item',
                                    'cond': {'$and': sell_item_conditions},
                                }
                            }
                        },
                        0,
                    ]
                }
            }
        })
    trade_net_pipeline.extend([
        {'$unwind': '$trade_items'},
        {
            '$group': {
                '_id': None,
                'total_trades_pay': {'$sum': {'$ifNull': ['$trade_items.pay', 0]}},
                'total_trades_receive': {'$sum': {'$ifNull': ['$trade_items.receive', 0]}},
            }
        },
        {
            '$project': {
                '_id': 0,
                'total_trades_net': {'$subtract': ['$total_trades_receive', '$total_trades_pay']},
            }
        },
    ])
    trade_cursor = await all_trades_collection.aggregate(trade_net_pipeline)
    trade_result = await trade_cursor.to_list(length=1)
    trade_net = trade_result[0]["total_trades_net"] if trade_result else 0
    summary["net_profit"] = trade_net + summary.get("total_net", 0)

    return {"summary": summary}


@router.post("/add_new_general_expenses")
async def add_new_general_expenses(general: GeneralExpensesModel,
                                   data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        if general.date is None:
            raise HTTPException(status_code=400, detail="Date is required")
        item_id = parse_object_id(general.item, "item")
        account_name_id = parse_object_id(general.account_name, "account_name")
        trade_id = optional_object_id(general.trade_id, "trade_id")
        if trade_id is not None:
            await ensure_trade_belongs_to_company(trade_id, company_id)

        capital_dict = {
            "company_id": company_id,
            "item": item_id,
            "trade_id": trade_id,
            "pay": zero_if_none(general.pay),
            "receive": zero_if_none(general.receive),
            "account_name": account_name_id,
            "comment": general.comment,
            "date": general.date,
            "createdAt": security.now_utc(),
            "updatedAt": security.now_utc()
        }

        result = await all_trades_items_collection.insert_one(capital_dict)

        new_capital_or_outstanding = await get_general_expenses_details(result.inserted_id, company_id)
        serialized = general_expenses_serialize(new_capital_or_outstanding)
        await manager.send_to_company(str(company_id), {
            "type": "general_expenses_created",
            "data": serialized
        })
        return {"message": "General expenses created successfully", "data": serialized}

    except HTTPException:
        raise
    except Exception as error:
        raise HTTPException(status_code=500, detail=str(error))


@router.delete("/delete_general_expenses/{type_id}")
async def delete_general_expenses(type_id: str, data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        type_object_id = parse_object_id(type_id, "type_id")
        result = await all_trades_items_collection.find_one_and_delete(
            {"_id": type_object_id, "company_id": company_id},
        )
        if not result:
            raise HTTPException(status_code=404, detail="General expenses not found.")
        totals = {
            "pay": result.get("pay", 0),
            "receive": result.get("receive", 0),
        }
        await manager.send_to_company(str(company_id), {
            "type": "general_expenses_deleted",
            "data": {"_id": type_id},
        })
        return {
            "message": "General expenses deleted successfully!",
            "totals": totals
        }

    except HTTPException:
        raise
    except Exception as error:
        raise HTTPException(status_code=500, detail=str(error))


@router.patch("/update_generale_expenses/{type_id}")
async def update_generale_expenses(type_id: str, general: GeneralExpensesModel,
                                   data: dict = Depends(security.get_current_user)
                                   ):
    try:
        update_data = general.model_dump(exclude_unset=True)
        company_id = ObjectId(data.get("company_id"))
        type_object_id = parse_object_id(type_id, "type_id")

        if "item" in update_data:
            update_data["item"] = parse_object_id(update_data["item"], "item")
        if "account_name" in update_data:
            update_data["account_name"] = parse_object_id(update_data["account_name"], "account_name")
        if "trade_id" in update_data:
            update_data["trade_id"] = optional_object_id(update_data["trade_id"], "trade_id")
            if update_data["trade_id"] is not None:
                await ensure_trade_belongs_to_company(update_data["trade_id"], company_id)
        if "pay" in update_data:
            update_data["pay"] = zero_if_none(update_data["pay"])
        if "receive" in update_data:
            update_data["receive"] = zero_if_none(update_data["receive"])

        update_data["updatedAt"] = security.now_utc()

        result = await all_trades_items_collection.update_one(
            {"_id": type_object_id, "company_id": company_id},
            {"$set": update_data}
        )

        if result.matched_count == 0:
            raise HTTPException(status_code=404, detail="General expenses not found")
        updated_capital = await get_general_expenses_details(type_object_id, company_id)
        serialized = general_expenses_serialize(updated_capital)

        totals_pipeline = [
            {
                "$match": {"company_id": company_id},
            },
            {
                "$group": {
                    "_id": None,
                    "totalPay": {"$sum": "$pay"},
                    "totalReceive": {"$sum": "$receive"}
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "pay": "$totalPay",
                    "receive": "$totalReceive",
                    "net": {"$subtract": ["$totalReceive", "$totalPay"]}
                }
            }
        ]
        cursor = await all_trades_items_collection.aggregate(totals_pipeline)
        totals_result = await cursor.to_list(length=1)
        totals = totals_result[0] if totals_result else {"pay": 0, "receive": 0, "net": 0}

        await manager.send_to_company(str(company_id), {
            "type": "general_expenses_updated",
            "data": serialized,
            "totals": totals
        })
        return {
            "message": "General expenses updated successfully",
            "data": serialized,
            "totals": totals
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

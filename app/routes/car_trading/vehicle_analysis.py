from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException

from app.core import security

from .common import all_trades_collection

router = APIRouter()


@router.get("/get_vehicle_analysis_details")
async def get_vehicle_analysis_details(data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        vehicle_analysis_pipeline = [
            {
                '$match': {
                    'company_id': company_id,
                }
            },
            {
                '$project': {
                    '_id': 1,
                    'car_brand': 1,
                    'car_model': 1,
                    'status': {
                        '$ifNull': [
                            '$status', ''
                        ]
                    },
                    'trim': {
                        '$ifNull': [
                            '$trim', '$car_trim'
                        ]
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
                                'name': 1,
                                'model': 1
                            }
                        }
                    ],
                    'as': 'model_details'
                }
            }, {
                '$set': {
                    'brand_name': {
                        '$ifNull': [
                            {
                                '$arrayElemAt': [
                                    '$brand_details.name', 0
                                ]
                            }, 'Unknown'
                        ]
                    },
                    'model_name': {
                        '$ifNull': [
                            {
                                '$arrayElemAt': [
                                    '$model_details.name', 0
                                ]
                            }, {
                                '$ifNull': [
                                    {
                                        '$arrayElemAt': [
                                            '$model_details.model', 0
                                        ]
                                    }, 'Unknown'
                                ]
                            }
                        ]
                    }
                }
            }, {
                '$lookup': {
                    'from': 'all_trades_items',
                    'let': {
                        'current_trade_id': '$_id'
                    },
                    'pipeline': [
                        {
                            '$match': {
                                '$expr': {
                                    '$eq': [
                                        '$trade_id', '$$current_trade_id'
                                    ]
                                }
                            }
                        }, {
                            '$lookup': {
                                'from': 'all_lists_values',
                                'localField': 'item',
                                'foreignField': '_id',
                                'pipeline': [
                                    {
                                        '$project': {
                                            '_id': 0,
                                            'name': 1
                                        }
                                    }
                                ],
                                'as': 'items_details'
                            }
                        }, {
                            '$set': {
                                'item_name': {
                                    '$toUpper': {
                                        '$trim': {
                                            'input': {
                                                '$convert': {
                                                    'input': {
                                                        '$arrayElemAt': [
                                                            '$items_details.name', 0
                                                        ]
                                                    },
                                                    'to': 'string',
                                                    'onError': '',
                                                    'onNull': ''
                                                }
                                            }
                                        }
                                    }
                                },
                                'pay_amount': {
                                    '$convert': {
                                        'input': '$pay',
                                        'to': 'double',
                                        'onError': 0,
                                        'onNull': 0
                                    }
                                },
                                'receive_amount': {
                                    '$convert': {
                                        'input': '$receive',
                                        'to': 'double',
                                        'onError': 0,
                                        'onNull': 0
                                    }
                                }
                            }
                        }, {
                            '$group': {
                                '_id': None,
                                'buy_price': {
                                    '$sum': {
                                        '$cond': [
                                            {
                                                '$eq': [
                                                    '$item_name', 'BUY'
                                                ]
                                            }, '$pay_amount', 0
                                        ]
                                    }
                                },
                                'sell_price': {
                                    '$sum': {
                                        '$cond': [
                                            {
                                                '$eq': [
                                                    '$item_name', 'SELL'
                                                ]
                                            }, '$receive_amount', 0
                                        ]
                                    }
                                },
                                'total_paid': {
                                    '$sum': '$pay_amount'
                                },
                                'total_received': {
                                    '$sum': '$receive_amount'
                                },
                                'expenses': {
                                    '$sum': {
                                        '$cond': [
                                            {
                                                '$not': [
                                                    {
                                                        '$in': [
                                                            '$item_name', [
                                                                'BUY', 'SELL'
                                                            ]
                                                        ]
                                                    }
                                                ]
                                            }, '$pay_amount', 0
                                        ]
                                    }
                                },
                                'revenue': {
                                    '$sum': {
                                        '$cond': [
                                            {
                                                '$not': [
                                                    {
                                                        '$in': [
                                                            '$item_name', [
                                                                'BUY', 'SELL'
                                                            ]
                                                        ]
                                                    }
                                                ]
                                            }, '$receive_amount', 0
                                        ]
                                    }
                                }
                            }
                        }, {
                            '$project': {
                                '_id': 0,
                                'buy_price': 1,
                                'sell_price': 1,
                                'total_paid': 1,
                                'total_received': 1,
                                'expenses': 1,
                                'revenue': 1
                            }
                        }
                    ],
                    'as': 'financial_summary'
                }
            }, {
                '$set': {
                    'buy_price': {
                        '$ifNull': [
                            {
                                '$arrayElemAt': [
                                    '$financial_summary.buy_price', 0
                                ]
                            }, 0
                        ]
                    },
                    'sell_price': {
                        '$ifNull': [
                            {
                                '$arrayElemAt': [
                                    '$financial_summary.sell_price', 0
                                ]
                            }, 0
                        ]
                    },
                    'total_paid': {
                        '$ifNull': [
                            {
                                '$arrayElemAt': [
                                    '$financial_summary.total_paid', 0
                                ]
                            }, 0
                        ]
                    },
                    'total_received': {
                        '$ifNull': [
                            {
                                '$arrayElemAt': [
                                    '$financial_summary.total_received', 0
                                ]
                            }, 0
                        ]
                    },
                    'expenses': {
                        '$ifNull': [
                            {
                                '$arrayElemAt': [
                                    '$financial_summary.expenses', 0
                                ]
                            }, 0
                        ]
                    },
                    'revenue': {
                        '$ifNull': [
                            {
                                '$arrayElemAt': [
                                    '$financial_summary.revenue', 0
                                ]
                            }, 0
                        ]
                    }
                }
            }, {
                '$set': {
                    'buy_sell_net': {
                        '$subtract': [
                            '$sell_price', '$buy_price'
                        ]
                    },
                    'expenses_revenue_net': {
                        '$subtract': [
                            '$revenue', '$expenses'
                        ]
                    },
                    'total_net': {
                        '$subtract': [
                            '$total_received', '$total_paid'
                        ]
                    }
                }
            }, {
                '$project': {
                    'brand_details': 0,
                    'model_details': 0,
                    'financial_summary': 0
                }
            }, {
                '$sort': {
                    'buy_price': -1
                }
            }, {
                '$group': {
                    '_id': {
                        'brand_id': '$car_brand',
                        'brand_name': '$brand_name'
                    },
                    'car_count': {
                        '$sum': 1
                    },
                    'buy_price': {
                        '$sum': '$buy_price'
                    },
                    'sell_price': {
                        '$sum': '$sell_price'
                    },
                    'total_paid': {
                        '$sum': '$total_paid'
                    },
                    'total_received': {
                        '$sum': '$total_received'
                    },
                    'expenses': {
                        '$sum': '$expenses'
                    },
                    'revenue': {
                        '$sum': '$revenue'
                    },
                    'cars': {
                        '$push': {
                            'car_id': {
                                '$convert': {
                                    'input': '$_id',
                                    'to': 'string',
                                    'onError': None,
                                    'onNull': None
                                }
                            },
                            'brand_id': {
                                '$convert': {
                                    'input': '$car_brand',
                                    'to': 'string',
                                    'onError': None,
                                    'onNull': None
                                }
                            },
                            'brand_name': '$brand_name',
                            'model_id': {
                                '$convert': {
                                    'input': '$car_model',
                                    'to': 'string',
                                    'onError': None,
                                    'onNull': None
                                }
                            },
                            'model_name': '$model_name',
                            'trim': '$trim',
                            'status': '$status',
                            'buy_price': '$buy_price',
                            'sell_price': '$sell_price',
                            'buy_sell_net': '$buy_sell_net',
                            'expenses': '$expenses',
                            'revenue': '$revenue',
                            'expenses_revenue_net': '$expenses_revenue_net',
                            'total_paid': '$total_paid',
                            'total_received': '$total_received',
                            'total_net': '$total_net'
                        }
                    }
                }
            }, {
                '$project': {
                    '_id': 0,
                    'brand_id': {
                        '$convert': {
                            'input': '$_id.brand_id',
                            'to': 'string',
                            'onError': None,
                            'onNull': None
                        }
                    },
                    'brand_name': '$_id.brand_name',
                    'car_count': 1,
                    'cars': 1,
                    'buy_price': 1,
                    'sell_price': 1,
                    'buy_sell_net': {
                        '$subtract': [
                            '$sell_price', '$buy_price'
                        ]
                    },
                    'expenses': 1,
                    'revenue': 1,
                    'expenses_revenue_net': {
                        '$subtract': [
                            '$revenue', '$expenses'
                        ]
                    },
                    'total_paid': 1,
                    'total_received': 1,
                    'total_net': {
                        '$subtract': [
                            '$total_received', '$total_paid'
                        ]
                    }
                }
            }, {
                '$setWindowFields': {
                    'sortBy': {
                        'buy_price': -1
                    },
                    'output': {
                        'brand_rank': {
                            '$documentNumber': {}
                        }
                    }
                }
            }, {
                '$set': {
                    'result_brand_id': {
                        '$cond': [
                            {
                                '$lte': [
                                    '$brand_rank', 10
                                ]
                            }, '$brand_id', 'others'
                        ]
                    },
                    'result_brand_name': {
                        '$cond': [
                            {
                                '$lte': [
                                    '$brand_rank', 10
                                ]
                            }, '$brand_name', 'Others'
                        ]
                    },
                    'sort_order': {
                        '$cond': [
                            {
                                '$lte': [
                                    '$brand_rank', 10
                                ]
                            }, '$brand_rank', 11
                        ]
                    }
                }
            }, {
                '$group': {
                    '_id': {
                        'brand_id': '$result_brand_id',
                        'brand_name': '$result_brand_name'
                    },
                    'sort_order': {
                        '$min': '$sort_order'
                    },
                    'car_count': {
                        '$sum': '$car_count'
                    },
                    'buy_price': {
                        '$sum': '$buy_price'
                    },
                    'sell_price': {
                        '$sum': '$sell_price'
                    },
                    'total_paid': {
                        '$sum': '$total_paid'
                    },
                    'total_received': {
                        '$sum': '$total_received'
                    },
                    'expenses': {
                        '$sum': '$expenses'
                    },
                    'revenue': {
                        '$sum': '$revenue'
                    },
                    'cars_arrays': {
                        '$push': '$cars'
                    }
                }
            }, {
                '$project': {
                    '_id': 0,
                    'brand_id': '$_id.brand_id',
                    'brand_name': '$_id.brand_name',
                    'sort_order': 1,
                    'car_count': 1,
                    'buy_price': 1,
                    'sell_price': 1,
                    'buy_sell_net': {
                        '$subtract': [
                            '$sell_price', '$buy_price'
                        ]
                    },
                    'expenses': 1,
                    'revenue': 1,
                    'expenses_revenue_net': {
                        '$subtract': [
                            '$revenue', '$expenses'
                        ]
                    },
                    'total_paid': 1,
                    'total_received': 1,
                    'total_net': {
                        '$subtract': [
                            '$total_received', '$total_paid'
                        ]
                    },
                    'cars': {
                        '$reduce': {
                            'input': '$cars_arrays',
                            'initialValue': [],
                            'in': {
                                '$concatArrays': [
                                    '$$value', '$$this'
                                ]
                            }
                        }
                    }
                }
            }, {
                '$sort': {
                    'sort_order': 1
                }
            }, {
                '$project': {
                    'sort_order': 0
                }
            }
        ]
        cursor = await all_trades_collection.aggregate(vehicle_analysis_pipeline)
        results = await cursor.to_list(None)
        return {"vehicle_analysis": results}


    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

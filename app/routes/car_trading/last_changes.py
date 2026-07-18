from datetime import datetime, timezone

from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException

from app.core import security

from .common import all_trades_collection, exclusive_date_end
from .models import LastChangesFilter

router = APIRouter()


@router.post("/get_last_changes")
async def get_last_changes(data_filter: LastChangesFilter, data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))

        from_date = datetime(2000, 1, 1, tzinfo=timezone.utc)
        to_date = datetime(2100, 12, 31, tzinfo=timezone.utc)
        account = None
        if data_filter.account is not None:
            account = data_filter.account

        if data_filter.from_date:
            from_date = data_filter.from_date
        if data_filter.to_date:
            to_date = exclusive_date_end(data_filter.to_date)
        amount_filter = {}
        if data_filter.min_amount is not None:
            amount_filter['$gte'] = data_filter.min_amount
        if data_filter.max_amount is not None:
            amount_filter['$lte'] = data_filter.max_amount

        last_changes_pipeline = [
            {
                '$match': {
                    'company_id': company_id
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
                                        {
                                            '$convert': {
                                                'input': '$trade_id',
                                                'to': 'string',
                                                'onError': '',
                                                'onNull': ''
                                            }
                                        }, {
                                            '$convert': {
                                                'input': '$$current_trade_id',
                                                'to': 'string',
                                                'onError': '',
                                                'onNull': ''
                                            }
                                        }
                                    ]
                                }
                            }
                        }, {
                            '$lookup': {
                                'from': 'all_lists_values',
                                'let': {
                                    'current_item_id': '$item'
                                },
                                'pipeline': [
                                    {
                                        '$match': {
                                            '$expr': {
                                                '$eq': [
                                                    {
                                                        '$convert': {
                                                            'input': '$_id',
                                                            'to': 'string',
                                                            'onError': '',
                                                            'onNull': ''
                                                        }
                                                    }, {
                                                        '$convert': {
                                                            'input': '$$current_item_id',
                                                            'to': 'string',
                                                            'onError': '',
                                                            'onNull': ''
                                                        }
                                                    }
                                                ]
                                            }
                                        }
                                    }, {
                                        '$project': {
                                            '_id': 0,
                                            'name': 1
                                        }
                                    }
                                ],
                                'as': 'item_details'
                            }
                        }, {
                            '$lookup': {
                                'from': 'all_lists_values',
                                'let': {
                                    'current_account_id': '$account_name'
                                },
                                'pipeline': [
                                    {
                                        '$match': {
                                            '$expr': {
                                                '$eq': [
                                                    {
                                                        '$convert': {
                                                            'input': '$_id',
                                                            'to': 'string',
                                                            'onError': '',
                                                            'onNull': ''
                                                        }
                                                    }, {
                                                        '$convert': {
                                                            'input': '$$current_account_id',
                                                            'to': 'string',
                                                            'onError': '',
                                                            'onNull': ''
                                                        }
                                                    }
                                                ]
                                            }
                                        }
                                    }, {
                                        '$project': {
                                            '_id': 0,
                                            'name': 1
                                        }
                                    }
                                ],
                                'as': 'account_details'
                            }
                        }, {
                            '$project': {
                                '_id': 1,
                                'trade_id': 1,
                                'comment': 1,
                                'pay': 1,
                                'receive': 1,
                                'updatedAt': 1,
                                'item_name': {
                                    '$ifNull': [
                                        {
                                            '$arrayElemAt': [
                                                '$item_details.name', 0
                                            ]
                                        }, '-'
                                    ]
                                },
                                'account_name_value': {
                                    '$ifNull': [
                                        {
                                            '$arrayElemAt': [
                                                '$account_details.name', 0
                                            ]
                                        }, '-'
                                    ]
                                }
                            }
                        }
                    ],
                    'as': 'trade_items'
                }
            }, {
                '$unwind': {
                    'path': '$trade_items',
                    'preserveNullAndEmptyArrays': True
                }
            }, {
                '$addFields': {
                    'effective_updatedAt': {
                        '$ifNull': [
                            '$trade_items.updatedAt', '$updatedAt'
                        ]
                    }
                }
            }, {
                '$match': {
                    'effective_updatedAt': {
                        '$gte': from_date,
                        '$lt': to_date
                    }
                }
            }, {
                '$lookup': {
                    'from': 'all_brands',
                    'let': {
                        'current_brand_id': '$car_brand'
                    },
                    'pipeline': [
                        {
                            '$match': {
                                '$expr': {
                                    '$eq': [
                                        {
                                            '$convert': {
                                                'input': '$_id',
                                                'to': 'string',
                                                'onError': '',
                                                'onNull': ''
                                            }
                                        }, {
                                            '$convert': {
                                                'input': '$$current_brand_id',
                                                'to': 'string',
                                                'onError': '',
                                                'onNull': ''
                                            }
                                        }
                                    ]
                                }
                            }
                        }, {
                            '$project': {
                                '_id': 0,
                                'name': 1
                            }
                        }
                    ],
                    'as': 'brand_details'
                }
            }, {
                '$lookup': {
                    'from': 'all_brand_models',
                    'let': {
                        'current_model_id': '$car_model'
                    },
                    'pipeline': [
                        {
                            '$match': {
                                '$expr': {
                                    '$eq': [
                                        {
                                            '$convert': {
                                                'input': '$_id',
                                                'to': 'string',
                                                'onError': '',
                                                'onNull': ''
                                            }
                                        }, {
                                            '$convert': {
                                                'input': '$$current_model_id',
                                                'to': 'string',
                                                'onError': '',
                                                'onNull': ''
                                            }
                                        }
                                    ]
                                }
                            }
                        }, {
                            '$project': {
                                '_id': 0,
                                'name': 1
                            }
                        }
                    ],
                    'as': 'model_details'
                }
            }, {
                '$lookup': {
                    'from': 'all_lists_values',
                    'let': {
                        'current_year_id': '$year'
                    },
                    'pipeline': [
                        {
                            '$match': {
                                '$expr': {
                                    '$eq': [
                                        {
                                            '$convert': {
                                                'input': '$_id',
                                                'to': 'string',
                                                'onError': '',
                                                'onNull': ''
                                            }
                                        }, {
                                            '$convert': {
                                                'input': '$$current_year_id',
                                                'to': 'string',
                                                'onError': '',
                                                'onNull': ''
                                            }
                                        }
                                    ]
                                }
                            }
                        }, {
                            '$project': {
                                '_id': 0,
                                'name': 1
                            }
                        }
                    ],
                    'as': 'year_details'
                }
            }, {
                '$project': {
                    '_id': {
                        '$convert': {
                            'input': '$_id',
                            'to': 'string',
                            'onError': '',
                            'onNull': ''
                        }
                    },
                    'trade_item_id': {
                        '$convert': {
                            'input': '$trade_items._id',
                            'to': 'string',
                            'onError': None,
                            'onNull': None
                        }
                    },
                    'type': {
                        '$literal': 'car'
                    },
                    'brand_name': {
                        '$ifNull': [
                            {
                                '$arrayElemAt': [
                                    '$brand_details.name', 0
                                ]
                            }, '-'
                        ]
                    },
                    'model_name': {
                        '$ifNull': [
                            {
                                '$arrayElemAt': [
                                    '$model_details.name', 0
                                ]
                            }, '-'
                        ]
                    },
                    'year': {
                        '$ifNull': [
                            {
                                '$arrayElemAt': [
                                    '$year_details.name', 0
                                ]
                            }, '-'
                        ]
                    },
                    'description': {
                        '$ifNull': [
                            '$trade_items.comment', ''
                        ]
                    },
                    'pay': {
                        '$ifNull': [
                            '$trade_items.pay', 0
                        ]
                    },
                    'receive': {
                        '$ifNull': [
                            '$trade_items.receive', 0
                        ]
                    },
                    'updatedAt': '$effective_updatedAt',
                    'item_name': {
                        '$ifNull': [
                            '$trade_items.item_name', '-'
                        ]
                    },
                    'account_name': {
                        '$ifNull': [
                            '$trade_items.account_name_value', '-'
                        ]
                    }
                }
            }, {
                '$unionWith': {
                    'coll': 'all_trades_items',
                    'pipeline': [
                        {
                            '$match': {
                                'company_id': company_id,
                                'trade_id': None,
                                'updatedAt': {
                                    '$gte': from_date,
                                    '$lt': to_date
                                }
                            }
                        }, {
                            '$lookup': {
                                'from': 'all_lists_values',
                                'let': {
                                    'current_item_id': '$item'
                                },
                                'pipeline': [
                                    {
                                        '$match': {
                                            '$expr': {
                                                '$eq': [
                                                    {
                                                        '$convert': {
                                                            'input': '$_id',
                                                            'to': 'string',
                                                            'onError': '',
                                                            'onNull': ''
                                                        }
                                                    }, {
                                                        '$convert': {
                                                            'input': '$$current_item_id',
                                                            'to': 'string',
                                                            'onError': '',
                                                            'onNull': ''
                                                        }
                                                    }
                                                ]
                                            }
                                        }
                                    }, {
                                        '$project': {
                                            '_id': 0,
                                            'name': 1
                                        }
                                    }
                                ],
                                'as': 'item_details'
                            }
                        }, {
                            '$lookup': {
                                'from': 'all_lists_values',
                                'let': {
                                    'current_account_id': '$account_name'
                                },
                                'pipeline': [
                                    {
                                        '$match': {
                                            '$expr': {
                                                '$eq': [
                                                    {
                                                        '$convert': {
                                                            'input': '$_id',
                                                            'to': 'string',
                                                            'onError': '',
                                                            'onNull': ''
                                                        }
                                                    }, {
                                                        '$convert': {
                                                            'input': '$$current_account_id',
                                                            'to': 'string',
                                                            'onError': '',
                                                            'onNull': ''
                                                        }
                                                    }
                                                ]
                                            }
                                        }
                                    }, {
                                        '$project': {
                                            '_id': 0,
                                            'name': 1
                                        }
                                    }
                                ],
                                'as': 'account_details'
                            }
                        }, {
                            '$project': {
                                '_id': {
                                    '$convert': {
                                        'input': '$_id',
                                        'to': 'string',
                                        'onError': '',
                                        'onNull': ''
                                    }
                                },
                                'trade_item_id': {
                                    '$convert': {
                                        'input': '$_id',
                                        'to': 'string',
                                        'onError': None,
                                        'onNull': None
                                    }
                                },
                                'type': {
                                    '$literal': 'expenses'
                                },
                                'brand_name': {
                                    '$literal': '-'
                                },
                                'model_name': {
                                    '$literal': '-'
                                },
                                'year': {
                                    '$literal': '-'
                                },
                                'description': {
                                    '$ifNull': [
                                        '$comment', ''
                                    ]
                                },
                                'pay': {
                                    '$ifNull': [
                                        '$pay', 0
                                    ]
                                },
                                'receive': {
                                    '$ifNull': [
                                        '$receive', 0
                                    ]
                                },
                                'updatedAt': 1,
                                'item_name': {
                                    '$ifNull': [
                                        {
                                            '$arrayElemAt': [
                                                '$item_details.name', 0
                                            ]
                                        }, '-'
                                    ]
                                },
                                'account_name': {
                                    '$ifNull': [
                                        {
                                            '$arrayElemAt': [
                                                '$account_details.name', 0
                                            ]
                                        }, '-'
                                    ]
                                }
                            }
                        }
                    ]
                }
            }, {
                '$unionWith': {
                    'coll': 'all_outstanding',
                    'pipeline': [
                        {
                            '$match': {
                                'company_id': company_id,
                                'updatedAt': {
                                    '$gte': from_date,
                                    '$lt': to_date
                                }
                            }
                        }, {
                            '$lookup': {
                                'from': 'all_lists_values',
                                'let': {
                                    'current_item_id': '$name'
                                },
                                'pipeline': [
                                    {
                                        '$match': {
                                            '$expr': {
                                                '$eq': [
                                                    {
                                                        '$convert': {
                                                            'input': '$_id',
                                                            'to': 'string',
                                                            'onError': '',
                                                            'onNull': ''
                                                        }
                                                    }, {
                                                        '$convert': {
                                                            'input': '$$current_item_id',
                                                            'to': 'string',
                                                            'onError': '',
                                                            'onNull': ''
                                                        }
                                                    }
                                                ]
                                            }
                                        }
                                    }, {
                                        '$project': {
                                            '_id': 0,
                                            'name': 1
                                        }
                                    }
                                ],
                                'as': 'item_details'
                            }
                        }, {
                            '$lookup': {
                                'from': 'all_lists_values',
                                'let': {
                                    'current_account_id': '$account_name'
                                },
                                'pipeline': [
                                    {
                                        '$match': {
                                            '$expr': {
                                                '$eq': [
                                                    {
                                                        '$convert': {
                                                            'input': '$_id',
                                                            'to': 'string',
                                                            'onError': '',
                                                            'onNull': ''
                                                        }
                                                    }, {
                                                        '$convert': {
                                                            'input': '$$current_account_id',
                                                            'to': 'string',
                                                            'onError': '',
                                                            'onNull': ''
                                                        }
                                                    }
                                                ]
                                            }
                                        }
                                    }, {
                                        '$project': {
                                            '_id': 0,
                                            'name': 1
                                        }
                                    }
                                ],
                                'as': 'account_details'
                            }
                        }, {
                            '$project': {
                                '_id': {
                                    '$convert': {
                                        'input': '$_id',
                                        'to': 'string',
                                        'onError': '',
                                        'onNull': ''
                                    }
                                },
                                'trade_item_id': {
                                    '$literal': None
                                },
                                'type': {
                                    '$literal': 'outstanding'
                                },
                                'brand_name': {
                                    '$literal': '-'
                                },
                                'model_name': {
                                    '$literal': '-'
                                },
                                'year': {
                                    '$literal': '-'
                                },
                                'description': {
                                    '$ifNull': [
                                        '$comment', ''
                                    ]
                                },
                                'pay': {
                                    '$ifNull': [
                                        '$pay', 0
                                    ]
                                },
                                'receive': {
                                    '$ifNull': [
                                        '$receive', 0
                                    ]
                                },
                                'updatedAt': 1,
                                'item_name': {
                                    '$ifNull': [
                                        {
                                            '$arrayElemAt': [
                                                '$item_details.name', 0
                                            ]
                                        }, '-'
                                    ]
                                },
                                'account_name': {
                                    '$ifNull': [
                                        {
                                            '$arrayElemAt': [
                                                '$account_details.name', 0
                                            ]
                                        }, '-'
                                    ]
                                }
                            }
                        }
                    ]
                }
            }, {
                '$unionWith': {
                    'coll': 'all_capitals',
                    'pipeline': [
                        {
                            '$match': {
                                'company_id': company_id,
                                'updatedAt': {
                                    '$gte': from_date,
                                    '$lt': to_date
                                }
                            }
                        }, {
                            '$lookup': {
                                'from': 'all_lists_values',
                                'let': {
                                    'current_item_id': '$name'
                                },
                                'pipeline': [
                                    {
                                        '$match': {
                                            '$expr': {
                                                '$eq': [
                                                    {
                                                        '$convert': {
                                                            'input': '$_id',
                                                            'to': 'string',
                                                            'onError': '',
                                                            'onNull': ''
                                                        }
                                                    }, {
                                                        '$convert': {
                                                            'input': '$$current_item_id',
                                                            'to': 'string',
                                                            'onError': '',
                                                            'onNull': ''
                                                        }
                                                    }
                                                ]
                                            }
                                        }
                                    }, {
                                        '$project': {
                                            '_id': 0,
                                            'name': 1
                                        }
                                    }
                                ],
                                'as': 'item_details'
                            }
                        }, {
                            '$lookup': {
                                'from': 'all_lists_values',
                                'let': {
                                    'current_account_id': '$account_name'
                                },
                                'pipeline': [
                                    {
                                        '$match': {
                                            '$expr': {
                                                '$eq': [
                                                    {
                                                        '$convert': {
                                                            'input': '$_id',
                                                            'to': 'string',
                                                            'onError': '',
                                                            'onNull': ''
                                                        }
                                                    }, {
                                                        '$convert': {
                                                            'input': '$$current_account_id',
                                                            'to': 'string',
                                                            'onError': '',
                                                            'onNull': ''
                                                        }
                                                    }
                                                ]
                                            }
                                        }
                                    }, {
                                        '$project': {
                                            '_id': 0,
                                            'name': 1
                                        }
                                    }
                                ],
                                'as': 'account_details'
                            }
                        }, {
                            '$project': {
                                '_id': {
                                    '$convert': {
                                        'input': '$_id',
                                        'to': 'string',
                                        'onError': '',
                                        'onNull': ''
                                    }
                                },
                                'trade_item_id': {
                                    '$literal': None
                                },
                                'type': {
                                    '$literal': 'capital'
                                },
                                'brand_name': {
                                    '$literal': '-'
                                },
                                'model_name': {
                                    '$literal': '-'
                                },
                                'year': {
                                    '$literal': '-'
                                },
                                'description': {
                                    '$ifNull': [
                                        '$comment', ''
                                    ]
                                },
                                'pay': {
                                    '$ifNull': [
                                        '$pay', 0
                                    ]
                                },
                                'receive': {
                                    '$ifNull': [
                                        '$receive', 0
                                    ]
                                },
                                'updatedAt': 1,
                                'item_name': {
                                    '$ifNull': [
                                        {
                                            '$arrayElemAt': [
                                                '$item_details.name', 0
                                            ]
                                        }, '-'
                                    ]
                                },
                                'account_name': {
                                    '$ifNull': [
                                        {
                                            '$arrayElemAt': [
                                                '$account_details.name', 0
                                            ]
                                        }, '-'
                                    ]
                                }
                            }
                        }
                    ]
                }
            }, {
                '$unionWith': {
                    'coll': 'all_trades_transfers',
                    'pipeline': [
                        {
                            '$match': {
                                'company_id': company_id,
                                'updatedAt': {
                                    '$gte': from_date,
                                    '$lt': to_date
                                }
                            }
                        }, {
                            '$lookup': {
                                'from': 'all_lists_values',
                                'let': {
                                    'current_account_id': '$from_account'
                                },
                                'pipeline': [
                                    {
                                        '$match': {
                                            '$expr': {
                                                '$eq': [
                                                    {
                                                        '$convert': {
                                                            'input': '$_id',
                                                            'to': 'string',
                                                            'onError': '',
                                                            'onNull': ''
                                                        }
                                                    }, {
                                                        '$convert': {
                                                            'input': '$$current_account_id',
                                                            'to': 'string',
                                                            'onError': '',
                                                            'onNull': ''
                                                        }
                                                    }
                                                ]
                                            }
                                        }
                                    }, {
                                        '$project': {
                                            '_id': 0,
                                            'name': 1
                                        }
                                    }
                                ],
                                'as': 'from_details'
                            }
                        }, {
                            '$lookup': {
                                'from': 'all_lists_values',
                                'let': {
                                    'current_account_id': '$to_account'
                                },
                                'pipeline': [
                                    {
                                        '$match': {
                                            '$expr': {
                                                '$eq': [
                                                    {
                                                        '$convert': {
                                                            'input': '$_id',
                                                            'to': 'string',
                                                            'onError': '',
                                                            'onNull': ''
                                                        }
                                                    }, {
                                                        '$convert': {
                                                            'input': '$$current_account_id',
                                                            'to': 'string',
                                                            'onError': '',
                                                            'onNull': ''
                                                        }
                                                    }
                                                ]
                                            }
                                        }
                                    }, {
                                        '$project': {
                                            '_id': 0,
                                            'name': 1
                                        }
                                    }
                                ],
                                'as': 'to_details'
                            }
                        }, {
                            '$project': {
                                'entries': [
                                    {
                                        '_id': {
                                            '$concat': [
                                                {
                                                    '$convert': {
                                                        'input': '$_id',
                                                        'to': 'string',
                                                        'onError': '',
                                                        'onNull': ''
                                                    }
                                                }, '-from'
                                            ]
                                        },
                                        'trade_item_id': None,
                                        'type': 'transfer',
                                        'brand_name': '-',
                                        'model_name': '-',
                                        'year': '-',
                                        'description': {
                                            '$ifNull': [
                                                '$comment', ''
                                            ]
                                        },
                                        'pay': {
                                            '$ifNull': [
                                                '$amount', 0
                                            ]
                                        },
                                        'receive': 0,
                                        'updatedAt': '$updatedAt',
                                        'item_name': '-',
                                        'account_name': {
                                            '$ifNull': [
                                                {
                                                    '$arrayElemAt': [
                                                        '$from_details.name', 0
                                                    ]
                                                }, '-'
                                            ]
                                        }
                                    }, {
                                        '_id': {
                                            '$concat': [
                                                {
                                                    '$convert': {
                                                        'input': '$_id',
                                                        'to': 'string',
                                                        'onError': '',
                                                        'onNull': ''
                                                    }
                                                }, '-to'
                                            ]
                                        },
                                        'trade_item_id': None,
                                        'type': 'transfer',
                                        'brand_name': '-',
                                        'model_name': '-',
                                        'year': '-',
                                        'description': {
                                            '$ifNull': [
                                                '$comment', ''
                                            ]
                                        },
                                        'pay': 0,
                                        'receive': {
                                            '$ifNull': [
                                                '$amount', 0
                                            ]
                                        },
                                        'updatedAt': '$updatedAt',
                                        'item_name': '-',
                                        'account_name': {
                                            '$ifNull': [
                                                {
                                                    '$arrayElemAt': [
                                                        '$to_details.name', 0
                                                    ]
                                                }, '-'
                                            ]
                                        }
                                    }
                                ]
                            }
                        }, {
                            '$unwind': '$entries'
                        }, {
                            '$replaceRoot': {
                                'newRoot': '$entries'
                            }
                        }
                    ]
                }
            }, {
                '$sort': {
                    'updatedAt': -1
                }
            }
        ]
        match_map = {}
        if amount_filter:
            match_map['$or'] = [
                {'pay': amount_filter},
                {'receive': amount_filter}
            ]
        if account:
            match_map['account_name'] = account

        if match_map:
            last_changes_pipeline.append({"$match": match_map})

        cursor = await all_trades_collection.aggregate(last_changes_pipeline)
        results = await cursor.to_list(None)
        return {"last_changes": results}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

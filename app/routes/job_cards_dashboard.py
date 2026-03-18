from datetime import datetime
from typing import Optional

from bson import ObjectId
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel

from app.core import security
from app.database import get_collection

router = APIRouter()
job_cards_collection = get_collection("job_cards")
branches_collection = get_collection("branches")
salesman_collection = get_collection("sales_man")
receipts_collection = get_collection("all_receipts")
all_banks_collection = get_collection("all_banks")


class TimeFilter(BaseModel):
    from_date: Optional[datetime] = None
    to_date: Optional[datetime] = None


@router.post("/get_job_cards_daily_summary")
async def get_job_cards_daily_summary(time_filter: TimeFilter, data: dict = Depends(security.get_current_user)):
    try:
        from_date = time_filter.from_date
        to_date = time_filter.to_date
        company_id = ObjectId(data.get('company_id'))
        daily_summary_pipeline = [
            {
                '$match': {
                    'company_id': company_id
                }
            }, {
                '$lookup': {
                    'from': 'job_cards',
                    'let': {
                        'branch_id': '$_id'
                    },
                    'pipeline': [
                        {
                            '$match': {
                                '$expr': {
                                    '$eq': [
                                        '$branch', '$$branch_id'
                                    ]
                                }
                            }
                        }, {
                            '$addFields': {
                                'date_field_to_filter': {
                                    '$switch': {
                                        'branches': [
                                            {
                                                'case': {
                                                    '$eq': [
                                                        {
                                                            '$toLower': '$job_status_1'
                                                        }, 'new'
                                                    ]
                                                },
                                                'then': '$job_date'
                                            }, {
                                                'case': {
                                                    '$eq': [
                                                        {
                                                            '$toLower': '$job_status_1'
                                                        }, 'cancelled'
                                                    ]
                                                },
                                                'then': '$job_cancellation_date'
                                            }, {
                                                'case': {
                                                    '$eq': [
                                                        {
                                                            '$toLower': '$job_status_1'
                                                        }, 'posted'
                                                    ]
                                                },
                                                'then': '$invoice_date'
                                            }
                                        ],
                                        'default': '$job_date'
                                    }
                                }
                            }
                        }, {
                            '$match': {
                                '$expr': {
                                    '$and': [
                                        {
                                            '$gte': [
                                                '$date_field_to_filter',
                                                from_date
                                            ]
                                        }, {
                                            '$lt': [
                                                '$date_field_to_filter',
                                                to_date
                                            ]
                                        }
                                    ]
                                }
                            }
                        }, {
                            '$lookup': {
                                'from': 'job_cards_invoice_items',
                                'localField': '_id',
                                'foreignField': 'job_card_id',
                                'as': 'jobs_items'
                            }
                        }, {
                            '$lookup': {
                                'from': 'all_receipts_invoices',
                                'localField': '_id',
                                'foreignField': 'job_id',
                                'as': 'receipts_invoices_details'
                            }
                        }, {
                            '$addFields': {
                                'itemsTotal': {
                                    '$sum': {
                                        '$map': {
                                            'input': '$jobs_items',
                                            'as': 'it',
                                            'in': {
                                                '$cond': [
                                                    {
                                                        '$eq': [
                                                            '$job_status_1', 'Posted'
                                                        ]
                                                    }, {
                                                        '$ifNull': [
                                                            '$$it.total', 0
                                                        ]
                                                    }, 0
                                                ]
                                            }
                                        }
                                    }
                                },
                                'itemsPaid': {
                                    '$sum': {
                                        '$map': {
                                            'input': '$receipts_invoices_details',
                                            'as': 'it_paid',
                                            'in': {
                                                '$ifNull': [
                                                    '$$it_paid.amount', 0
                                                ]
                                            }
                                        }
                                    }
                                },
                                'itemsNet': {
                                    '$sum': {
                                        '$map': {
                                            'input': '$jobs_items',
                                            'as': 'it',
                                            'in': {
                                                '$cond': [
                                                    {
                                                        '$eq': [
                                                            '$job_status_1', 'Posted'
                                                        ]
                                                    }, {
                                                        '$add': [
                                                            {
                                                                '$ifNull': [
                                                                    '$$it.total', 0
                                                                ]
                                                            }, {
                                                                '$ifNull': [
                                                                    '$$it.vat', 0
                                                                ]
                                                            }
                                                        ]
                                                    }, 0
                                                ]
                                            }
                                        }
                                    }
                                }
                            }
                        }, {
                            '$group': {
                                '_id': None,
                                'totalPosted': {
                                    '$sum': {
                                        '$cond': [
                                            {
                                                '$eq': [
                                                    '$job_status_1', 'Posted'
                                                ]
                                            }, 1, 0
                                        ]
                                    }
                                },
                                'totalNew': {
                                    '$sum': {
                                        '$cond': [
                                            {
                                                '$eq': [
                                                    '$job_status_1', 'New'
                                                ]
                                            }, 1, 0
                                        ]
                                    }
                                },
                                'totalItemsAmount': {
                                    '$sum': '$itemsTotal'
                                },
                                'totalItemsNet': {
                                    '$sum': '$itemsNet'
                                },
                                'totalItemsPaid': {
                                    '$sum': '$itemsPaid'
                                }
                            }
                        }, {
                            '$addFields': {
                                'totalJobs': {
                                    '$add': [
                                        '$totalNew', '$totalPosted'
                                    ]
                                }
                            }
                        }
                    ],
                    'as': 'job_details'
                }
            }, {
                '$match': {
                    'job_details.0': {
                        '$exists': True
                    }
                }
            }, {
                '$project': {
                    'name': 1,
                    'job_details': 1
                }
            }, {
                '$unwind': '$job_details'
            }, {
                '$facet': {
                    'branches': [
                        {
                            '$project': {
                                '_id': 1,
                                'name': 1,
                                'job_details': [
                                    '$job_details'
                                ]
                            }
                        }
                    ],
                    'summary': [
                        {
                            '$group': {
                                '_id': None,
                                'totalPosted': {
                                    '$sum': '$job_details.totalPosted'
                                },
                                'totalNew': {
                                    '$sum': '$job_details.totalNew'
                                },
                                'totalItemsAmount': {
                                    '$sum': '$job_details.totalItemsAmount'
                                },
                                'totalJobs': {
                                    '$sum': '$job_details.totalJobs'
                                },
                                'totalItemsNet': {
                                    '$sum': '$job_details.totalItemsNet'
                                },
                                'totalItemsPaid': {
                                    '$sum': '$job_details.totalItemsPaid'
                                }
                            }
                        }, {
                            '$project': {
                                '_id': None,
                                'name': 'ALL BRANCHES',
                                'job_details': [
                                    {
                                        '_id': None,
                                        'totalPosted': '$totalPosted',
                                        'totalNew': '$totalNew',
                                        'totalItemsAmount': '$totalItemsAmount',
                                        'totalJobs': '$totalJobs',
                                        'totalItemsNet': '$totalItemsNet',
                                        'totalItemsPaid': '$totalItemsPaid',
                                    }
                                ]
                            }
                        }
                    ]
                }
            }, {
                '$project': {
                    'all': {
                        '$concatArrays': [
                            '$branches', '$summary'
                        ]
                    }
                }
            }, {
                '$unwind': '$all'
            }, {
                '$replaceRoot': {
                    'newRoot': '$all'
                }
            }, {
                '$addFields': {
                    '_id': {
                        '$toString': '$_id'
                    },
                    'total_posted': {
                        '$arrayElemAt': [
                            '$job_details.totalPosted', 0
                        ]
                    },
                    'total_new': {
                        '$arrayElemAt': [
                            '$job_details.totalNew', 0
                        ]
                    },
                    'total_items_amount': {
                        '$arrayElemAt': [
                            '$job_details.totalItemsAmount', 0
                        ]
                    },
                    'total_items_net': {
                        '$arrayElemAt': [
                            '$job_details.totalItemsNet', 0
                        ]
                    },
                    'total_items_paid': {
                        '$arrayElemAt': [
                            '$job_details.totalItemsPaid', 0
                        ]
                    },
                    'jobs_count': {
                        '$arrayElemAt': [
                            '$job_details.totalJobs', 0
                        ]
                    }
                }
            }, {
                '$project': {
                    'job_details': 0
                }
            }
        ]
        cursor = await branches_collection.aggregate(daily_summary_pipeline)
        results = await cursor.to_list(None)
        return {"daily_summary": results}


    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/get_new_job_cards_daily_summary")
async def get_new_job_cards_daily_summary(data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get('company_id'))
        daily_summary_pipeline = [
            {
                '$match': {
                    'company_id': company_id
                }
            }, {
                '$lookup': {
                    'from': 'branches',
                    'let': {
                        'branch_id': '$branch'
                    },
                    'pipeline': [
                        {
                            '$match': {
                                '$expr': {
                                    '$eq': [
                                        '$_id', '$$branch_id'
                                    ]
                                }
                            }
                        }, {
                            '$match': {
                                'company_id': company_id
                            }
                        }, {
                            '$project': {
                                '_id': 1,
                                'name': 1
                            }
                        }
                    ],
                    'as': 'branch'
                }
            }, {
                '$unwind': '$branch'
            }, {
                '$addFields': {
                    'date_field_to_filter': {
                        '$switch': {
                            'branches': [
                                {
                                    'case': {
                                        '$eq': [
                                            {
                                                '$toLower': '$job_status_1'
                                            }, 'new'
                                        ]
                                    },
                                    'then': '$job_date'
                                }, {
                                    'case': {
                                        '$eq': [
                                            {
                                                '$toLower': '$job_status_1'
                                            }, 'cancelled'
                                        ]
                                    },
                                    'then': '$job_cancellation_date'
                                }, {
                                    'case': {
                                        '$eq': [
                                            {
                                                '$toLower': '$job_status_1'
                                            }, 'posted'
                                        ]
                                    },
                                    'then': '$invoice_date'
                                }
                            ],
                            'default': '$job_date'
                        }
                    }
                }
            }, {
                '$group': {
                    '_id': '$branch._id',
                    'name': {
                        '$first': '$branch.name'
                    },
                    'totalNew': {
                        '$sum': {
                            '$cond': [
                                {
                                    '$eq': [
                                        '$job_status_1', 'New'
                                    ]
                                }, 1, 0
                            ]
                        }
                    },
                    'totalNotApproved': {
                        '$sum': {
                            '$cond': [
                                {
                                    '$eq': [
                                        '$job_status_2', 'New'
                                    ]
                                }, 1, 0
                            ]
                        }
                    },
                    'totalApproved': {
                        '$sum': {
                            '$cond': [
                                {
                                    '$eq': [
                                        '$job_status_2', 'Approved'
                                    ]
                                }, 1, 0
                            ]
                        }
                    },
                    'totalReady': {
                        '$sum': {
                            '$cond': [
                                {
                                    '$eq': [
                                        '$job_status_2', 'Ready'
                                    ]
                                }, 1, 0
                            ]
                        }
                    },
                    'totalReturned': {
                        '$sum': {
                            '$cond': [
                                {
                                    '$and': [
                                        {
                                            '$eq': [
                                                '$label', 'Returned'
                                            ]
                                        }, {
                                            '$eq': [
                                                '$job_status_1', 'New'
                                            ]
                                        }
                                    ]
                                }, 1, 0
                            ]
                        }
                    }
                }
            }, {
                '$facet': {
                    'branches': [
                        {
                            '$project': {
                                '_id': 1,
                                'name': 1,
                                'job_details': [
                                    {
                                        '_id': None,
                                        'totalNew': '$totalNew',
                                        'totalNotApproved': '$totalNotApproved',
                                        'totalApproved': '$totalApproved',
                                        'totalReady': '$totalReady',
                                        'totalReturned': '$totalReturned'
                                    }
                                ]
                            }
                        }
                    ],
                    'summary': [
                        {
                            '$group': {
                                '_id': None,
                                'totalNew': {
                                    '$sum': '$totalNew'
                                },
                                'totalNotApproved': {
                                    '$sum': '$totalNotApproved'
                                },
                                'totalApproved': {
                                    '$sum': '$totalApproved'
                                },
                                'totalReady': {
                                    '$sum': '$totalReady'
                                },
                                'totalReturned': {
                                    '$sum': '$totalReturned'
                                }
                            }
                        }, {
                            '$project': {
                                '_id': None,
                                'name': 'ALL BRANCHES',
                                'job_details': [
                                    {
                                        '_id': None,
                                        'totalNew': '$totalNew',
                                        'totalNotApproved': '$totalNotApproved',
                                        'totalApproved': '$totalApproved',
                                        'totalReady': '$totalReady',
                                        'totalReturned': '$totalReturned'
                                    }
                                ]
                            }
                        }
                    ]
                }
            }, {
                '$project': {
                    'all': {
                        '$concatArrays': [
                            '$branches', '$summary'
                        ]
                    }
                }
            }, {
                '$unwind': '$all'
            }, {
                '$replaceRoot': {
                    'newRoot': '$all'
                }
            }, {
                '$addFields': {
                    '_id': {
                        '$toString': '$_id'
                    },
                    'total_new': {
                        '$arrayElemAt': [
                            '$job_details.totalNew', 0
                        ]
                    },
                    'total_not_approved': {
                        '$arrayElemAt': [
                            '$job_details.totalNotApproved', 0
                        ]
                    },
                    'total_approved': {
                        '$arrayElemAt': [
                            '$job_details.totalApproved', 0
                        ]
                    },
                    'total_ready': {
                        '$arrayElemAt': [
                            '$job_details.totalReady', 0
                        ]
                    },
                    'total_returned': {
                        '$arrayElemAt': [
                            '$job_details.totalReturned', 0
                        ]
                    }
                }
            }, {
                '$match': {
                    '$or': [
                        {
                            'total_new': {
                                '$gt': 0
                            }
                        }, {
                            'total_not_approved': {
                                '$gt': 0
                            }
                        }, {
                            'total_approved': {
                                '$gt': 0
                            }
                        }, {
                            'total_ready': {
                                '$gt': 0
                            }
                        }, {
                            'total_returned': {
                                '$gt': 0
                            }
                        }
                    ]
                }
            }, {
                '$project': {
                    'job_details': 0
                }
            }
        ]
        cursor = await job_cards_collection.aggregate(daily_summary_pipeline)
        results = await cursor.to_list(None)
        return {"new_daily_summary": results}


    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/get_jobs_dates/{date_type}")
async def get_jobs_dates(date_type: str, data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get('company_id'))
        date_format = '%d-%m-%Y' if date_type.lower() == 'day' else '%m-%Y' if date_type.lower() == 'month' else None
        jobs_dates_pipeline = [
            {
                '$match': {
                    'company_id': company_id
                }
            }, {
                '$group': {
                    '_id': {
                        '$dateTrunc': {
                            'date': {
                                '$cond': [
                                    {
                                        '$eq': ["$job_status_1", "Posted"]
                                    },
                                    "$invoice_date",
                                    "$job_date"
                                ]
                            },
                            'unit': date_type.lower()
                        }
                    }
                }
            }, {
                '$project': {
                    'dateObj': '$_id',
                    'date': {
                        '$dateToString': {
                            'format': date_format,
                            'date': '$_id'
                        }
                    }
                }
            }, {
                '$sort': {
                    'dateObj': -1
                }
            }, {
                '$setWindowFields': {
                    'sortBy': {
                        'dateObj': -1
                    },
                    'output': {
                        'idx': {
                            '$documentNumber': {}
                        }
                    }
                }
            }, {
                '$project': {
                    'k': {
                        '$toString': '$idx'
                    },
                    'v': {
                        'date': '$date'
                    }
                }
            }, {
                '$group': {
                    '_id': None,
                    'items': {
                        '$push': {
                            'k': '$k',
                            'v': '$v'
                        }
                    }
                }
            }, {
                '$replaceRoot': {
                    'newRoot': {
                        '$arrayToObject': '$items'
                    }
                }
            }
        ]
        cursor = await job_cards_collection.aggregate(jobs_dates_pipeline)
        results = await cursor.next()
        return {"dates": results}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/get_salesman_summary")
async def get_salesman_summary(time_filter: TimeFilter, data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get('company_id'))
        from_date = time_filter.from_date
        to_date = time_filter.to_date
        salesman_summary_pipeline = [
            {
                '$match': {
                    'company_id': company_id
                }
            }, {
                '$lookup': {
                    'from': 'job_cards',
                    'let': {
                        'salesman': '$_id'
                    },
                    'pipeline': [
                        {
                            '$match': {
                                '$expr': {
                                    '$eq': [
                                        '$salesman', '$$salesman'
                                    ]
                                }
                            }
                        }, {
                            '$addFields': {
                                'date_field_to_filter': {
                                    '$switch': {
                                        'branches': [
                                            {
                                                'case': {
                                                    '$eq': [
                                                        {
                                                            '$toLower': '$job_status_1'
                                                        }, 'new'
                                                    ]
                                                },
                                                'then': '$job_date'
                                            }, {
                                                'case': {
                                                    '$eq': [
                                                        {
                                                            '$toLower': '$job_status_1'
                                                        }, 'cancelled'
                                                    ]
                                                },
                                                'then': '$job_cancellation_date'
                                            }, {
                                                'case': {
                                                    '$eq': [
                                                        {
                                                            '$toLower': '$job_status_1'
                                                        }, 'posted'
                                                    ]
                                                },
                                                'then': '$invoice_date'
                                            }
                                        ],
                                        'default': '$job_date'
                                    }
                                }
                            }
                        }, {
                            '$match': {
                                '$expr': {
                                    '$and': [
                                        {
                                            '$gte': [
                                                '$date_field_to_filter',
                                                from_date
                                            ]
                                        }, {
                                            '$lt': [
                                                '$date_field_to_filter',
                                                to_date
                                            ]
                                        }
                                    ]
                                }
                            }
                        }, {
                            '$lookup': {
                                'from': 'job_cards_invoice_items',
                                'localField': '_id',
                                'foreignField': 'job_card_id',
                                'as': 'jobs_items'
                            }
                        }, {
                            '$lookup': {
                                'from': 'all_receipts_invoices',
                                'localField': '_id',
                                'foreignField': 'job_id',
                                'as': 'receipts_invoices_details'
                            }
                        }, {
                            '$addFields': {
                                'itemsTotal': {
                                    '$sum': {
                                        '$map': {
                                            'input': '$jobs_items',
                                            'as': 'it',
                                            'in': {
                                                '$cond': [
                                                    {
                                                        '$eq': [
                                                            '$job_status_1', 'Posted'
                                                        ]
                                                    }, {
                                                        '$ifNull': [
                                                            '$$it.total', 0
                                                        ]
                                                    }, 0
                                                ]
                                            }
                                        }
                                    }
                                },
                                'itemsPaid': {
                                    '$sum': {
                                        '$map': {
                                            'input': '$receipts_invoices_details',
                                            'as': 'it_paid',
                                            'in': {
                                                '$ifNull': [
                                                    '$$it_paid.amount', 0
                                                ]
                                            }
                                        }
                                    }
                                },
                                'itemsNet': {
                                    '$sum': {
                                        '$map': {
                                            'input': '$jobs_items',
                                            'as': 'it',
                                            'in': {
                                                '$cond': [
                                                    {
                                                        '$eq': [
                                                            '$job_status_1', 'Posted'
                                                        ]
                                                    }, {
                                                        '$add': [
                                                            {
                                                                '$ifNull': [
                                                                    '$$it.total', 0
                                                                ]
                                                            }, {
                                                                '$ifNull': [
                                                                    '$$it.vat', 0
                                                                ]
                                                            }
                                                        ]
                                                    }, 0
                                                ]
                                            }
                                        }
                                    }
                                }
                            }
                        }, {
                            '$group': {
                                '_id': None,
                                'totalPosted': {
                                    '$sum': {
                                        '$cond': [
                                            {
                                                '$eq': [
                                                    '$job_status_1', 'Posted'
                                                ]
                                            }, 1, 0
                                        ]
                                    }
                                },
                                'totalNew': {
                                    '$sum': {
                                        '$cond': [
                                            {
                                                '$eq': [
                                                    '$job_status_1', 'New'
                                                ]
                                            }, 1, 0
                                        ]
                                    }
                                },
                                'totalItemsAmount': {
                                    '$sum': '$itemsTotal'
                                },
                                'totalItemsNet': {
                                    '$sum': '$itemsNet'
                                },
                                'totalItemsPaid': {
                                    '$sum': '$itemsPaid'
                                }
                            }
                        }, {
                            '$addFields': {
                                'totalJobs': {
                                    '$add': [
                                        '$totalNew', '$totalPosted'
                                    ]
                                }
                            }
                        }
                    ],
                    'as': 'job_details'
                }
            }, {
                '$match': {
                    'job_details.0': {
                        '$exists': True
                    }
                }
            }, {
                '$project': {
                    'name': 1,
                    'job_details': 1
                }
            }, {
                '$unwind': '$job_details'
            }, {
                '$facet': {
                    'branches': [
                        {
                            '$project': {
                                '_id': 1,
                                'name': 1,
                                'job_details': [
                                    '$job_details'
                                ]
                            }
                        }
                    ],
                    'summary': [
                        {
                            '$group': {
                                '_id': None,
                                'totalPosted': {
                                    '$sum': '$job_details.totalPosted'
                                },
                                'totalNew': {
                                    '$sum': '$job_details.totalNew'
                                },
                                'totalItemsAmount': {
                                    '$sum': '$job_details.totalItemsAmount'
                                },
                                'totalJobs': {
                                    '$sum': '$job_details.totalJobs'
                                },
                                'totalItemsNet': {
                                    '$sum': '$job_details.totalItemsNet'
                                },
                                'totalItemsPaid': {
                                    '$sum': '$job_details.totalItemsPaid'
                                }
                            }
                        }, {
                            '$project': {
                                '_id': None,
                                'name': 'ALL BRANCHES',
                                'job_details': [
                                    {
                                        '_id': None,
                                        'totalPosted': '$totalPosted',
                                        'totalNew': '$totalNew',
                                        'totalItemsAmount': '$totalItemsAmount',
                                        'totalJobs': '$totalJobs',
                                        'totalItemsNet': '$totalItemsNet',
                                        'totalItemsPaid': '$totalItemsPaid',
                                    }
                                ]
                            }
                        }
                    ]
                }
            }, {
                '$project': {
                    'all': {
                        '$concatArrays': [
                            '$branches', '$summary'
                        ]
                    }
                }
            }, {
                '$unwind': '$all'
            }, {
                '$replaceRoot': {
                    'newRoot': '$all'
                }
            }, {
                '$addFields': {
                    '_id': {
                        '$toString': '$_id'
                    },
                    'total_posted': {
                        '$arrayElemAt': [
                            '$job_details.totalPosted', 0
                        ]
                    },
                    'total_new': {
                        '$arrayElemAt': [
                            '$job_details.totalNew', 0
                        ]
                    },
                    'total_items_amount': {
                        '$arrayElemAt': [
                            '$job_details.totalItemsAmount', 0
                        ]
                    },
                    'total_items_net': {
                        '$arrayElemAt': [
                            '$job_details.totalItemsNet', 0
                        ]
                    },
                    'total_items_paid': {
                        '$arrayElemAt': [
                            '$job_details.totalItemsPaid', 0
                        ]
                    },
                    'jobs_count': {
                        '$arrayElemAt': [
                            '$job_details.totalJobs', 0
                        ]
                    }
                }
            }, {
                '$project': {
                    'job_details': 0
                }
            }
        ]
        cursor = await salesman_collection.aggregate(salesman_summary_pipeline)
        results = await cursor.to_list(None)
        return {"salesman_summary": results}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/get_customer_aging_summary")
async def get_customer_aging_summary(data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get('company_id'))
        customers_aging_pipeline = [
            {
                '$match': {
                    'company_id': company_id,
                    'job_status_1': 'Posted'
                }
            }, {
                '$project': {
                    'customer': 1,
                    'invoice_date': 1,
                    'company_id': 1
                }
            }, {
                '$lookup': {
                    'from': 'job_cards_invoice_items',
                    'let': {
                        'job_id': '$_id'
                    },
                    'pipeline': [
                        {
                            '$match': {
                                '$expr': {
                                    '$eq': [
                                        '$job_card_id', '$$job_id'
                                    ]
                                }
                            }
                        }, {
                            '$group': {
                                '_id': None,
                                'total_net': {
                                    '$sum': '$net'
                                }
                            }
                        }
                    ],
                    'as': 'invoice_sum'
                }
            }, {
                '$set': {
                    'net_amount': {
                        '$ifNull': [
                            {
                                '$first': '$invoice_sum.total_net'
                            }, 0
                        ]
                    }
                }
            }, {
                '$lookup': {
                    'from': 'all_receipts_invoices',
                    'let': {
                        'job_id': '$_id'
                    },
                    'pipeline': [
                        {
                            '$match': {
                                '$expr': {
                                    '$eq': [
                                        '$job_id', '$$job_id'
                                    ]
                                }
                            }
                        }, {
                            '$group': {
                                '_id': None,
                                'total_received': {
                                    '$sum': '$amount'
                                }
                            }
                        }
                    ],
                    'as': 'receipt_sum'
                }
            }, {
                '$set': {
                    'received': {
                        '$ifNull': [
                            {
                                '$first': '$receipt_sum.total_received'
                            }, 0
                        ]
                    }
                }
            }, {
                '$set': {
                    'outstanding': {
                        '$round': [
                            {
                                '$subtract': [
                                    '$net_amount', '$received'
                                ]
                            }, 2
                        ]
                    }
                }
            }, {
                '$match': {
                    'outstanding': {
                        '$gt': 0.01
                    }
                }
            }, {
                '$set': {
                    'age_days': {
                        '$dateDiff': {
                            'startDate': '$invoice_date',
                            'endDate': '$$NOW',
                            'unit': 'day'
                        }
                    }
                }
            }, {
                '$set': {
                    'bucket_0_30': {
                        '$cond': [
                            {
                                '$lte': [
                                    '$age_days', 30
                                ]
                            }, '$outstanding', 0
                        ]
                    },
                    'bucket_31_60': {
                        '$cond': [
                            {
                                '$and': [
                                    {
                                        '$gt': [
                                            '$age_days', 30
                                        ]
                                    }, {
                                        '$lte': [
                                            '$age_days', 60
                                        ]
                                    }
                                ]
                            }, '$outstanding', 0
                        ]
                    },
                    'bucket_61_90': {
                        '$cond': [
                            {
                                '$and': [
                                    {
                                        '$gt': [
                                            '$age_days', 60
                                        ]
                                    }, {
                                        '$lte': [
                                            '$age_days', 90
                                        ]
                                    }
                                ]
                            }, '$outstanding', 0
                        ]
                    },
                    'bucket_0_90': {
                        '$cond': [
                            {
                                '$lte': [
                                    '$age_days', 90
                                ]
                            }, '$outstanding', 0
                        ]
                    },
                    'bucket_91_120': {
                        '$cond': [
                            {
                                '$and': [
                                    {
                                        '$gt': [
                                            '$age_days', 90
                                        ]
                                    }, {
                                        '$lte': [
                                            '$age_days', 120
                                        ]
                                    }
                                ]
                            }, '$outstanding', 0
                        ]
                    },
                    'bucket_121_150': {
                        '$cond': [
                            {
                                '$and': [
                                    {
                                        '$gt': [
                                            '$age_days', 120
                                        ]
                                    }, {
                                        '$lte': [
                                            '$age_days', 150
                                        ]
                                    }
                                ]
                            }, '$outstanding', 0
                        ]
                    },
                    'bucket_151_180': {
                        '$cond': [
                            {
                                '$and': [
                                    {
                                        '$gt': [
                                            '$age_days', 150
                                        ]
                                    }, {
                                        '$lte': [
                                            '$age_days', 180
                                        ]
                                    }
                                ]
                            }, '$outstanding', 0
                        ]
                    },
                    'bucket_91_180': {
                        '$cond': [
                            {
                                '$and': [
                                    {
                                        '$gt': [
                                            '$age_days', 91
                                        ]
                                    }, {
                                        '$lte': [
                                            '$age_days', 180
                                        ]
                                    }
                                ]
                            }, '$outstanding', 0
                        ]
                    },
                    'bucket_181_360': {
                        '$cond': [
                            {
                                '$and': [
                                    {
                                        '$gt': [
                                            '$age_days', 180
                                        ]
                                    }, {
                                        '$lte': [
                                            '$age_days', 360
                                        ]
                                    }
                                ]
                            }, '$outstanding', 0
                        ]
                    },
                    'bucket_360_plus': {
                        '$cond': [
                            {
                                '$gt': [
                                    '$age_days', 360
                                ]
                            }, '$outstanding', 0
                        ]
                    }
                }
            }, {
                '$group': {
                    '_id': '$customer',
                    'total_outstanding': {
                        '$sum': '$outstanding'
                    },
                    'bucket_0_30': {
                        '$sum': '$bucket_0_30'
                    },
                    'bucket_31_60': {
                        '$sum': '$bucket_31_60'
                    },
                    'bucket_61_90': {
                        '$sum': '$bucket_61_90'
                    },
                    'bucket_0_90': {
                        '$sum': '$bucket_0_90'
                    },
                    'bucket_91_120': {
                        '$sum': '$bucket_91_120'
                    },
                    'bucket_121_150': {
                        '$sum': '$bucket_121_150'
                    },
                    'bucket_151_180': {
                        '$sum': '$bucket_151_180'
                    },
                    'bucket_91_180': {
                        '$sum': '$bucket_91_180'
                    },
                    'bucket_181_360': {
                        '$sum': '$bucket_181_360'
                    },
                    'bucket_360_plus': {
                        '$sum': '$bucket_360_plus'
                    }
                }
            }, {
                '$lookup': {
                    'from': 'entity_information',
                    'localField': '_id',
                    'foreignField': '_id',
                    'as': 'entity'
                }
            }, {
                '$unwind': '$entity'
            }, {
                '$set': {
                    'group_key': {
                        '$cond': {
                            'if': {
                                '$or': [
                                    {
                                        '$eq': [
                                            '$entity.group_name', None
                                        ]
                                    }, {
                                        '$eq': [
                                            '$entity.group_name', ''
                                        ]
                                    }
                                ]
                            },
                            'then': '$entity.entity_name',
                            'else': '$entity.group_name'
                        }
                    }
                }
            }, {
                '$group': {
                    '_id': '$group_key',
                    'customer_ids': {
                        '$addToSet': '$_id'
                    },
                    'total_outstanding': {
                        '$sum': '$total_outstanding'
                    },
                    'bucket_0_30': {
                        '$sum': '$bucket_0_30'
                    },
                    'bucket_31_60': {
                        '$sum': '$bucket_31_60'
                    },
                    'bucket_61_90': {
                        '$sum': '$bucket_61_90'
                    },
                    'bucket_0_90': {
                        '$sum': '$bucket_0_90'
                    },
                    'bucket_91_120': {
                        '$sum': '$bucket_91_120'
                    },
                    'bucket_121_150': {
                        '$sum': '$bucket_121_150'
                    },
                    'bucket_151_180': {
                        '$sum': '$bucket_151_180'
                    },
                    'bucket_91_180': {
                        '$sum': '$bucket_91_180'
                    },
                    'bucket_181_360': {
                        '$sum': '$bucket_181_360'
                    },
                    'bucket_360_plus': {
                        '$sum': '$bucket_360_plus'
                    }
                }
            }, {
                '$lookup': {
                    'from': 'all_receipts',
                    'let': {
                        'customer_ids': '$customer_ids'
                    },
                    'pipeline': [
                        {
                            '$match': {
                                '$expr': {
                                    '$and': [
                                        {
                                            '$in': [
                                                '$customer', '$$customer_ids'
                                            ]
                                        }, {
                                            '$eq': [
                                                '$company_id', company_id
                                            ]
                                        }, {
                                            '$eq': [
                                                '$status', 'Posted'
                                            ]
                                        }
                                    ]
                                }
                            }
                        }, {
                            '$group': {
                                '_id': None,
                                'last_payment_date': {
                                    '$max': '$receipt_date'
                                }
                            }
                        }
                    ],
                    'as': 'receipt_last'
                }
            }, {
                '$set': {
                    'last_payment_date': {
                        '$ifNull': [
                            {
                                '$first': '$receipt_last.last_payment_date'
                            }, None
                        ]
                    },
                    'total_outstanding': {
                        '$round': [
                            '$total_outstanding', 2
                        ]
                    }
                }
            }, {
                '$match': {
                    'total_outstanding': {
                        '$gt': 0.01
                    }
                }
            }, {
                '$sort': {
                    'bucket_360_plus': -1,
                    'bucket_181_360': -1,
                    'bucket_91_180': -1,
                    'bucket_0_90': -1
                }
            }, {
                '$limit': 2000
            }, {
                '$project': {
                    '_id': 0,
                    'group_name': '$_id',
                    'last_payment_date': 1,
                    'total_outstanding': 1,
                    '0_to_30_days': '$bucket_0_30',
                    '31_to_60_days': '$bucket_31_60',
                    '61_to_90_days': '$bucket_61_90',
                    '0_to_90_days': '$bucket_0_90',
                    '91_to_120_days': '$bucket_91_120',
                    '121_to_150_days': '$bucket_121_150',
                    '151_to_180_days': '$bucket_151_180',
                    '91_to_180_days': '$bucket_91_180',
                    '181_to_360_days': '$bucket_181_360',
                    'more_than_360_days': '$bucket_360_plus'
                }
            }
        ]
        print("Aging loading...")

        cursor = await job_cards_collection.aggregate(customers_aging_pipeline)
        results = await cursor.to_list(None)
        print("Aging done")

        return {"customers_aging": results}

    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/get_account_transfers")
async def get_account_transfers(data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get('company_id'))
        account_summary_pipeline = [
            {
                '$match': {
                    'company_id': company_id,
                    'status': 'Posted'
                }
            }, {
                '$project': {
                    'account': 1,
                    'rate': 1
                }
            }, {
                '$lookup': {
                    'from': 'all_receipts_invoices',
                    'localField': '_id',
                    'foreignField': 'receipt_id',
                    'as': 'inv'
                }
            }, {
                '$set': {
                    'total_with_rate': {
                        '$multiply': [
                            {
                                '$ifNull': [
                                    {
                                        '$sum': '$inv.amount'
                                    }, 0
                                ]
                            }, '$rate'
                        ]
                    }
                }
            }, {
                '$project': {
                    'account': 1,
                    'total_with_rate': 1
                }
            }, {
                '$unionWith': {
                    'coll': 'all_payments',
                    'pipeline': [
                        {
                            '$match': {
                                'company_id': company_id,
                                'status': 'Posted'
                            }
                        }, {
                            '$project': {
                                'account': 1,
                                'rate': 1
                            }
                        }, {
                            '$lookup': {
                                'from': 'all_payments_invoices',
                                'localField': '_id',
                                'foreignField': 'payment_id',
                                'as': 'inv'
                            }
                        }, {
                            '$set': {
                                'total_with_rate': {
                                    '$multiply': [
                                        {
                                            '$ifNull': [
                                                {
                                                    '$sum': '$inv.amount'
                                                }, 0
                                            ]
                                        }, '$rate', -1
                                    ]
                                }
                            }
                        }, {
                            '$project': {
                                'account': 1,
                                'total_with_rate': 1
                            }
                        }
                    ]
                }
            }, {
                '$unionWith': {
                    'coll': 'account_transfers',
                    'pipeline': [
                        {
                            '$match': {
                                'company_id': company_id,
                                'status': 'Posted'
                            }
                        }, {
                            '$project': {
                                'entries': [
                                    {
                                        'account': '$from_account',
                                        'total_with_rate': {
                                            '$multiply': [
                                                '$amount', -1
                                            ]
                                        }
                                    }, {
                                        'account': '$to_account',
                                        'total_with_rate': '$amount'
                                    }
                                ]
                            }
                        }, {
                            '$unwind': '$entries'
                        }, {
                            '$replaceRoot': {
                                'newRoot': '$entries'
                            }
                        }, {
                            '$match': {
                                'account': {
                                    '$ne': None
                                }
                            }
                        }
                    ]
                }
            }, {
                '$group': {
                    '_id': '$account',
                    'amount': {
                        '$sum': '$total_with_rate'
                    }
                }
            }, {
                '$lookup': {
                    'from': 'all_banks',
                    'localField': '_id',
                    'foreignField': '_id',
                    'pipeline': [
                        {
                            '$project': {
                                'account_number': 1
                            }
                        }
                    ],
                    'as': 'bank'
                }
            }, {
                '$set': {
                    'account_number': {
                        '$ifNull': [
                            {
                                '$first': '$bank.account_number'
                            }, None
                        ]
                    }
                }
            }, {
                '$project': {
                    'bank': 0
                }
            }, {
                '$project': {
                    '_id': 0
                }
            }, {
                '$sort': {
                    'amount': -1
                }
            }
        ]

        cursor = await receipts_collection.aggregate(account_summary_pipeline)
        results = await cursor.to_list(None)
        return {"accounts_summary": results}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/get_cashflow_dates")
async def get_cashflow_dates(data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get('company_id'))
        cashflow_pipeline = [
            {
                '$match': {
                    'company_id': company_id,
                    'status': 'Posted'
                }
            }, {
                '$project': {
                    '_id': 0,
                    'date': '$receipt_date'
                }
            }, {
                '$unionWith': {
                    'coll': 'all_payments',
                    'pipeline': [
                        {
                            '$match': {
                                'company_id': company_id,
                                'status': 'Posted'
                            }
                        }, {
                            '$project': {
                                '_id': 0,
                                'date': '$payment_date'
                            }
                        }
                    ]
                }
            }, {
                '$unionWith': {
                    'coll': 'account_transfers',
                    'pipeline': [
                        {
                            '$match': {
                                'company_id': company_id,
                                'status': 'Posted'
                            }
                        }, {
                            '$project': {
                                '_id': 0,
                                'date': 1
                            }
                        }
                    ]
                }
            },

            {
                "$match": {
                    "date": {"$ne": None}
                }
            },
            {
                '$project': {
                    'dateObj': {
                        '$dateTrunc': {
                            'date': '$date',
                            'unit': 'day'
                        }
                    }
                }
            }, {
                '$group': {
                    '_id': '$dateObj'
                }
            }, {
                '$project': {
                    'dateObj': '$_id',
                    'date': {
                        '$dateToString': {
                            'format': '%d-%m-%Y',
                            'date': '$_id'
                        }
                    }
                }
            }, {
                '$sort': {
                    'dateObj': -1
                }
            }, {
                '$setWindowFields': {
                    'sortBy': {
                        'dateObj': -1
                    },
                    'output': {
                        'idx': {
                            '$documentNumber': {}
                        }
                    }
                }
            }, {
                '$project': {
                    'k': {
                        '$toString': '$idx'
                    },
                    'v': {
                        'date': '$date'
                    }
                }
            }, {
                '$group': {
                    '_id': None,
                    'items': {
                        '$push': {
                            'k': '$k',
                            'v': '$v'
                        }
                    }
                }
            }, {
                '$replaceRoot': {
                    'newRoot': {
                        '$arrayToObject': '$items'
                    }
                }
            }
        ]
        cursor = await receipts_collection.aggregate(cashflow_pipeline)
        results = await cursor.to_list(None)
        return {"cashflow_dates": results[0] if results else []}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/get_cashflow_summary")
async def get_cashflow_summary(time_filter: TimeFilter, data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        from_date = time_filter.from_date
        to_date = time_filter.to_date
        print(from_date)
        print(to_date)
        cashflow_pipeline = [
            {
                '$match': {
                    'company_id': company_id,
                    'status': 'Posted',
                    'receipt_date': {
                        '$gte': from_date,
                        '$lt': to_date
                    }
                }
            }, {
                '$project': {
                    'account': 1,
                    'rate': 1
                }
            }, {
                '$lookup': {
                    'from': 'all_receipts_invoices',
                    'localField': '_id',
                    'foreignField': 'receipt_id',
                    'as': 'inv'
                }
            }, {
                '$set': {
                    'received': {
                        '$multiply': [
                            {
                                '$ifNull': [
                                    {
                                        '$sum': '$inv.amount'
                                    }, 0
                                ]
                            }, {
                                '$ifNull': [
                                    '$rate', 1
                                ]
                            }
                        ]
                    },
                    'paid': 0,
                    'trans_in': 0,
                    'trans_out': 0
                }
            }, {
                '$project': {
                    'account': 1,
                    'received': 1,
                    'paid': 1,
                    'trans_in': 1,
                    'trans_out': 1
                }
            }, {
                '$unionWith': {
                    'coll': 'all_payments',
                    'pipeline': [
                        {
                            '$match': {
                                'company_id': company_id,
                                'status': 'Posted',
                                'payment_date': {
                                    '$gte': from_date,
                                    '$lt': to_date
                                }
                            }
                        }, {
                            '$project': {
                                'account': 1,
                                'rate': 1
                            }
                        }, {
                            '$lookup': {
                                'from': 'all_payments_invoices',
                                'localField': '_id',
                                'foreignField': 'payment_id',
                                'as': 'inv'
                            }
                        }, {
                            '$set': {
                                'paid': {
                                    '$multiply': [
                                        {
                                            '$ifNull': [
                                                {
                                                    '$sum': '$inv.amount'
                                                }, 0
                                            ]
                                        }, {
                                            '$ifNull': [
                                                '$rate', 1
                                            ]
                                        }, -1
                                    ]
                                },
                                'received': 0,
                                'trans_in': 0,
                                'trans_out': 0
                            }
                        }, {
                            '$project': {
                                'account': 1,
                                'received': 1,
                                'paid': 1,
                                'trans_in': 1,
                                'trans_out': 1
                            }
                        }
                    ]
                }
            }, {
                '$unionWith': {
                    'coll': 'account_transfers',
                    'pipeline': [
                        {
                            '$match': {
                                'company_id': company_id,
                                'status': 'Posted',
                                'date': {
                                    '$gte': from_date,
                                    '$lt': to_date
                                }
                            }
                        }, {
                            '$project': {
                                'entries': [
                                    {
                                        'account': '$from_account',
                                        'trans_out': {
                                            '$multiply': [
                                                '$amount', -1
                                            ]
                                        },
                                        'trans_in': 0,
                                        'received': 0,
                                        'paid': 0
                                    }, {
                                        'account': '$to_account',
                                        'trans_in': '$amount',
                                        'trans_out': 0,
                                        'received': 0,
                                        'paid': 0
                                    }
                                ]
                            }
                        }, {
                            '$unwind': '$entries'
                        }, {
                            '$replaceRoot': {
                                'newRoot': '$entries'
                            }
                        }, {
                            '$match': {
                                'account': {
                                    '$ne': None
                                }
                            }
                        }
                    ]
                }
            }, {
                '$group': {
                    '_id': '$account',
                    'total_received': {
                        '$sum': '$received'
                    },
                    'total_paid': {
                        '$sum': '$paid'
                    },
                    'total_trans_out': {
                        '$sum': '$trans_out'
                    },
                    'total_trans_in': {
                        '$sum': '$trans_in'
                    }
                }
            }, {
                '$set': {
                    'net': {
                        '$add': [
                            '$total_received', '$total_paid', '$total_trans_out', '$total_trans_in'
                        ]
                    }
                }
            }, {
                '$lookup': {
                    'from': 'all_banks',
                    'localField': '_id',
                    'foreignField': '_id',
                    'pipeline': [
                        {
                            '$project': {
                                'account_number': 1
                            }
                        }
                    ],
                    'as': 'bank'
                }
            }, {
                '$set': {
                    'account_number': {
                        '$first': '$bank.account_number'
                    }
                }
            }, {
                '$project': {
                    '_id': 0,
                    'bank': 0
                }
            }, {
                '$sort': {
                    'net': -1
                }
            }
        ]
        cursor = await receipts_collection.aggregate(cashflow_pipeline)
        results = await cursor.to_list(None)
        print(results)
        return {"summary": results}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/get_post_dated_cheques")
async def get_post_dated_cheques(data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        post_dated_cheques_pipeline = [
            {
                '$match': {
                    'company_id': company_id
                }
            }, {
                '$lookup': {
                    'from': 'all_lists_values',
                    'localField': 'account_type_id',
                    'foreignField': '_id',
                    'as': 'type_details'
                }
            }, {
                '$addFields': {
                    'type': {
                        '$ifNull': [
                            {
                                '$arrayElemAt': [
                                    '$type_details.name', 0
                                ]
                            }, None
                        ]
                    }
                }
            }, {
                '$match': {
                    'type': {
                        '$regex': '^cheque$',
                        '$options': 'i'
                    }
                }
            }, {
                '$project': {
                    '_id': 1,
                    'account_number': 1
                }
            }, {
                '$lookup': {
                    'from': 'all_payments',
                    'let': {
                        'bankId': '$_id',
                        'accNum': '$account_number'
                    },
                    'pipeline': [
                        {
                            '$match': {
                                '$expr':
                                    {
                                        '$eq': [
                                            '$account', '$$bankId'
                                        ]
                                    },

                            }
                        }, {
                            '$lookup': {
                                'from': 'all_payments_invoices',
                                'localField': '_id',
                                'foreignField': 'payment_id',
                                'as': 'invoices'
                            }
                        }, {
                            '$addFields': {
                                'paid': {
                                    '$sum': '$invoices.amount'
                                }
                            }
                        }, {
                            '$addFields': {
                                'paid': {
                                    '$multiply': [
                                        '$paid', '$rate'
                                    ]
                                }
                            }
                        }, {
                            '$project': {
                                'source': {
                                    '$literal': 'payment'
                                },
                                'counter': '$payment_number',
                                'date': '$payment_date',
                                'cheque_date': 1,
                                'beneficiary': '$vendor',
                                'bank_account': '$$accNum',
                                'paid': 1,
                                'received': {
                                    '$literal': 0
                                }
                            }
                        }
                    ],
                    'as': 'payments'
                }
            }, {
                '$lookup': {
                    'from': 'all_receipts',
                    'let': {
                        'bankId': '$_id',
                        'accNum': '$account_number'
                    },
                    'pipeline': [
                        {
                            '$match': {
                                '$expr': {
                                    '$eq': [
                                        '$account', '$$bankId'
                                    ]
                                }
                            }
                        }, {
                            '$lookup': {
                                'from': 'all_receipts_invoices',
                                'localField': '_id',
                                'foreignField': 'receipt_id',
                                'as': 'invoices'
                            }
                        }, {
                            '$addFields': {
                                'received': {
                                    '$sum': '$invoices.amount'
                                }
                            }
                        }, {
                            '$addFields': {
                                'received': {
                                    '$multiply': [
                                        '$received', '$rate'
                                    ]
                                }
                            }
                        }, {
                            '$project': {
                                'source': {
                                    '$literal': 'receipt'
                                },
                                'counter': '$receipt_number',
                                'date': '$receipt_date',
                                'cheque_date': 1,
                                'beneficiary': '$customer',
                                'bank_account': '$$accNum',
                                'received': 1,
                                'paid': {
                                    '$literal': 0
                                }
                            }
                        }
                    ],
                    'as': 'receipts'
                }
            }, {
                '$project': {
                    'data': {
                        '$concatArrays': [
                            '$payments', '$receipts'
                        ]
                    }
                }
            }, {
                '$unwind': '$data'
            }, {
                '$replaceRoot': {
                    'newRoot': '$data'
                }
            }, {
                '$lookup': {
                    'from': 'entity_information',
                    'localField': 'beneficiary',
                    'foreignField': '_id',
                    'as': 'beneficiary_details'
                }
            }, {
                '$unwind': {
                    'path': '$beneficiary_details',
                    'preserveNullAndEmptyArrays': True
                }
            }, {
                '$addFields': {
                    'beneficiary_name': '$beneficiary_details.entity_name'
                }
            }, {
                '$project': {
                    '_id': 0,
                    'counter': 1,
                    'source': 1,
                    'date': 1,
                    'cheque_date': 1,
                    'bank_account': 1,
                    'beneficiary_name': 1,
                    'received': 1,
                    'paid': 1
                }
            },
            {
                "$sort":{
                    "cheque_date": 1
                }
            }
        ]
        cursor = await all_banks_collection.aggregate(post_dated_cheques_pipeline)
        results = await cursor.to_list(None)
        print(len(results))
        return {"summary": results}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

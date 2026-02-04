from datetime import datetime
from typing import Optional

from bson import ObjectId
from fastapi import APIRouter, HTTPException, Depends, UploadFile, Form, File
from pydantic import BaseModel

from app.core import security
from app.database import get_collection

router = APIRouter()
job_cards_collection = get_collection("job_cards")
branches_collection = get_collection("branches")


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
                                                '$ifNull': [
                                                    '$$it.total', 0
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
                                        'totalItemsPaid': '$totalItemsPaid'
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


@router.post("/get_new_job_cards_daily_summary")
async def get_new_job_cards_daily_summary(time_filter: TimeFilter, data: dict = Depends(security.get_current_user)):
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
                            '$group': {
                                '_id': None,
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
                                                '$eq': [
                                                    '$label', 'Returned'
                                                ]
                                            }, 1, 0
                                        ]
                                    }
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
                                'totalNew': {
                                    '$sum': '$job_details.totalNew'
                                },
                                'totalNotApproved': {
                                    '$sum': '$job_details.totalNotApproved'
                                },
                                'totalApproved': {
                                    '$sum': '$job_details.totalApproved'
                                },
                                'totalReady': {
                                    '$sum': '$job_details.totalReady'
                                },
                                'totalReturned': {
                                    '$sum': '$job_details.totalReturned'
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
                '$project': {
                    'job_details': 0
                }
            }
        ]
        cursor = await branches_collection.aggregate(daily_summary_pipeline)
        results = await cursor.to_list(None)
        return {"new_daily_summary": results}


    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

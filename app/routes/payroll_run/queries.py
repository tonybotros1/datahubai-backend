import copy
from typing import Any

from bson import ObjectId
from fastapi import APIRouter, Depends

from app.core import security
from .collections import payroll_runs_collection

router = APIRouter()


all_payroll_runs_pipeline = [
    {
        '$lookup': {
            'from': 'payroll',
            'localField': 'payroll_id',
            'foreignField': '_id',
            'as': 'payroll_details'
        }
    }, {
        '$lookup': {
            'from': 'payroll_period_details',
            'localField': 'period_id',
            'foreignField': '_id',
            'as': 'period_details'
        }
    }, {
        '$addFields': {
            'payroll_name': {
                '$ifNull': [
                    {
                        '$first': '$payroll_details.name'
                    }, None
                ]
            },
            'period_name': {
                '$ifNull': [
                    {
                        '$first': '$period_details.period_name'
                    }, None
                ]
            }
        }
    }, {
        '$project': {
            '_id': {
                '$toString': '$_id'
            },
            'run_number': 1,
            'description': 1,
            'payment_number': 1,
            'payroll_name': 1,
            'period_name': 1
        }
    }
]


payroll_runs_details_pipeline = [
    {
        '$lookup': {
            'from': 'payroll',
            'localField': 'payroll_id',
            'foreignField': '_id',
            'pipeline': [
                {
                    '$project': {
                        '_id': 0,
                        'name': 1
                    }
                }
            ],
            'as': 'payroll_details'
        }
    }, {
        '$lookup': {
            'from': 'payroll_period_details',
            'localField': 'period_id',
            'foreignField': '_id',
            'pipeline': [
                {
                    '$project': {
                        '_id': 0,
                        'period_name': 1,
                        'start_date': 1,
                        'end_date': 1
                    }
                }
            ],
            'as': 'period_details'
        }
    }, {
        '$lookup': {
            'from': 'payroll_runs_employees',
            'localField': '_id',
            'foreignField': 'run_id',
            'pipeline': [
                {
                    '$lookup': {
                        'from': 'employees',
                        'localField': 'employee_id',
                        'foreignField': '_id',
                        'pipeline': [
                            {
                                '$project': {
                                    '_id': 0,
                                    'full_name': 1,
                                    'people_counter': 1
                                }
                            }
                        ],
                        'as': 'employee_details'
                    }
                }, {
                    '$lookup': {
                        'from': 'employees_email',
                        'let': {
                            'employee_id': '$employee_id',
                            'company_id': '$company_id'
                        },
                        'pipeline': [
                            {
                                '$match': {
                                    '$expr': {
                                        '$and': [
                                            {'$eq': ['$employee_id', '$$employee_id']},
                                            {'$eq': ['$company_id', '$$company_id']},
                                            {'$ne': ['$email', None]},
                                            {'$ne': ['$email', '']},
                                            {
                                                '$eq': [
                                                    {
                                                        '$ifNull': ['$use_for_payslips', False]
                                                    },
                                                    True
                                                ]
                                            }
                                        ]
                                    }
                                }
                            }, {
                                '$sort': {
                                    'updatedAt': -1
                                }
                            }, {
                                '$limit': 1
                            }, {
                                '$project': {
                                    '_id': 0,
                                    'email': 1
                                }
                            }
                        ],
                        'as': 'employee_email_details'
                    }
                }, {
                    '$lookup': {
                        'from': 'payroll_runs_employees_elements',
                        'localField': '_id',
                        'foreignField': 'run_employee_id',
                        'pipeline': [
                            {
                                '$lookup': {
                                    'from': 'payroll_elements',
                                    'localField': 'payroll_element_id',
                                    'foreignField': '_id',
                                    'pipeline': [
                                        {
                                            '$project': {
                                                '_id': 0,
                                                'name': 1,
                                                'type': 1
                                            }
                                        }
                                    ],
                                    'as': 'element_details'
                                }
                            }, {
                                '$set': {
                                    'element_name': {
                                        '$first': '$element_details.name'
                                    },
                                    'element_type': {
                                        '$first': '$element_details.type'
                                    }
                                }
                            }, {
                                '$project': {
                                    '_id': {
                                        '$toString': '$_id'
                                    },
                                    'value': 1,
                                    'element_name': 1,
                                    'element_type': 1,
                                    'priority': 1,
                                    'payment': {
                                        '$cond': [
                                            {
                                                '$eq': [
                                                    '$element_type', 'Earning'
                                                ]
                                            }, '$value', 0
                                        ]
                                    },
                                    'deduction': {
                                        '$cond': [
                                            {
                                                '$eq': [
                                                    '$element_type', 'Deduction'
                                                ]
                                            }, '$value', 0
                                        ]
                                    },
                                    'information': {
                                        '$cond': [
                                            {
                                                '$eq': [
                                                    '$element_type', 'Information'
                                                ]
                                            }, '$value', 0
                                        ]
                                    },
                                    'number': {
                                        '$cond': [
                                            {
                                                '$eq': [
                                                    '$element_type', 'Information'
                                                ]
                                            }, '$number', 0
                                        ]
                                    }
                                }
                            }, {
                                '$sort': {
                                    'priority': 1
                                }
                            }
                        ],
                        'as': 'run_employee_details'
                    }
                }, {
                    '$lookup': {
                        'from': 'employees_bank_accounts',
                        'localField': 'employee_id',
                        'foreignField': 'employee_id',
                        'pipeline': [
                            {
                                '$sort': {
                                    'createdAt': -1
                                }
                            }, {
                                '$limit': 1
                            }, {
                                '$lookup': {
                                    'from': 'all_lists_values',
                                    'localField': 'bank_name',
                                    'foreignField': '_id',
                                    'pipeline': [
                                        {
                                            '$project': {
                                                '_id': 0,
                                                'name': 1
                                            }
                                        }
                                    ],
                                    'as': 'bank_name_details'
                                }
                            }, {
                                '$project': {
                                    '_id': 0,
                                    'bank_name': {
                                        '$first': '$bank_name_details.name'
                                    },
                                    'account_number': 1,
                                    'iban': 1,
                                    'swift_code': 1
                                }
                            }
                        ],
                        'as': 'bank_account_details'
                    }
                }, {
                    '$set': {
                        'employee_name': {
                            '$first': '$employee_details.full_name'
                        },
                        'employee_number': {
                            '$first': '$employee_details.people_counter'
                        },
                        'employee_email': {
                            '$first': '$employee_email_details.email'
                        },
                        'bank_account': {
                            '$first': '$bank_account_details'
                        },
                        'total_payments': {
                            '$sum': '$run_employee_details.payment'
                        },
                        'total_deductions': {
                            '$sum': '$run_employee_details.deduction'
                        }
                    }
                }, {
                    '$set': {
                        'net_salary': {
                            '$subtract': [
                                '$total_payments', '$total_deductions'
                            ]
                        }
                    }
                }, {
                    '$set': {
                        'run_employee_information': {
                            '$filter': {
                                'input': '$run_employee_details',
                                'as': 'el',
                                'cond': {
                                    '$eq': [
                                        '$$el.element_type', 'Information'
                                    ]
                                }
                            }
                        },
                        'run_employee_details': {
                            '$filter': {
                                'input': '$run_employee_details',
                                'as': 'el',
                                'cond': {
                                    '$ne': [
                                        '$$el.element_type', 'Information'
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
                        'employee_id': {
                            '$toString': '$employee_id'
                        },
                        'employee_name': 1,
                        'employee_email': 1,
                        'employee_number': 1,
                        'bank_name': '$bank_account.bank_name',
                        'account_number': '$bank_account.account_number',
                        'iban': '$bank_account.iban',
                        'swift_code': '$bank_account.swift_code',
                        'total_payments': 1,
                        'total_deductions': 1,
                        'net_salary': 1,
                        'run_employee_details': 1,
                        'run_employee_information': 1
                    }
                }, {
                    '$sort': {
                        'employee_name': 1
                    }
                }
            ],
            'as': 'employees_details'
        }
    }, {
        '$project': {
            '_id': {
                '$toString': '$_id'
            },
            'run_number': 1,
            'description': 1,
            'payment_number': 1,
            'payroll_name': {
                '$first': '$payroll_details.name'
            },
            'period_name': {
                '$first': '$period_details.period_name'
            },
            'period_start_date': {
                '$first': '$period_details.start_date'
            },
            'period_end_date': {
                '$first': '$period_details.end_date'
            },
            'employees_details': 1
        }
    }
]


@router.get("/get_all_payroll_runs")
async def get_all_payroll_runs(data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        new_pipeline: Any = copy.deepcopy(all_payroll_runs_pipeline)
        new_pipeline.insert(0, {"$match": {"company_id": company_id}})
        new_pipeline.append({"$sort": {"period_name": -1}})
        cursor = await payroll_runs_collection.aggregate(new_pipeline)
        results = await cursor.to_list(None)
        return {"payroll_runs": results}

    except Exception:
        raise


@router.get("/get_payroll_runs_details/{run_id}")
async def get_payroll_runs_details(run_id: str, data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        run_id = ObjectId(run_id)
        new_pipeline: Any = copy.deepcopy(payroll_runs_details_pipeline)
        new_pipeline.insert(0, {"$match": {"company_id": company_id, "_id": run_id}})
        cursor = await payroll_runs_collection.aggregate(new_pipeline)
        results = await cursor.to_list(None)

        # # =================================================
        # start = datetime(2026, 4, 1)
        # end = datetime(2026, 4, 30)
        # await get_leave_days(ObjectId("69cfa8718f07622eb9ce9b68"), start, end)
        # # =================================================

        # # =================================================
        # await get_payroll_element_value(ObjectId("69d64ad68fc5df07583ec9a8"),ObjectId("69cfa8718f07622eb9ce9b68"))
        # # =================================================

        return {"payroll_runs_details": results[0] if len(results) > 0 else None}

    except Exception:
        raise

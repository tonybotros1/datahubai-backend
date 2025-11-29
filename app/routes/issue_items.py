import copy
from typing import Optional, List, Any
from bson import ObjectId
from fastapi import APIRouter, HTTPException, Depends, UploadFile, Form, File
from pydantic import BaseModel
from app import database
from app.core import security
from app.database import get_collection
from datetime import datetime, timezone, timedelta
from app.routes.car_trading import PyObjectId
from app.routes.counters import create_custom_counter
from app.routes.quotation_cards import get_quotation_card_details
from app.widgets.check_date import is_date_equals_today_or_older
from app.widgets.upload_files import upload_file, delete_file_from_server
from app.widgets.upload_images import upload_image, delete_image_from_server

router = APIRouter()
issuing_collection = get_collection("issuing")
inventory_items_collection = get_collection("inventory_items")
converters_collection = get_collection("converters")


def serializer(doc: dict) -> dict:
    def convert(value):
        if isinstance(value, ObjectId):
            return str(value)
        elif isinstance(value, datetime):
            return value.isoformat()
        elif isinstance(value, list):
            return [convert(v) for v in value]
        elif isinstance(value, dict):
            return {k: convert(v) for k, v in value.items()}
        return value

    return {k: convert(v) for k, v in doc.items()}


items_details_pipeline = [
    {
        '$lookup': {
            'from': 'receiving_items',
            'let': {
                'inventory_item_id': '$_id',
                'compId': '$company_id'
            },
            'pipeline': [
                {
                    '$match': {
                        '$expr': {
                            '$and': [
                                {
                                    '$eq': [
                                        '$inventory_item_id', '$$inventory_item_id'
                                    ]
                                }, {
                                    '$eq': [
                                        '$company_id', '$$compId'
                                    ]
                                }
                            ]
                        }
                    }
                }, {
                    '$lookup': {
                        'from': 'receiving',
                        'localField': 'receiving_id',
                        'foreignField': '_id',
                        'as': 'parent_check'
                    }
                }, {
                    '$match': {
                        'parent_check.status': 'Posted'
                    }
                }, {
                    '$sort': {
                        'createdAt': -1,
                        '_id': -1
                    }
                }, {
                    '$limit': 1
                }, {
                    '$project': {
                        'parent_check': 0
                    }
                }
            ],
            'as': 'latest_item'
        }
    }, {
        '$unwind': {
            'path': '$latest_item',
            'preserveNullAndEmptyArrays': True
        }
    }, {
        '$lookup': {
            'from': 'receiving',
            'localField': 'latest_item.receiving_id',
            'foreignField': '_id',
            'as': 'header'
        }
    }, {
        '$unwind': {
            'path': '$header',
            'preserveNullAndEmptyArrays': True
        }
    }, {
        '$lookup': {
            'from': 'receiving_items',
            'let': {
                'rid': '$header._id'
            },
            'pipeline': [
                {
                    '$match': {
                        '$expr': {
                            '$eq': [
                                '$receiving_id', '$$rid'
                            ]
                        }
                    }
                }, {
                    '$group': {
                        '_id': None,
                        'total': {
                            '$sum': {
                                '$multiply': [
                                    {
                                        '$subtract': [
                                            '$original_price', '$discount'
                                        ]
                                    }, '$quantity'
                                ]
                            }
                        }
                    }
                }
            ],
            'as': 'calculated_totals'
        }
    }, {
        '$lookup': {
            'from': 'issuing_items',
            'let': {
                'inventory_item_id': '$_id',
                'compId': '$company_id'
            },
            'pipeline': [
                {
                    '$match': {
                        '$expr': {
                            '$and': [
                                {
                                    '$eq': [
                                        '$inventory_item_id', '$$inventory_item_id'
                                    ]
                                }, {
                                    '$eq': [
                                        '$company_id', '$$compId'
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
                        'total_used_quantity': {
                            '$sum': '$quantity'
                        }
                    }
                }
            ],
            'as': 'issuing_items_details'
        }
    }, {
        '$addFields': {
            'items_total': {
                '$ifNull': [
                    {
                        '$first': '$calculated_totals.total'
                    }, 0
                ]
            },
            'total_used_quantity': {
                '$ifNull': [
                    {
                        '$first': '$issuing_items_details.total_used_quantity'
                    }, 0
                ]
            },
            'rate': {
                '$ifNull': [
                    '$header.rate', 1
                ]
            },
            'overhead_sum': {
                '$add': [
                    {
                        '$ifNull': [
                            '$header.shipping', 0
                        ]
                    }, {
                        '$ifNull': [
                            '$header.handling', 0
                        ]
                    }, {
                        '$ifNull': [
                            '$header.other', 0
                        ]
                    }
                ]
            },
            'global_discount_amount': {
                '$ifNull': [
                    '$header.amount', 0
                ]
            },
            'final_quantity': {
                '$subtract': [
                    {
                        '$ifNull': [
                            '$latest_item.quantity', 0
                        ]
                    }, {
                        '$ifNull': [
                            '$total_used_quantity', 0
                        ]
                    }
                ]
            }
        }
    }, {
        '$addFields': {
            'base_net_price': {
                '$subtract': [
                    {
                        '$ifNull': [
                            '$latest_item.original_price', 0
                        ]
                    }, {
                        '$ifNull': [
                            '$latest_item.discount', 0
                        ]
                    }
                ]
            }
        }
    }, {
        '$addFields': {
            'add_cost': {
                '$cond': [
                    {
                        '$eq': [
                            '$items_total', 0
                        ]
                    }, 0, {
                        '$multiply': [
                            {
                                '$divide': [
                                    '$base_net_price', '$items_total'
                                ]
                            }, '$overhead_sum'
                        ]
                    }
                ]
            },
            'add_disc': {
                '$cond': [
                    {
                        '$eq': [
                            '$items_total', 0
                        ]
                    }, 0, {
                        '$multiply': [
                            {
                                '$divide': [
                                    '$base_net_price', '$items_total'
                                ]
                            }, '$global_discount_amount'
                        ]
                    }
                ]
            }
        }
    }, {
        '$addFields': {
            'local_price': {
                '$multiply': [
                    {
                        '$add': [
                            '$base_net_price', {
                                '$subtract': [
                                    '$add_cost', '$add_disc'
                                ]
                            }
                        ]
                    }, '$rate'
                ]
            }
        }
    }, {
        '$addFields': {
            'last_price': {
                '$ifNull': [
                    {
                        '$add': [
                            {
                                '$ifNull': [
                                    '$latest_item.vat', 0
                                ]
                            }, '$local_price'
                        ]
                    }, 0
                ]
            }
        }
    }, {
        '$addFields': {
            'total': {
                '$multiply': [
                    '$last_price', '$final_quantity'
                ]
            }
        }
    }, {
        '$project': {
            'name': 1,
            'code': 1,
            'total': 1,
            'final_quantity': 1,
            'last_price': 1
        }
    }
]

converters_details_pipeline: list[dict[str, Any]] = [
    {
        '$match': {
            'status': 'Posted'
        }
    }, {
        '$lookup': {
            'from': 'issuing_converters_details',
            'localField': '_id',
            'foreignField': 'converter_id',
            'as': 'used_details'
        }
    }, {
        '$match': {
            'used_details': {
                '$eq': []
            }
        }
    }, {
        '$project': {
            'used_details': 0
        }
    }, {
        '$lookup': {
            'from': 'issuing',
            'let': {
                'converter_id': '$_id'
            },
            'pipeline': [
                {
                    '$match': {
                        '$expr': {
                            '$and': [
                                {
                                    '$eq': [
                                        '$converter_id', '$$converter_id'
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
                    '$lookup': {
                        'from': 'issuing_items_details',
                        'let': {
                            'issue_id': '$_id'
                        },
                        'pipeline': [
                            {
                                '$match': {
                                    '$expr': {
                                        '$eq': [
                                            '$issue_id', '$$issue_id'
                                        ]
                                    }
                                }
                            }, {
                                '$lookup': {
                                    'from': 'inventory_items',
                                    'localField': 'inventory_item_id',
                                    'foreignField': '_id',
                                    'as': 'inventory_details'
                                }
                            }, {
                                '$unwind': {
                                    'path': '$inventory_details',
                                    'preserveNullAndEmptyArrays': True
                                }
                            }, {
                                '$addFields': {
                                    'item_code': {
                                        '$ifNull': [
                                            '$inventory_details.code', None
                                        ]
                                    },
                                    'item_name': {
                                        '$ifNull': [
                                            '$inventory_details.name', None
                                        ]
                                    }
                                }
                            }, {
                                '$project': {
                                    'inventory_details': 0
                                }
                            }
                        ],
                        'as': 'items_details'
                    }
                }, {
                    '$unwind': '$items_details'
                }, {
                    '$addFields': {
                        'item_name': {
                            '$ifNull': [
                                '$items_details.item_name', None
                            ]
                        },
                        'item_code': {
                            '$ifNull': [
                                '$items_details.item_code', None
                            ]
                        },
                        'quantity': {
                            '$ifNull': [
                                '$items_details.quantity', 0
                            ]
                        },
                        'price': {
                            '$ifNull': [
                                '$items_details.price', 0
                            ]
                        }
                    }
                }, {
                    '$addFields': {
                        'total': {
                            '$multiply': [
                                '$quantity', '$price'
                            ]
                        }
                    }
                }, {
                    '$project': {
                        'items_details': 0,
                        'issue_type': 0,
                        'job_card_id': 0,
                        'converter_id': 0,
                        'received_by': 0,
                        'note': 0,
                        'status': 0,
                        'createdAt': 0,
                        'updatedAt': 0
                    }
                }
            ],
            'as': 'issues'
        }
    }, {
        '$project': {
            'date': 0,
            'description': 0,
            'status': 0,
            'company_id': 0,
            'updatedAt': 0,
            'createdAt': 0
        }
    }, {
        '$addFields': {
            'last_price': {
                '$sum': '$issues.total'
            },
            'final_quantity': 1
        }
    }, {
        '$addFields': {
            'total': {
                '$multiply': [
                    '$last_price', '$final_quantity'
                ]
            }
        }
    }, {
        '$project': {
            'issues': 0
        }
    }
]


@router.get("/get_items_details_section")
async def get_items_details_section(data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        new_pipeline = copy.deepcopy(items_details_pipeline)
        new_pipeline.insert(0, {
            "$match": {
                "company_id": company_id
            }
        })
        cursor = await inventory_items_collection.aggregate(new_pipeline)
        results = await cursor.to_list(None)
        print(results[0])
        serialized = [serializer(r) for r in results]
        return {"items_details": serialized}


    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/get_converters_details_section")
async def get_converters_details_section(data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        new_pipeline = copy.deepcopy(converters_details_pipeline)
        new_pipeline.insert(0, {
            "$match": {
                "company_id": company_id
            }
        })
        cursor = await converters_collection.aggregate(new_pipeline)
        results = await cursor.to_list(None)
        serialized = [serializer(r) for r in results]
        return {"converters_details": serialized}


    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/get_issuing_status/{issue_id}")
async def get_issuing_status(issue_id: str, _: dict = Depends(security.get_current_user)):
    try:
        if not ObjectId.is_valid(issue_id):
            raise HTTPException(status_code=400, detail="Invalid issue_id format")

        issue_id = ObjectId(issue_id)

        result = await issuing_collection.find_one(
            {"_id": issue_id},
            {"_id": 0, "status": 1}
        )

        if not result:
            raise HTTPException(status_code=404, detail="Issuing not found")

        return {"status": "success", "data": result}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")

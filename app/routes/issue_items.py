import copy
from typing import Optional, List, Any
from bson import ObjectId
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from app import database
from app.core import security
from app.database import get_collection
from datetime import datetime, timezone, timedelta
from app.routes.car_trading import PyObjectId
from app.routes.counters import create_custom_counter

router = APIRouter()
issuing_collection = get_collection("issuing")
issuing_items_details_collection = get_collection("issuing_items_details")
issuing_converters_details_collection = get_collection("issuing_converters_details")
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


class ItemsDetailsModel(BaseModel):
    id: Optional[str] = None
    inventory_item_id: Optional[str] = None
    quantity: Optional[int] = None
    price: Optional[float] = None
    issue_id: Optional[str] = None
    is_added: Optional[bool] = None
    is_deleted: Optional[bool] = None
    is_modified: Optional[bool] = None


class ConverterDetailsModel(BaseModel):
    id: Optional[str] = None
    converter_id: Optional[str] = None
    quantity: Optional[int] = None
    price: Optional[float] = None
    issue_id: Optional[str] = None
    is_added: Optional[bool] = None
    is_deleted: Optional[bool] = None
    is_modified: Optional[bool] = None


class IssuingModel(BaseModel):
    date: Optional[datetime] = None
    branch: Optional[str] = None
    issue_type: Optional[str] = None
    job_card_id: Optional[str] = None
    converter_id: Optional[str] = None
    note: Optional[str] = None
    received_by: Optional[str] = None
    status: Optional[str] = None
    items_details: Optional[List[ItemsDetailsModel]] = None
    converters_details: Optional[List[ConverterDetailsModel]] = None


class SearchIssuingModel(BaseModel):
    issuing_number: Optional[str] = None
    job_card_id: Optional[PyObjectId] = None
    converter_id: Optional[PyObjectId] = None
    received_by: Optional[PyObjectId] = None
    status: Optional[str] = None
    from_date: Optional[datetime] = None
    to_date: Optional[datetime] = None
    all: Optional[bool] = False
    today: Optional[bool] = False
    this_month: Optional[bool] = False
    this_year: Optional[bool] = False


class SearchItemsDetailsModel(BaseModel):
    code: Optional[str] = None
    name: Optional[str] = None


class SearchConvertersDetailsModel(BaseModel):
    number: Optional[str] = None
    name: Optional[str] = None


issuing_pipeline = [
    {
        '$lookup': {
            'from': 'all_lists_values',
            'localField': 'issue_type',
            'foreignField': '_id',
            'as': 'issue_type_details'
        }
    }, {
        '$unwind': {
            'path': '$issue_type_details',
            'preserveNullAndEmptyArrays': True
        }
    }, {
        '$lookup': {
            'from': 'job_cards',
            'let': {
                'job_card_id': '$job_card_id'
            },
            'pipeline': [
                {
                    '$match': {
                        '$expr': {
                            '$eq': [
                                '$_id', '$$job_card_id'
                            ]
                        }
                    }
                }, {
                    '$lookup': {
                        'from': 'all_brands',
                        'localField': 'car_brand',
                        'foreignField': '_id',
                        'as': 'brand_details'
                    }
                }, {
                    '$unwind': {
                        'path': '$brand_details',
                        'preserveNullAndEmptyArrays': True
                    }
                }, {
                    '$lookup': {
                        'from': 'all_brand_models',
                        'localField': 'car_model',
                        'foreignField': '_id',
                        'as': 'model_details'
                    }
                }, {
                    '$unwind': {
                        'path': '$model_details',
                        'preserveNullAndEmptyArrays': True
                    }
                }, {
                    '$addFields': {
                        'car_brand_name': {
                            '$ifNull': [
                                '$brand_details.name', ''
                            ]
                        },
                        'car_model_name': {
                            '$ifNull': [
                                '$model_details.name', ''
                            ]
                        }
                    }
                }
            ],
            'as': 'job_card_details'
        }
    }, {
        '$unwind': {
            'path': '$job_card_details',
            'preserveNullAndEmptyArrays': True
        }
    }, {
        '$lookup': {
            'from': 'converters',
            'localField': 'converter_id',
            'foreignField': '_id',
            'as': 'converters_details'
        }
    }, {
        '$unwind': {
            'path': '$converters_details',
            'preserveNullAndEmptyArrays': True
        }
    }, {
        '$lookup': {
            'from': 'branches',
            'localField': 'branch',
            'foreignField': '_id',
            'as': 'branch_details'
        }
    }, {
        '$unwind': {
            'path': '$branch_details',
            'preserveNullAndEmptyArrays': True
        }
    }, {
        '$lookup': {
            'from': 'employees',
            'localField': 'received_by',
            'foreignField': '_id',
            'as': 'received_by_details'
        }
    }, {
        '$unwind': {
            'path': '$received_by_details',
            'preserveNullAndEmptyArrays': True
        }
    }, {
        '$addFields': {
            'branch_name': {
                '$ifNull': [
                    '$branch_details.name', None
                ]
            },
            'issue_type_name': {
                '$ifNull': [
                    '$issue_type_details.name', None
                ]
            },
            'received_by_name': {
                '$ifNull': [
                    '$received_by_details.name', None
                ]
            },
            'details_string': {
                '$cond': {
                    'if': {
                        '$ne': [
                            {
                                '$ifNull': [
                                    '$job_card_details._id', None
                                ]
                            }, None
                        ]
                    },
                    'then': {
                        '$concat': [
                            {
                                '$ifNull': [
                                    '$job_card_details.job_number', ''
                                ]
                            }, ' [', {
                                '$ifNull': [
                                    '$job_card_details.car_brand_name', ''
                                ]
                            }, ' ', {
                                '$ifNull': [
                                    '$job_card_details.car_model_name', ''
                                ]
                            }, ']', ' [', {
                                '$ifNull': [
                                    '$job_card_details.plate_number', ''
                                ]
                            }, ']'
                        ]
                    },
                    'else': {
                        '$concat': [
                            {
                                '$ifNull': [
                                    '$converters_details.converter_number', ''
                                ]
                            }, ' [', {
                                '$ifNull': [
                                    '$converters_details.name', ''
                                ]
                            }, ']'
                        ]
                    }
                }
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
                        'as': 'inventory_items_details'
                    }
                }, {
                    '$unwind': {
                        'path': '$inventory_items_details',
                        'preserveNullAndEmptyArrays': True
                    }
                }, {
                    '$addFields': {
                        'name': {
                            '$ifNull': [
                                '$inventory_items_details.name', None
                            ]
                        },
                        'code': {
                            '$ifNull': [
                                '$inventory_items_details.code', None
                            ]
                        },
                        'last_price': '$price',
                        'final_quantity': '$quantity',
                        'total': {
                            '$multiply': [
                                '$price', '$quantity'
                            ]
                        }
                    }
                }, {
                    '$project': {
                        'inventory_items_details': 0,
                        'quantity': 0,
                        'price': 0,
                        'updatedAt': 0,
                        'createdAt': 0
                    }
                }
            ],
            'as': 'items_details_section'
        }
    }, {
        '$lookup': {
            'from': 'issuing_converters_details',
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
                        'from': 'converters',
                        'localField': 'converter_id',
                        'foreignField': '_id',
                        'as': 'converter_details'
                    }
                }, {
                    '$unwind': {
                        'path': '$converter_details',
                        'preserveNullAndEmptyArrays': True
                    }
                }, {
                    '$addFields': {
                        'name': {
                            '$ifNull': [
                                '$converter_details.name', None
                            ]
                        },
                        'converter_number': {
                            '$ifNull': [
                                '$converter_details.converter_number', None
                            ]
                        },
                        'last_price': '$price',
                        'final_quantity': '$quantity',
                        'total': {
                            '$multiply': [
                                '$price', '$quantity'
                            ]
                        }
                    }
                }, {
                    '$project': {
                        'converter_details': 0,
                        'quantity': 0,
                        'price': 0,
                        'updatedAt': 0,
                        'createdAt': 0
                    }
                }
            ],
            'as': 'converters_details_section'
        }
    }, {
        '$project': {
            'received_by_details': 0,
            'branch_details': 0,
            'issue_type_details': 0,
            'converters_details': 0,
            'job_card_details': 0
        }
    }
]

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
            'from': 'issuing_items_details',
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
                    '$group': {
                        '_id': None,
                        'total_used_quantity': {
                            '$sum': '$quantity'
                        }
                    }
                }
            ],
            'as': 'all_issuing_items_details'
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
                        '$first': '$all_issuing_items_details.total_used_quantity'
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
            }
        }
    }, {
        '$addFields': {
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
    },
    {
        '$addFields': {
            '_id': {
                '$toString': '$_id'
            }
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
    },
    {
        '$addFields': {
            '_id': {
                '$toString': '$_id'
            }
        }
    }
]


async def get_issuing_details(receiving_id: ObjectId):
    new_pipeline = copy.deepcopy(issuing_pipeline)
    new_pipeline.insert(0, {
        "$match": {
            "_id": receiving_id
        }
    })
    cursor = await issuing_collection.aggregate(new_pipeline)
    result = await cursor.next()
    return result


@router.post("/get_items_details_section")
async def get_items_details_section(filter_items: SearchItemsDetailsModel,
                                    data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        new_pipeline = copy.deepcopy(items_details_pipeline)
        match_stage = {}
        if company_id:
            match_stage["company_id"] = company_id
        if filter_items.code:
            match_stage["code"] = {"$regex": filter_items.code, "$options": "i"}
        if filter_items.name:
            match_stage["name"] = {"$regex": filter_items.name, "$options": "i"}
        new_pipeline.insert(0, {"$match": match_stage})
        new_pipeline.append({"$limit": 200})
        cursor = await inventory_items_collection.aggregate(new_pipeline)
        results = await cursor.to_list(None)
        return {"items_details": results}


    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/get_converters_details_section")
async def get_converters_details_section(filter_converters: SearchConvertersDetailsModel,
                                         data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        new_pipeline = copy.deepcopy(converters_details_pipeline)
        match_stage = {}
        if company_id:
            match_stage["company_id"] = company_id
        if filter_converters.number:
            match_stage["converter_number"] = {"$regex": filter_converters.number, "$options": "i"}
        if filter_converters.name:
            match_stage["name"] = {"$regex": filter_converters.name, "$options": "i"}
        new_pipeline.insert(0, {"$match": match_stage})
        new_pipeline.append({"$limit": 200})
        cursor = await converters_collection.aggregate(new_pipeline)
        results = await cursor.to_list(None)
        return {"converters_details": results}


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


@router.post("/add_new_issuing")
async def add_new_issuing(issue: IssuingModel, data: dict = Depends(security.get_current_user)):
    async with database.client.start_session() as session:
        try:
            await session.start_transaction()
            company_id = ObjectId(data.get("company_id"))
            issue = issue.model_dump(exclude_unset=True)
            items_details = issue.pop("items_details", None)
            converters_details = issue.pop("converters_details", None)

            iss_ids = ["branch", "issue_type", "received_by", "job_card_id", "converter_id"]
            for field in iss_ids:
                if issue.get(field):
                    issue[field] = ObjectId(issue[field]) if issue[field] else None
            new_issuing_counter = await create_custom_counter("ISN", "IS", data, description="Issuing Number",
                                                              session=session)
            issue["createdAt"] = security.now_utc()
            issue["updatedAt"] = security.now_utc()
            issue["company_id"] = company_id
            issue["issuing_number"] = new_issuing_counter['final_counter'] if new_issuing_counter[
                'success'] else None
            result = await issuing_collection.insert_one(issue, session=session)
            if not result.inserted_id:
                raise HTTPException(status_code=500, detail="Failed to insert issuing")

            if items_details:
                for inv in items_details:
                    if inv.get("inventory_item_id"):
                        inv["inventory_item_id"] = ObjectId(inv["inventory_item_id"])
                    inv["createdAt"] = security.now_utc()
                    inv["updatedAt"] = security.now_utc()
                    inv['company_id'] = company_id
                    inv['issue_id'] = result.inserted_id
                    inv.pop("id", None)
                    inv.pop("is_added", None)
                    inv.pop("is_deleted", None)
                    inv.pop("is_modified", None)

            else:
                items_details = []

            if items_details:
                new_item = await issuing_items_details_collection.insert_many(items_details, session=session)
                if not new_item.inserted_ids:
                    raise HTTPException(status_code=500, detail="Failed to insert issuing items details")

            if converters_details:
                for inv in converters_details:
                    if inv.get("converter_id"):
                        inv["converter_id"] = ObjectId(inv["converter_id"])
                    inv["createdAt"] = security.now_utc()
                    inv["updatedAt"] = security.now_utc()
                    inv['company_id'] = company_id
                    inv['issue_id'] = result.inserted_id
                    inv.pop("id", None)
                    inv.pop("is_added", None)
                    inv.pop("is_deleted", None)
                    inv.pop("is_modified", None)

            else:
                converters_details = []

            if converters_details:
                new_converter = await issuing_converters_details_collection.insert_many(converters_details,
                                                                                        session=session)
                if not new_converter.inserted_ids:
                    raise HTTPException(status_code=500, detail="Failed to insert issuing converters details")

            await session.commit_transaction()
            return {"issuing_id": str(result.inserted_id),
                    "issuing_number": new_issuing_counter['final_counter'] if new_issuing_counter[
                        'success'] else None}

        except Exception as e:
            await session.abort_transaction()
            raise HTTPException(status_code=500, detail=str(e))


@router.patch("/update_issuing/{issue_id}")
async def update_issuing(issue_id: str, issuing: IssuingModel, _: dict = Depends(security.get_current_user)):
    try:
        issue_id = ObjectId(issue_id)
        issuing = issuing.model_dump(exclude_unset=True)

        iss_ids = ["branch", "issue_type", "received_by", "job_card_id", "converter_id"]
        for field in iss_ids:
            if issuing.get(field):
                issuing[field] = ObjectId(issuing[field]) if issuing[field] else None

        issuing.update({
            "updatedAt": security.now_utc(),
        })
        result = await issuing_collection.update_one({"_id": issue_id}, {"$set": issuing})
        if result.modified_count == 0:
            raise HTTPException(status_code=404)

        updated_issuing = await get_issuing_details(issue_id)
        serialized = serializer(updated_issuing)
        return {"issuing": serialized}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.patch("/update_issuing_items_details")
async def update_issuing_items_details(
        items: list[ItemsDetailsModel],
        data: dict = Depends(security.get_current_user)
):
    try:
        company_id = ObjectId(data["company_id"])
        items = [item.model_dump(exclude_unset=True) for item in items]
        issuing_id = ObjectId(items[0]["issue_id"]) if items else None
        if not issuing_id:
            raise HTTPException(status_code=404, detail="Issuing ID not found")
        added_list = []
        deleted_list = []
        modified_list = []

        for item in items:
            if item.get("is_deleted"):
                if "id" not in item or not ObjectId.is_valid(item["id"]):
                    continue
                deleted_list.append(ObjectId(item["id"]))

            elif item.get("is_added") and not item.get("is_deleted"):
                item.pop("id", None)
                item['issue_id'] = ObjectId(item['issue_id']) if item['issue_id'] else None
                item['company_id'] = company_id
                item['inventory_item_id'] = ObjectId(item['inventory_item_id']) if item['inventory_item_id'] else None
                item["createdAt"] = security.now_utc()
                item["updatedAt"] = security.now_utc()
                item['quantity'] = item['quantity']
                item['price'] = item['price']
                item.pop("is_deleted", None)
                item.pop("is_added", None)
                item.pop("is_modified", None)
                added_list.append(item)


            elif item.get("is_modified") and not item.get("is_deleted") and not item.get("is_added"):
                if "id" not in item or not ObjectId.is_valid(item["id"]):
                    continue
                item_id = ObjectId(item["id"])
                item["updatedAt"] = security.now_utc()
                if "inventory_item_id" in item:
                    item.pop('inventory_item_id')
                if "issue_id" in item:
                    item.pop("issue_id", None)
                item.pop("is_deleted", None)
                item.pop("is_added", None)
                item.pop("is_modified", None)
                modified_list.append((item_id, item))

        async with  database.client.start_session() as s:
            try:
                await s.start_transaction()
                if deleted_list:
                    await issuing_items_details_collection.delete_many(
                        {"_id": {"$in": deleted_list}}, session=s
                    )

                if added_list:
                    await issuing_items_details_collection.insert_many(
                        added_list, session=s
                    )

                for item_id, item_data in modified_list:
                    item_data.pop("id", None)
                    await issuing_items_details_collection.update_one(
                        {"_id": item_id},
                        {"$set": item_data},
                        session=s
                    )

                await s.commit_transaction()
            except Exception as e:
                await s.abort_transaction()
                raise HTTPException(status_code=500, detail=str(e))

        # # updated_items = []
        # if issuing_id:
        #     updated_items = serializer(await get_receiving_items_details(receiving_id))
        # else:
        #     updated_items = []
        #
        # return {"updated_items": updated_items}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.patch("/update_issuing_converters_details")
async def update_issuing_converters_details(
        items: list[ConverterDetailsModel],
        data: dict = Depends(security.get_current_user)
):
    try:
        company_id = ObjectId(data["company_id"])
        items = [item.model_dump(exclude_unset=True) for item in items]
        issuing_id = ObjectId(items[0]["issue_id"]) if items else None
        if not issuing_id:
            raise HTTPException(status_code=404, detail="Issuing ID not found")
        added_list = []
        deleted_list = []
        modified_list = []

        for item in items:
            if item.get("is_deleted"):
                if "id" not in item:
                    continue
                deleted_list.append(ObjectId(item["id"]))

            elif item.get("is_added") and not item.get("is_deleted"):
                item.pop("id", None)
                item['issue_id'] = ObjectId(item['issue_id']) if item['issue_id'] else None
                item['company_id'] = company_id
                item['converter_id'] = ObjectId(item['converter_id']) if item['converter_id'] else None
                item["createdAt"] = security.now_utc()
                item["updatedAt"] = security.now_utc()
                item['quantity'] = item['quantity']
                item['price'] = item['price']
                item.pop("is_deleted", None)
                item.pop("is_added", None)
                item.pop("is_modified", None)
                added_list.append(item)


            elif item.get("is_modified") and not item.get("is_deleted") and not item.get("is_added"):
                if "id" not in item:
                    continue
                item_id = ObjectId(item["id"])
                item["updatedAt"] = security.now_utc()
                if "converter_id" in item:
                    item.pop('converter_id')
                if "issue_id" in item:
                    item.pop("issue_id", None)
                item.pop("is_deleted", None)
                item.pop("is_added", None)
                item.pop("is_modified", None)
                modified_list.append((item_id, item))

        async with  database.client.start_session() as s:
            try:
                await s.start_transaction()
                if deleted_list:
                    await issuing_converters_details_collection.delete_many(
                        {"_id": {"$in": deleted_list}}, session=s
                    )

                if added_list:
                    await issuing_converters_details_collection.insert_many(
                        added_list, session=s
                    )

                for item_id, item_data in modified_list:
                    item_data.pop("id", None)
                    await issuing_converters_details_collection.update_one(
                        {"_id": item_id},
                        {"$set": item_data},
                        session=s
                    )

                await s.commit_transaction()
            except Exception as e:
                await s.abort_transaction()
                raise HTTPException(status_code=500, detail=str(e))


    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/delete_issuing/{issuing_id}")
async def delete_issuing(issuing_id: str, _: dict = Depends(security.get_current_user)):
    async with database.client.start_session() as session:
        try:
            await session.start_transaction()
            issuing_id = ObjectId(issuing_id)
            if not issuing_id:
                raise HTTPException(status_code=404, detail="Issuing ID not found")
            current_issuing = await issuing_collection.find_one({"_id": issuing_id}, session=session)
            if not current_issuing:
                raise HTTPException(status_code=404, detail="Issuing not found")
            if current_issuing['status'] != "New":
                raise HTTPException(status_code=403, detail="Only new receiving allowed")
            result = await issuing_collection.delete_one({"_id": issuing_id}, session=session)
            if result.deleted_count == 0:
                raise HTTPException(status_code=404, detail="Issuing not found or already deleted")
            await issuing_items_details_collection.delete_many({"issue_id": issuing_id}, session=session)
            await issuing_converters_details_collection.delete_many({"issue_id": issuing_id}, session=session)

            await session.commit_transaction()
            return {"message": "Issuing deleted successfully", "issuing_id": str(issuing_id)}

        except HTTPException:
            await session.abort_transaction()
            raise

        except Exception as e:
            await session.abort_transaction()
            raise HTTPException(status_code=500, detail=f"Delete failed: {str(e)}")


@router.post("/search_engine_for_issuing")
async def search_engine_for_issuing(
        filter_issuing: SearchIssuingModel,
        data: dict = Depends(security.get_current_user),
):
    try:
        company_id = data.get("company_id")
        if not company_id:
            raise HTTPException(status_code=400, detail="Company ID missing")

        company_id = ObjectId(company_id)
        search_pipeline = copy.deepcopy(issuing_pipeline)

        match_stage = {}
        if company_id:
            match_stage['company_id'] = company_id
        if filter_issuing.issuing_number:
            match_stage["issuing_number"] = {
                "$regex": filter_issuing.issuing_number, "$options": "i"
            }
        if filter_issuing.job_card_id:
            match_stage["job_card_id"] = filter_issuing.job_card_id
        if filter_issuing.converter_id:
            match_stage["converter_id"] = filter_issuing.converter_id
        if filter_issuing.status:
            match_stage["status"] = filter_issuing.status

        # 2️⃣ Handle date filters
        now = datetime.now(timezone.utc)
        date_field = "date"
        date_filter = {}

        if filter_issuing.today:
            start = datetime(now.year, now.month, now.day, tzinfo=timezone.utc)
            end = start + timedelta(days=1)
            date_filter[date_field] = {"$gte": start, "$lt": end}

        elif filter_issuing.this_month:
            start = datetime(now.year, now.month, 1, tzinfo=timezone.utc)
            if now.month == 12:
                end = datetime(now.year + 1, 1, 1, tzinfo=timezone.utc)
            else:
                end = datetime(now.year, now.month + 1, 1, tzinfo=timezone.utc)
            date_filter[date_field] = {"$gte": start, "$lt": end}

        elif filter_issuing.this_year:
            start = datetime(now.year, 1, 1, tzinfo=timezone.utc)
            end = datetime(now.year + 1, 1, 1, tzinfo=timezone.utc)
            date_filter[date_field] = {"$gte": start, "$lt": end}

        elif filter_issuing.from_date or filter_issuing.to_date:
            date_filter[date_field] = {}
            if filter_issuing.from_date:
                date_filter[date_field]["$gte"] = filter_issuing.from_date
            if filter_issuing.to_date:
                date_filter[date_field]["$lte"] = filter_issuing.to_date

        # Merge both filters into one $match
        if date_filter:
            match_stage.update(date_filter)

        search_pipeline.insert(0, {"$match": match_stage})

        # 3️⃣ Add computed field
        search_pipeline.append({
            '$addFields': {
                'totals': {
                    '$add': [
                        {
                            '$sum': '$items_details_section.total'
                        }, {
                            '$sum': '$converters_details_section.total'
                        }
                    ]
                }
            }
        })
        search_pipeline.append({
            "$facet": {
                "issuing": [
                    {"$sort": {"issuing_number": -1}},
                ],
                "grand_totals": [
                    {
                        "$group": {
                            "_id": None,
                            "grand_total": {"$sum": "$totals"},
                        }
                    },
                    {
                        "$project": {
                            "_id": 0
                        }
                    }
                ]
            }
        })

        cursor = await issuing_collection.aggregate(search_pipeline)
        result = await cursor.to_list(None)

        if result and len(result) > 0:
            data = result[0]
            receiving = [serializer(r) for r in data.get("issuing", [])]
            totals = data.get("grand_totals", [])
            grand_totals = totals[0] if totals else {"grand_total": 0}
        else:
            receiving = []
            grand_totals = {"grand_totals": 0}

        return {
            "issuing": receiving,
            "grand_totals": grand_totals
        }


    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")

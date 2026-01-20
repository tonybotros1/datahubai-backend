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
from app.routes.job_cards import serializer

router = APIRouter()
receiving_collection = get_collection("receiving")
receiving_items_collection = get_collection("receiving_items")

receiving_details_pipeline: list[dict[str, Any]] = [
    {
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
            'from': 'currencies',
            'let': {
                'currency_id': '$currency'
            },
            'pipeline': [
                {
                    '$match': {
                        '$expr': {
                            '$eq': [
                                '$_id', '$$currency_id'
                            ]
                        }
                    }
                }, {
                    '$lookup': {
                        'from': 'all_countries',
                        'let': {
                            'country_id': '$country_id'
                        },
                        'pipeline': [
                            {
                                '$match': {
                                    '$expr': {
                                        '$eq': [
                                            '$_id', '$$country_id'
                                        ]
                                    }
                                }
                            }
                        ],
                        'as': 'country_details'
                    }
                }, {
                    '$unwind': {
                        'path': '$country_details',
                        'preserveNullAndEmptyArrays': True
                    }
                }
            ],
            'as': 'currency_details'
        }
    }, {
        '$unwind': {
            'path': '$currency_details',
            'preserveNullAndEmptyArrays': True
        }
    }, {
        '$lookup': {
            'from': 'entity_information',
            'localField': 'vendor',
            'foreignField': '_id',
            'as': 'vendor_details'
        }
    }, {
        '$unwind': {
            'path': '$vendor_details',
            'preserveNullAndEmptyArrays': True
        }
    }, {
        '$lookup': {
            'from': 'employees',
            'let': {
                'approvedId': '$approved_by',
                'orderedId': '$ordered_by',
                'purchasedId': '$purchased_by'
            },
            'pipeline': [
                {
                    '$match': {
                        '$expr': {
                            '$in': [
                                '$_id', [
                                    '$$approvedId', '$$orderedId', '$$purchasedId'
                                ]
                            ]
                        }
                    }
                }, {
                    '$project': {
                        '_id': 1,
                        'name': 1
                    }
                }
            ],
            'as': 'employees_details'
        }
    }, {
        '$lookup': {
            'from': 'receiving_items',
            'let': {
                'receiving_id': '$_id'
            },
            'pipeline': [
                {
                    '$match': {
                        '$expr': {
                            '$eq': [
                                '$receiving_id', '$$receiving_id'
                            ]
                        }
                    }
                }, {
                    '$lookup': {
                        'from': 'inventory_items',
                        'localField': 'inventory_item_id',
                        'foreignField': '_id',
                        'as': 'inventory_item_details'
                    }
                }, {
                    '$unwind': {
                        'path': '$inventory_item_details',
                        'preserveNullAndEmptyArrays': True
                    }
                }, {
                    '$addFields': {
                        'inventory_item_name': {
                            '$ifNull': [
                                '$inventory_item_details.name', None
                            ]
                        },
                        'inventory_item_code': {
                            '$ifNull': [
                                '$inventory_item_details.code', None
                            ]
                        }
                    }
                }, {
                    '$project': {
                        'inventory_item_details': 0
                    }
                }
            ],
            'as': 'items_details'
        }
    }, {
        '$addFields': {
            'currency_code': {
                '$ifNull': [
                    '$currency_details.country_details.currency_code', None
                ]
            },
            'items_total': {
                '$sum': {
                    '$map': {
                        'input': '$items_details',
                        'as': 'item',
                        'in': {
                            '$multiply': [
                                {
                                    '$subtract': [
                                        {'$toDouble': {'$ifNull': ['$$item.original_price', 0]}},
                                        {'$toDouble': {'$ifNull': ['$$item.discount', 0]}}
                                    ]
                                }, {'$toDouble': {'$ifNull': ['$$item.quantity', 0]}}
                            ]
                        }
                    }
                }
            }
        }
    }, {
        '$addFields': {
            'items_details': {
                '$map': {
                    'input': '$items_details',
                    'as': 'item',
                    'in': {
                        '$mergeObjects': [
                            '$$item', {
                                'add_cost': {
                                    '$cond': [
                                        {
                                            '$or': [
                                                {
                                                    '$eq': [
                                                        '$$item.quantity', 0
                                                    ]
                                                }, {
                                                    '$eq': [
                                                        '$items_total', 0
                                                    ]
                                                }
                                            ]
                                        }, 0, {
                                            '$divide': [
                                                {
                                                    '$multiply': [
                                                        {
                                                            '$divide': [
                                                                {
                                                                    '$multiply': [
                                                                        {
                                                                            '$subtract': [
                                                                                '$$item.original_price',
                                                                                '$$item.discount'
                                                                            ]
                                                                        }, '$$item.quantity'
                                                                    ]
                                                                }, '$items_total'
                                                            ]
                                                        }, {
                                                            '$add': [
                                                                '$handling', '$shipping', '$other'
                                                            ]
                                                        }
                                                    ]
                                                }, '$$item.quantity'
                                            ]
                                        }
                                    ]
                                },
                                'add_disc': {
                                    '$cond': [
                                        {
                                            '$or': [
                                                {
                                                    '$eq': [
                                                        '$$item.quantity', 0
                                                    ]
                                                }, {
                                                    '$eq': [
                                                        '$items_total', 0
                                                    ]
                                                }
                                            ]
                                        }, 0, {
                                            '$divide': [
                                                {
                                                    '$multiply': [
                                                        {
                                                            '$divide': [
                                                                {
                                                                    '$multiply': [
                                                                        {
                                                                            '$subtract': [
                                                                                '$$item.original_price',
                                                                                '$$item.discount'
                                                                            ]
                                                                        }, '$$item.quantity'
                                                                    ]
                                                                }, '$items_total'
                                                            ]
                                                        }, '$amount'
                                                    ]
                                                }, '$$item.quantity'
                                            ]
                                        }
                                    ]
                                }
                            }
                        ]
                    }
                }
            }
        }
    }, {
        '$addFields': {
            'items_details': {
                '$map': {
                    'input': '$items_details',
                    'as': 'item',
                    'in': {
                        '$mergeObjects': [
                            '$$item', {
                                'local_price': {
                                    '$multiply': [
                                        {
                                            '$add': [
                                                {
                                                    '$subtract': [
                                                        {'$toDouble': {'$ifNull': ['$$item.original_price', 0]}},
                                                        {'$toDouble': {'$ifNull': ['$$item.discount', 0]}}
                                                    ]
                                                }, {
                                                    '$subtract': [
                                                        {'$toDouble': {'$ifNull': ['$$item.add_cost', 0]}},
                                                        {'$toDouble': {'$ifNull': ['$$item.add_disc', 0]}}
                                                    ]
                                                }
                                            ]
                                        },  { '$toDouble': { '$ifNull': ['$rate', 1] } }
                                    ]
                                }
                            }
                        ]
                    }
                }
            }
        }
    }, {
        '$addFields': {
            'items_details': {
                '$map': {
                    'input': '$items_details',
                    'as': 'item',
                    'in': {
                        '$mergeObjects': [
                            '$$item', {
                                'total': {
                                    '$multiply': [
                                        '$$item.local_price', '$$item.quantity'
                                    ]
                                },
                                'net': {
                                    '$add': [
                                        '$$item.total', '$$item.vat'
                                    ]
                                }
                            }
                        ]
                    }
                }
            }
        }
    }, {
        '$addFields': {
            'items_details': {
                '$map': {
                    'input': '$items_details',
                    'as': 'item',
                    'in': {
                        '$mergeObjects': [
                            '$$item', {
                                'net': {
                                    '$add': [
                                        '$$item.total', '$$item.vat'
                                    ]
                                }
                            }
                        ]
                    }
                }
            }
        }
    }, {
        '$addFields': {
            'approved_by_name': {
                '$first': {
                    '$map': {
                        'input': {
                            '$filter': {
                                'input': '$employees_details',
                                'as': 'emp',
                                'cond': {
                                    '$eq': [
                                        '$$emp._id', '$approved_by'
                                    ]
                                }
                            }
                        },
                        'as': 'emp',
                        'in': '$$emp.name'
                    }
                }
            },
            'ordered_by_name': {
                '$first': {
                    '$map': {
                        'input': {
                            '$filter': {
                                'input': '$employees_details',
                                'as': 'emp',
                                'cond': {
                                    '$eq': [
                                        '$$emp._id', '$ordered_by'
                                    ]
                                }
                            }
                        },
                        'as': 'emp',
                        'in': '$$emp.name'
                    }
                }
            },
            'purchased_by_name': {
                '$first': {
                    '$map': {
                        'input': {
                            '$filter': {
                                'input': '$employees_details',
                                'as': 'emp',
                                'cond': {
                                    '$eq': [
                                        '$$emp._id', '$purchased_by'
                                    ]
                                }
                            }
                        },
                        'as': 'emp',
                        'in': '$$emp.name'
                    }
                }
            },
            'branch_name': {
                '$ifNull': [
                    '$branch_details.name', None
                ]
            },
            'vendor_name': {
                '$ifNull': [
                    '$vendor_details.entity_name', None
                ]
            }
        }
    }, {
        '$project': {
            'employees_details': 0,
            'branch_details': 0,
            'vendor_details': 0,
            'currency_details': 0
        }
    }
]
receiving_items_details_for_selected_receiving_doc_pipeline: list[dict[str, Any]] = [
    {
        '$lookup': {
            'from': 'receiving_items',
            'let': {
                'receiving_id': '$_id'
            },
            'pipeline': [
                {
                    '$match': {
                        '$expr': {
                            '$eq': [
                                '$receiving_id', '$$receiving_id'
                            ]
                        }
                    }
                }, {
                    '$lookup': {
                        'from': 'inventory_items',
                        'localField': 'inventory_item_id',
                        'foreignField': '_id',
                        'as': 'inventory_item_details'
                    }
                }, {
                    '$unwind': {
                        'path': '$inventory_item_details',
                        'preserveNullAndEmptyArrays': True
                    }
                }, {
                    '$addFields': {
                        'inventory_item_name': {
                            '$ifNull': [
                                '$inventory_item_details.name', None
                            ]
                        },
                        'inventory_item_code': {
                            '$ifNull': [
                                '$inventory_item_details.code', None
                            ]
                        }
                    }
                }, {
                    '$project': {
                        'inventory_item_details': 0
                    }
                }
            ],
            'as': 'items_details'
        }
    }, {
        '$addFields': {
            'items_total': {
                '$sum': {
                    '$map': {
                        'input': '$items_details',
                        'as': 'item',
                        'in': {
                            '$multiply': [
                                {
                                    '$subtract': [
                                        '$$item.original_price', '$$item.discount'
                                    ]
                                }, '$$item.quantity'
                            ]
                        }
                    }
                }
            }
        }
    }, {
        '$addFields': {
            'items_details': {
                '$map': {
                    'input': '$items_details',
                    'as': 'item',
                    'in': {
                        '$mergeObjects': [
                            '$$item', {
                                'add_cost': {
                                    '$cond': [
                                        {
                                            '$or': [
                                                {
                                                    '$eq': [
                                                        '$$item.quantity', 0
                                                    ]
                                                }, {
                                                    '$eq': [
                                                        '$items_total', 0
                                                    ]
                                                }
                                            ]
                                        }, 0, {
                                            '$divide': [
                                                {
                                                    '$multiply': [
                                                        {
                                                            '$divide': [
                                                                {
                                                                    '$multiply': [
                                                                        {
                                                                            '$subtract': [
                                                                                '$$item.original_price',
                                                                                '$$item.discount'
                                                                            ]
                                                                        }, '$$item.quantity'
                                                                    ]
                                                                }, '$items_total'
                                                            ]
                                                        }, {
                                                            '$add': [
                                                                '$handling', '$shipping', '$other'
                                                            ]
                                                        }
                                                    ]
                                                }, '$$item.quantity'
                                            ]
                                        }
                                    ]
                                },
                                'add_disc': {
                                    '$cond': [
                                        {
                                            '$or': [
                                                {
                                                    '$eq': [
                                                        '$$item.quantity', 0
                                                    ]
                                                }, {
                                                    '$eq': [
                                                        '$items_total', 0
                                                    ]
                                                }
                                            ]
                                        }, 0, {
                                            '$divide': [
                                                {
                                                    '$multiply': [
                                                        {
                                                            '$divide': [
                                                                {
                                                                    '$multiply': [
                                                                        {
                                                                            '$subtract': [
                                                                                '$$item.original_price',
                                                                                '$$item.discount'
                                                                            ]
                                                                        }, '$$item.quantity'
                                                                    ]
                                                                }, '$items_total'
                                                            ]
                                                        }, '$amount'
                                                    ]
                                                }, '$$item.quantity'
                                            ]
                                        }
                                    ]
                                }
                            }
                        ]
                    }
                }
            }
        }
    }, {
        '$addFields': {
            'items_details': {
                '$map': {
                    'input': '$items_details',
                    'as': 'item',
                    'in': {
                        '$mergeObjects': [
                            '$$item', {
                                'local_price': {
                                    '$multiply': [
                                        {
                                            '$add': [
                                                {
                                                    '$subtract': [
                                                        '$$item.original_price', '$$item.discount'
                                                    ]
                                                }, {
                                                    '$subtract': [
                                                        '$$item.add_cost', '$$item.add_disc'
                                                    ]
                                                }
                                            ]
                                        }, '$rate'
                                    ]
                                }
                            }
                        ]
                    }
                }
            }
        }
    }, {
        '$addFields': {
            'items_details': {
                '$map': {
                    'input': '$items_details',
                    'as': 'item',
                    'in': {
                        '$mergeObjects': [
                            '$$item', {
                                'total': {
                                    '$multiply': [
                                        '$$item.local_price', '$$item.quantity'
                                    ]
                                },
                                'net': {
                                    '$add': [
                                        '$$item.total', '$$item.vat'
                                    ]
                                }
                            }
                        ]
                    }
                }
            }
        }
    }, {
        '$addFields': {
            'items_details': {
                '$map': {
                    'input': '$items_details',
                    'as': 'item',
                    'in': {
                        '$mergeObjects': [
                            '$$item', {
                                'net': {
                                    '$add': [
                                        '$$item.total', '$$item.vat'
                                    ]
                                }
                            }
                        ]
                    }
                }
            }
        }
    }, {
        '$project': {
            '_id': 0,
            'items_details': 1
        }
    }
]


class ReceivingItemModel(BaseModel):
    id: Optional[str] = None
    inventory_item_id: Optional[str] = None
    quantity: Optional[int] = None
    original_price: Optional[float] = None
    discount: Optional[float] = None
    vat: Optional[float] = None
    receiving_id: Optional[str] = None
    is_added: Optional[bool] = None
    is_deleted: Optional[bool] = None
    is_modified: Optional[bool] = None


class ReceivingModel(BaseModel):
    date: Optional[datetime] = None
    branch: Optional[str] = None
    reference_number: Optional[str] = None
    vendor: Optional[str] = None
    note: Optional[str] = None
    currency: Optional[str] = None
    rate: Optional[float] = None
    approved_by: Optional[str] = None
    ordered_by: Optional[str] = None
    purchased_by: Optional[str] = None
    shipping: Optional[float] = None
    handling: Optional[float] = None
    other: Optional[float] = None
    amount: Optional[float] = None
    status: Optional[str] = None
    items: Optional[List[ReceivingItemModel]] = None


class SearchReceivingModel(BaseModel):
    receiving_number: Optional[str] = None
    reference_number: Optional[str] = None
    vendor: Optional[PyObjectId] = None
    status: Optional[str] = None
    from_date: Optional[datetime] = None
    to_date: Optional[datetime] = None
    all: Optional[bool] = False
    today: Optional[bool] = False
    this_month: Optional[bool] = False
    this_year: Optional[bool] = False


async def get_receiving_details(receiving_id: ObjectId):
    new_pipeline = copy.deepcopy(receiving_details_pipeline)
    new_pipeline.insert(0, {
        "$match": {
            "_id": receiving_id
        }
    })
    cursor = await receiving_collection.aggregate(new_pipeline)
    result = await cursor.next()
    return result


async def get_receiving_items_details(receiving_id: ObjectId):
    new_pipeline = copy.deepcopy(receiving_items_details_for_selected_receiving_doc_pipeline)
    new_pipeline.insert(0, {
        "$match": {
            "_id": receiving_id
        }
    })
    cursor = await receiving_collection.aggregate(new_pipeline)
    result = await cursor.next()
    return result


@router.post("/add_new_receiving")
async def add_new_receiving(receive: ReceivingModel, data: dict = Depends(security.get_current_user)):
    async with database.client.start_session() as session:
        try:
            await session.start_transaction()
            company_id = ObjectId(data.get("company_id"))
            receive = receive.model_dump(exclude_unset=True)
            receiving_items = receive.pop("items", None)

            rec_ids = ["branch", "vendor", "currency", "approved_by", "ordered_by", "purchased_by"]
            for field in rec_ids:
                if receive.get(field):
                    receive[field] = ObjectId(receive[field]) if receive[field] else None
            new_receiving_counter = await create_custom_counter("REN", "RE", data, description="Receiving Number",
                                                                session=session)
            receive["createdAt"] = security.now_utc()
            receive["updatedAt"] = security.now_utc()
            receive["company_id"] = company_id
            receive["receiving_number"] = new_receiving_counter['final_counter'] if new_receiving_counter[
                'success'] else None
            result = await receiving_collection.insert_one(receive, session=session)
            if not result.inserted_id:
                raise HTTPException(status_code=500, detail="Failed to insert receiving")

            if receiving_items:
                for inv in receiving_items:
                    print(inv)
                    if inv.get("inventory_item_id"):
                        inv["inventory_item_id"] = ObjectId(inv["inventory_item_id"])
                    inv["createdAt"] = security.now_utc()
                    inv["updatedAt"] = security.now_utc()
                    inv['company_id'] = company_id
                    inv['receiving_id'] = result.inserted_id
                    inv.pop("id", None)
                    inv.pop("is_added", None)
                    inv.pop("is_deleted", None)
                    inv.pop("is_modified", None)

            else:
                receiving_items = []

            if receiving_items:
                new_invoices = await receiving_items_collection.insert_many(receiving_items, session=session)
                if not new_invoices.inserted_ids:
                    raise HTTPException(status_code=500, detail="Failed to insert receiving items")

            await session.commit_transaction()
            new_receiving = await get_receiving_details(result.inserted_id)
            serialized = serializer(new_receiving)
            return {"receiving": serialized}

        except Exception as e:
            await session.abort_transaction()
            raise HTTPException(status_code=500, detail=str(e))


@router.patch("/update_receiving/{receiving_id}")
async def update_receiving(receiving_id: str, receiving: ReceivingModel, _: dict = Depends(security.get_current_user)):
    try:
        receiving_id = ObjectId(receiving_id)
        receiving = receiving.model_dump(exclude_unset=True)

        rec_ids = ["branch", "vendor", "currency", "approved_by", "ordered_by", "purchased_by"]
        for field in rec_ids:
            if receiving.get(field):
                receiving[field] = ObjectId(receiving[field])

        receiving.update({
            "updatedAt": security.now_utc(),
        })
        result = await receiving_collection.update_one({"_id": receiving_id}, {"$set": receiving})
        if result.modified_count == 0:
            raise HTTPException(status_code=404)

        updated_receiving = await get_receiving_details(receiving_id)
        serialized = serializer(updated_receiving)
        return {"receiving": serialized}

    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=str(e))


@router.patch("/update_receiving_items")
async def update_receiving_items(
        items: list[ReceivingItemModel],
        data: dict = Depends(security.get_current_user)
):
    try:
        company_id = ObjectId(data["company_id"])
        items = [item.model_dump(exclude_unset=True) for item in items]
        receiving_id = ObjectId(items[0]["receiving_id"]) if items else None
        if not receiving_id:
            raise HTTPException(status_code=404, detail="Receiving ID not found")
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
                item['receiving_id'] = ObjectId(item['receiving_id']) if item['receiving_id'] else None
                item['company_id'] = company_id
                item['inventory_item_id'] = ObjectId(item['inventory_item_id']) if item['inventory_item_id'] else None
                item["createdAt"] = security.now_utc()
                item["updatedAt"] = security.now_utc()
                item['quantity'] = item['quantity']
                item['discount'] = item['discount']
                item['original_price'] = item['original_price']
                item['vat'] = item['vat']
                item.pop("is_deleted", None)
                item.pop("is_added", None)
                item.pop("is_modified", None)
                added_list.append(item)


            elif item.get("is_modified") and not item.get("is_deleted") and not item.get("is_added"):
                if "id" not in item:
                    continue
                item_id = ObjectId(item["id"])
                item["updatedAt"] = security.now_utc()
                if "inventory_item_id" in item:
                    item['inventory_item_id'] = ObjectId(item['inventory_item_id']) if item[
                        'inventory_item_id'] else None
                if "receiving_id" in item:
                    item.pop("receiving_id", None)
                item.pop("is_deleted", None)
                item.pop("is_added", None)
                item.pop("is_modified", None)
                modified_list.append((item_id, item))

        async with  database.client.start_session() as s:
            try:
                await s.start_transaction()
                if deleted_list:
                    await receiving_items_collection.delete_many(
                        {"_id": {"$in": deleted_list}}, session=s
                    )

                if added_list:
                    await receiving_items_collection.insert_many(
                        added_list, session=s
                    )

                for item_id, item_data in modified_list:
                    item_data.pop("id", None)
                    await receiving_items_collection.update_one(
                        {"_id": item_id},
                        {"$set": item_data},
                        session=s
                    )

                await s.commit_transaction()
            except Exception as e:
                await s.abort_transaction()
                raise HTTPException(status_code=500, detail=str(e))

        updated_items = []
        if receiving_id:
            updated_items = serializer(await get_receiving_items_details(receiving_id))
        else:
            updated_items = []

        return {"updated_items": updated_items}

    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/delete_receiving/{receiving_id}")
async def delete_receiving(receiving_id: str, _: dict = Depends(security.get_current_user)):
    async with database.client.start_session() as session:
        try:
            await session.start_transaction()
            receiving_id = ObjectId(receiving_id)
            if not receiving_id:
                raise HTTPException(status_code=404, detail="Receiving ID not found")
            current_receiving = await receiving_collection.find_one({"_id": receiving_id}, session=session)
            if not current_receiving:
                raise HTTPException(status_code=404, detail="Receiving not found")
            if current_receiving['status'] != "New":
                raise HTTPException(status_code=403, detail="Only new receiving allowed")
            result = await receiving_collection.delete_one({"_id": receiving_id}, session=session)
            if result.deleted_count == 0:
                raise HTTPException(status_code=404, detail="Receiving not found or already deleted")
            await receiving_items_collection.delete_many({"receiving_id": receiving_id}, session=session)

            await session.commit_transaction()
            return {"message": "Receiving deleted successfully", "receiving_id": str(receiving_id)}

        except HTTPException:
            await session.abort_transaction()
            raise

        except Exception as e:
            print(e)
            await session.abort_transaction()
            raise HTTPException(status_code=500, detail=f"Delete failed: {str(e)}")


@router.get("/get_receiving_status/{receiving_id}")
async def get_receiving_status(receiving_id: str, _: dict = Depends(security.get_current_user)):
    try:
        if not ObjectId.is_valid(receiving_id):
            raise HTTPException(status_code=400, detail="Invalid receiving_id format")

        receiving_id = ObjectId(receiving_id)

        result = await receiving_collection.find_one(
            {"_id": receiving_id},
            {"_id": 0, "status": 1}
        )

        if not result:
            raise HTTPException(status_code=404, detail="Receiving not found")

        return {"status": "success", "data": result}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")


@router.post("/search_engine_for_receiving")
async def search_engine_for_receiving(
        filter_receiving: SearchReceivingModel,
        data: dict = Depends(security.get_current_user),
):
    try:
        company_id = data.get("company_id")
        if not company_id:
            raise HTTPException(status_code=400, detail="Company ID missing")

        company_id = ObjectId(company_id)
        search_pipeline = copy.deepcopy(receiving_details_pipeline)

        match_stage = {}
        if company_id:
            match_stage['company_id'] = company_id
        if filter_receiving.vendor:
            match_stage["vendor"] = filter_receiving.vendor
        if filter_receiving.receiving_number:
            match_stage["receiving_number"] = {
                "$regex": filter_receiving.receiving_number, "$options": "i"
            }
        if filter_receiving.reference_number:
            match_stage["reference_number"] = {
                "$regex": filter_receiving.reference_number, "$options": "i"
            }
        if filter_receiving.status:
            match_stage["status"] = filter_receiving.status

        # 2️⃣ Handle date filters
        now = datetime.now(timezone.utc)
        date_field = "date"
        date_filter = {}

        if filter_receiving.today:
            start = datetime(now.year, now.month, now.day, tzinfo=timezone.utc)
            end = start + timedelta(days=1)
            date_filter[date_field] = {"$gte": start, "$lt": end}

        elif filter_receiving.this_month:
            start = datetime(now.year, now.month, 1, tzinfo=timezone.utc)
            if now.month == 12:
                end = datetime(now.year + 1, 1, 1, tzinfo=timezone.utc)
            else:
                end = datetime(now.year, now.month + 1, 1, tzinfo=timezone.utc)
            date_filter[date_field] = {"$gte": start, "$lt": end}

        elif filter_receiving.this_year:
            start = datetime(now.year, 1, 1, tzinfo=timezone.utc)
            end = datetime(now.year + 1, 1, 1, tzinfo=timezone.utc)
            date_filter[date_field] = {"$gte": start, "$lt": end}

        elif filter_receiving.from_date or filter_receiving.to_date:
            date_filter[date_field] = {}
            if filter_receiving.from_date:
                date_filter[date_field]["$gte"] = filter_receiving.from_date
            if filter_receiving.to_date:
                date_filter[date_field]["$lte"] = filter_receiving.to_date

        # Merge both filters into one $match
        if date_filter:
            match_stage.update(date_filter)

        search_pipeline.insert(0, {"$match": match_stage})

        # 3️⃣ Add computed field
        search_pipeline.append({
            "$addFields": {
                "totals": {"$sum": "$items_details.total"},
                "vats": {"$sum": "$items_details.vat"},
                "nets": {"$sum": "$items_details.net"},
            }
        })
        search_pipeline.append({
            "$facet": {
                "receiving": [
                    {"$sort": {"receiving_number": -1}},
                ],
                "grand_totals": [
                    {
                        "$group": {
                            "_id": None,
                            "grand_total": {"$sum": "$totals"},
                            "grand_vat": {"$sum": "$vats"},
                            "grand_net": {"$sum": "$nets"},
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

        cursor = await receiving_collection.aggregate(search_pipeline)
        result = await cursor.to_list(None)

        if result and len(result) > 0:
            data = result[0]
            receiving = [serializer(r) for r in data.get("receiving", [])]
            totals = data.get("grand_totals", [])
            grand_totals = totals[0] if totals else {"grand_total": 0, "grand_vat": 0, "grand_net": 0}
        else:
            receiving = []
            grand_totals = {"grand_received": 0}

        return {
            "receiving": receiving,
            "grand_totals": grand_totals
        }


    except HTTPException:
        raise
    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")

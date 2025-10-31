import copy
from typing import Optional, List
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
receipts_collection = get_collection("all_receipts")
receipts_invoices_collection = get_collection("all_receipts_invoices")
job_cards_collection = get_collection("job_cards")


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


class Invoices(BaseModel):
    id: Optional[str] = None
    job_id: Optional[str] = None
    amount: Optional[float] = None
    receipt_id: Optional[str] = None
    is_added: Optional[bool] = None
    is_modified: Optional[bool] = None
    is_deleted: Optional[bool] = None


class ReceiptsModel(BaseModel):
    status: Optional[str] = None
    receipt_date: Optional[datetime] = None
    customer: Optional[str] = None
    note: Optional[str] = None
    receipt_type: Optional[str] = None
    cheque_number: Optional[str] = None
    bank_name: Optional[str] = None
    cheque_date: Optional[datetime] = None
    account: Optional[str] = None
    currency: Optional[str] = None
    rate: Optional[float] = None
    invoices: Optional[List[Invoices]] = None


class ReceiptSearch(BaseModel):
    receipt_number: Optional[str] = None
    receipt_type: Optional[PyObjectId] = None
    customer_name: Optional[PyObjectId] = None
    account: Optional[PyObjectId] = None
    bank_name: Optional[PyObjectId] = None
    cheque_number: Optional[str] = None
    status: Optional[str] = None
    from_date: Optional[datetime] = None
    to_date: Optional[datetime] = None
    all: Optional[bool] = False
    today: Optional[bool] = False
    this_month: Optional[bool] = False
    this_year: Optional[bool] = False


ar_receipt_details_pipeline = [
    {
        '$lookup': {
            'from': 'entity_information',
            'localField': 'customer',
            'foreignField': '_id',
            'as': 'customer_details'
        }
    }, {
        '$unwind': {
            'path': '$customer_details',
            'preserveNullAndEmptyArrays': True
        }
    }, {
        '$lookup': {
            'from': 'all_lists_values',
            'localField': 'receipt_type',
            'foreignField': '_id',
            'as': 'receipt_type_details'
        }
    }, {
        '$unwind': {
            'path': '$receipt_type_details',
            'preserveNullAndEmptyArrays': True
        }
    }, {
        '$lookup': {
            'from': 'all_lists_values',
            'localField': 'bank_name',
            'foreignField': '_id',
            'as': 'bank_details'
        }
    }, {
        '$unwind': {
            'path': '$bank_details',
            'preserveNullAndEmptyArrays': True
        }
    }, {
        '$lookup': {
            'from': 'all_banks',
            'localField': 'account',
            'foreignField': '_id',
            'as': 'account_details'
        }
    }, {
        '$unwind': {
            'path': '$account_details',
            'preserveNullAndEmptyArrays': True
        }
    }, {
        '$lookup': {
            'from': 'all_receipts_invoices',
            'let': {
                'receipt_id': '$_id'
            },
            'pipeline': [
                {
                    '$match': {
                        '$expr': {
                            '$eq': [
                                '$receipt_id', '$$receipt_id'
                            ]
                        }
                    }
                }, {
                    '$project': {
                        'receipt_invoice_id': '$_id',
                        'job_id': 1,
                        'amount': 1
                    }
                }, {
                    '$lookup': {
                        'from': 'job_cards',
                        'let': {
                            'job_id': '$job_id'
                        },
                        'pipeline': [
                            {
                                '$match': {
                                    '$expr': {
                                        '$eq': [
                                            '$_id', '$$job_id'
                                        ]
                                    }
                                }
                            }, {
                                '$project': {
                                    'car_brand': 1,
                                    'car_model': 1,
                                    'invoice_number': 1,
                                    'invoice_date': 1,
                                    'plate_number': 1
                                }
                            }
                        ],
                        'as': 'job_details'
                    }
                }, {
                    '$unwind': {
                        'path': '$job_details',
                        'preserveNullAndEmptyArrays': True
                    }
                }, {
                    '$lookup': {
                        'from': 'all_brands',
                        'let': {
                            'brand_id': '$job_details.car_brand'
                        },
                        'pipeline': [
                            {
                                '$match': {
                                    '$expr': {
                                        '$eq': [
                                            '$_id', '$$brand_id'
                                        ]
                                    }
                                }
                            }, {
                                '$project': {
                                    'name': 1
                                }
                            }
                        ],
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
                        'let': {
                            'model_id': '$job_details.car_model'
                        },
                        'pipeline': [
                            {
                                '$match': {
                                    '$expr': {
                                        '$eq': [
                                            '$_id', '$$model_id'
                                        ]
                                    }
                                }
                            }, {
                                '$project': {
                                    'name': 1
                                }
                            }
                        ],
                        'as': 'model_details'
                    }
                }, {
                    '$unwind': {
                        'path': '$model_details',
                        'preserveNullAndEmptyArrays': True
                    }
                }, {
                    '$lookup': {
                        'from': 'job_cards_invoice_items',
                        'let': {
                            'job_id': '$job_details._id'
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
                                '$project': {
                                    'net': 1
                                }
                            }
                        ],
                        'as': 'invoice_items_details'
                    }
                }, {
                    '$addFields': {
                        'net_amount': {
                            '$sum': '$invoice_items_details.net'
                        },
                        'receipt_amount': {
                            '$ifNull': [
                                '$amount', 0
                            ]
                        },
                        'invoice_date_str': {
                            '$dateToString': {
                                'format': '%d-%m-%Y',
                                'date': '$job_details.invoice_date'
                            }
                        }
                    }
                }, {
                    '$addFields': {
                        'outstanding_amount': {
                            '$subtract': [
                                '$net_amount', '$receipt_amount'
                            ]
                        }
                    }
                }, {
                    '$match': {
                        'outstanding_amount': {
                            '$gt': 0
                        }
                    }
                }, {
                    '$addFields': {
                        '_id': '$receipt_invoice_id'
                    }
                }, {
                    '$project': {
                        '_id': 1,
                        'is_selected': {
                            '$literal': True
                        },
                        'job_id': '$job_details._id',
                        'invoice_number': '$job_details.invoice_number',
                        'invoice_date': '$invoice_date_str',
                        'invoice_amount': {
                            '$ifNull': [
                                '$net_amount', 0
                            ]
                        },
                        'receipt_amount': 1,
                        'outstanding_amount': 1,
                        'notes': {
                            '$concat': [
                                'Invoice Number: ', {
                                    '$ifNull': [
                                        '$job_details.invoice_number', ''
                                    ]
                                }, ', Invoice Date: ', '$invoice_date_str', ', Brand: ', {
                                    '$ifNull': [
                                        '$brand_details.name', ''
                                    ]
                                }, ', Model: ', {
                                    '$ifNull': [
                                        '$model_details.name', ''
                                    ]
                                }, ', Plate Number: ', {
                                    '$ifNull': [
                                        '$job_details.plate_number', ''
                                    ]
                                }
                            ]
                        }
                    }
                }
            ],
            'as': 'invoices_details'
        }
    }, {
        '$addFields': {
            'customer_name': {
                '$ifNull': [
                    '$customer_details.entity_name', None
                ]
            },
            'receipt_type_name': {
                '$ifNull': [
                    '$receipt_type_details.name', None
                ]
            },
            'account_number': {
                '$ifNull': [
                    '$account_details.account_number', None
                ]
            },
            'bank_name': {
                '$ifNull': [
                    '$bank_details.name', None
                ]
            },
            'bank_name_id': {
                '$ifNull': ['$bank_details._id', None]
            }
        }
    }, {
        '$project': {
            'customer_details': 0,
            'receipt_type_details': 0,
            'bank_details': 0,
            'account_details': 0
        }
    }
]


@router.get("/get_all_customer_invoices/{customer_id}")
async def get_all_customer_invoices(customer_id: str, data: dict = Depends(security.get_current_user)):
    try:
        customer_id = ObjectId(customer_id)
        company_id = ObjectId(data.get("company_id"))
        customer_invoices_pipeline = [
            {
                '$match': {
                    'company_id': company_id,
                    'customer': customer_id,
                    'job_status_1': 'Posted'
                }
            }, {
                '$lookup': {
                    'from': 'all_brands',
                    'let': {
                        'brand_id': '$car_brand'
                    },
                    'pipeline': [
                        {
                            '$match': {
                                '$expr': {
                                    '$eq': [
                                        '$_id', '$$brand_id'
                                    ]
                                }
                            }
                        }, {
                            '$project': {
                                'name': 1
                            }
                        }
                    ],
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
                    'let': {
                        'model_id': '$car_model'
                    },
                    'pipeline': [
                        {
                            '$match': {
                                '$expr': {
                                    '$eq': [
                                        '$_id', '$$model_id'
                                    ]
                                }
                            }
                        }, {
                            '$project': {
                                'name': 1
                            }
                        }
                    ],
                    'as': 'model_details'
                }
            }, {
                '$unwind': {
                    'path': '$model_details',
                    'preserveNullAndEmptyArrays': True
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
                            '$project': {
                                'net': 1
                            }
                        }
                    ],
                    'as': 'invoice_items_details'
                }
            }, {
                '$addFields': {
                    'net_amount': {
                        '$sum': {
                            '$map': {
                                'input': '$invoice_items_details',
                                'as': 'item',
                                'in': {
                                    '$ifNull': [
                                        '$$item.net', 0
                                    ]
                                }
                            }
                        }
                    }
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
                    'received': {
                        '$sum': {
                            '$map': {
                                'input': '$receipts_invoices_details',
                                'as': 'receipt',
                                'in': {
                                    '$ifNull': [
                                        '$$receipt.amount', 0
                                    ]
                                }
                            }
                        }
                    }
                }
            }, {
                '$addFields': {
                    'final_outstanding': {
                        '$subtract': [
                            '$net_amount', '$received'
                        ]
                    }
                }
            }, {
                '$match': {
                    'final_outstanding': {
                        '$gt': 0
                    }
                }
            }, {
                '$project': {
                    '_id': 0,
                    'is_selected': {
                        '$literal': False
                    },
                    'job_id': '$_id',
                    'invoice_number': 1,
                    'invoice_date': {
                        '$dateToString': {
                            'format': '%d-%m-%Y',
                            'date': '$invoice_date'
                        }
                    },
                    'invoice_amount': {
                        '$toString': '$net_amount'
                    },
                    'receipt_amount': {
                        '$toString': '$received'
                    },
                    'outstanding_amount': {
                        '$toString': '$final_outstanding'
                    },
                    'notes': {
                        '$concat': [
                            'Invoice Number: ', {
                                '$ifNull': [
                                    '$invoice_number', ''
                                ]
                            }, ', ', 'Invoice Date: ', {
                                '$dateToString': {
                                    'format': '%d-%m-%Y',
                                    'date': '$invoice_date'
                                }
                            }, ', ', 'Brand: ', {
                                '$ifNull': [
                                    '$brand_details.name', ''
                                ]
                            }, ', ', 'Model: ', {
                                '$ifNull': [
                                    '$model_details.name', ''
                                ]
                            }, ', ', 'Plate Number: ', {
                                '$ifNull': [
                                    '$plate_number', ''
                                ]
                            }
                        ]
                    }
                }
            }
        ]
        cursor = await job_cards_collection.aggregate(customer_invoices_pipeline)
        results = await cursor.to_list(None)
        serialized = [serializer(r) for r in results]
        return {"invoices": serialized}


    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"failed: {str(e)}")


async def get_receipt_details(receipt_id: ObjectId):
    new_pipeline = ar_receipt_details_pipeline.copy()
    new_pipeline.insert(1, {
        "$match": {
            "_id": receipt_id
        }
    })
    cursor = await receipts_collection.aggregate(new_pipeline)
    result = await cursor.next()
    return result


@router.post("/add_new_receipt")
async def add_new_receipt(
        receipt: ReceiptsModel,
        data: dict = Depends(security.get_current_user)
):
    async with database.client.start_session() as session:
        try:
            await session.start_transaction()
            company_id = ObjectId(data.get("company_id"))
            new_receipt_counter = await create_custom_counter("RN", "R", data, session)

            # Convert the Pydantic model to a dict, excluding unset values
            receipt_dict = receipt.model_dump(exclude_unset=True)
            receipt_invoices = receipt_dict.pop("invoices", None)

            id_fields = ["customer", "receipt_type", "bank_name", "account"]
            for field in id_fields:
                if receipt_dict.get(field):
                    receipt_dict[field] = ObjectId(receipt_dict[field]) if receipt_dict[field] else None

            receipt_dict["company_id"] = company_id
            receipt_dict["createdAt"] = security.now_utc()
            receipt_dict["updatedAt"] = security.now_utc()
            receipt_dict["receipt_number"] = new_receipt_counter['final_counter'] if new_receipt_counter[
                'success'] else None
            result = await receipts_collection.insert_one(receipt_dict, session=session)
            if not result.inserted_id:
                raise HTTPException(status_code=500, detail="Failed to insert receipt")

            # receipt_invoices = []
            if receipt_invoices:
                for inv in receipt_invoices:
                    if inv.get("job_id"):
                        inv["job_id"] = ObjectId(inv["job_id"])
                        inv["createdAt"] = security.now_utc()
                        inv["updatedAt"] = security.now_utc()
                        inv['company_id'] = company_id
                        inv['receipt_id'] = result.inserted_id
                        inv.pop("id", None)
                        inv.pop("is_added", None)
                        inv.pop("is_deleted", None)
                        inv.pop("is_modified", None)
                # receipt_invoices = receipt_dict.pop("invoices", None)

            else:
                receipt_invoices = []

            if receipt_invoices:
                new_invoices = await receipts_invoices_collection.insert_many(receipt_invoices, session=session)
                if not new_invoices.inserted_ids:
                    raise HTTPException(status_code=500, detail="Failed to insert receipt invoices")

            await session.commit_transaction()
            new_receipt = await get_receipt_details(result.inserted_id)
            serialized = serializer(new_receipt)
            return {"receipt": serialized}

        except Exception as e:
            await session.abort_transaction()
            raise HTTPException(status_code=500, detail=str(e))


@router.patch("/update_receipt_invoices")
async def update_receipt_invoices(
        items: list[Invoices],
        data: dict = Depends(security.get_current_user)
):
    try:
        company_id = ObjectId(data["company_id"])
        items = [item.model_dump(exclude_unset=True) for item in items]

        added_list = []
        deleted_list = []
        modified_list = []
        updated_list = []

        for item in items:
            print(item)
            if item.get("is_deleted"):
                if "id" not in item:
                    continue
                print('yes deleted')
                print(item['id'])
                deleted_list.append(ObjectId(item["id"]))

            elif item.get("is_added") and not item.get("is_deleted"):
                print('yes added')
                item.pop("id", None)
                item['receipt_id'] = ObjectId(item['receipt_id']) if item['receipt_id'] else None
                item['company_id'] = company_id
                item['job_id'] = ObjectId(item['job_id']) if item['job_id'] else None
                item["createdAt"] = security.now_utc()
                item["updatedAt"] = security.now_utc()
                item['amount'] = item['amount']
                item.pop("is_deleted", None)
                item.pop("is_added", None)
                item.pop("is_modified", None)
                added_list.append(item)


            elif item.get("is_modified") and not item.get("is_deleted") and not item.get("is_added"):
                if "id" not in item:
                    continue
                item_id = ObjectId(item["id"])
                print('yes modified')
                print(item_id)
                item["updatedAt"] = security.now_utc()
                if "job_id" in item:
                    item.pop("job_id", None)
                if "receipt_id" in item:
                    item.pop("receipt_id", None)
                item["amount"] = item["amount"] if item["amount"] else None
                item.pop("is_deleted", None)
                item.pop("is_added", None)
                item.pop("is_modified", None)
                modified_list.append((item_id, item))

        async with  database.client.start_session() as s:
            await s.start_transaction()
            if deleted_list:
                await receipts_invoices_collection.delete_many(
                    {"_id": {"$in": deleted_list}}, session=s
                )

            if added_list:
                added_invoices = await receipts_invoices_collection.insert_many(
                    added_list, session=s
                )
                inserted_ids = added_invoices.inserted_ids
                for item, new_id in zip(added_list, inserted_ids):
                    response_item = {
                        "_id": str(new_id),
                        "job_id": str(item.get("job_id")),
                    }
                    updated_list.append(response_item)

            for item_id, item_data in modified_list:
                item_data.pop("id", None)
                await receipts_invoices_collection.update_one(
                    {"_id": item_id},
                    {"$set": item_data},
                    session=s
                )
                updated_list.append(
                    {"_id": str(item_id), "job_id": str(item_data["job_id"]) if item_data.get("job_id") else None})

            await s.commit_transaction()
        return {"updated_items": updated_list, "deleted_items": [str(d) for d in deleted_list]}

    except Exception as e:
        print(e)
        await s.abort_transaction()
        raise HTTPException(status_code=500, detail=str(e))


@router.patch("/update_ar_receipt/{receipt_id}")
async def update_ar_receipt(receipt_id: str, receipt: ReceiptsModel, _: dict = Depends(security.get_current_user)):
    try:
        receipt_id = ObjectId(receipt_id)
        receipt_data_dict = receipt.model_dump(exclude_unset=True)

        id_fields = ["customer", "receipt_type", "bank_name", "account"]
        for field in id_fields:
            if receipt_data_dict.get(field):
                receipt_data_dict[field] = ObjectId(receipt_data_dict[field])

        receipt_data_dict.update({
            "updatedAt": security.now_utc(),
        })
        result = await receipts_collection.update_one({"_id": receipt_id}, {"$set": receipt_data_dict})
        if result.modified_count == 0:
            raise HTTPException(status_code=404)

    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/delete_receipt/{receipt_id}")
async def delete_receipt(receipt_id: str, _: dict = Depends(security.get_current_user)):
    async with database.client.start_session() as session:
        try:
            await session.start_transaction()
            receipt_id = ObjectId(receipt_id)
            if not receipt_id:
                raise HTTPException(status_code=404, detail="Receipt ID not found")
            current_receipt = await receipts_collection.find_one({"_id": receipt_id}, session=session)
            if not current_receipt:
                raise HTTPException(status_code=404, detail="Receipt not found")
            if current_receipt['status'] != "New":
                raise HTTPException(status_code=403, detail="Only New Receipts allowed")
            result = await receipts_collection.delete_one({"_id": receipt_id}, session=session)
            if result.deleted_count == 0:
                raise HTTPException(status_code=404, detail="Receipt not found or already deleted")
            await receipts_invoices_collection.delete_many({"receipt_id": receipt_id}, session=session)

            await session.commit_transaction()
            return {"message": "Receipt deleted successfully", "receipt_id": str(receipt_id)}

        except HTTPException:
            await session.abort_transaction()
            raise

        except Exception as e:
            print(e)
            await session.abort_transaction()
            raise HTTPException(status_code=500, detail=f"Delete failed: {str(e)}")


@router.get("/get_ar_receipt_status/{receipt_id}")
async def get_ar_receipt_status(receipt_id: str, _: dict = Depends(security.get_current_user)):
    try:
        if not ObjectId.is_valid(receipt_id):
            raise HTTPException(status_code=400, detail="Invalid receipt id format")

        receipt_object_id = ObjectId(receipt_id)

        result = await receipts_collection.find_one(
            {"_id": receipt_object_id},
            {"_id": 0, "status": 1}
        )

        if not result:
            raise HTTPException(status_code=404, detail="AR Receipt not found")

        return {"status": "success", "data": result}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")


@router.post("/search_engine_for_ar_receipts")
async def search_engine_for_ar_receipts(
        filter_receipts: ReceiptSearch,
        data: dict = Depends(security.get_current_user),
):
    try:
        company_id = data.get("company_id")
        if not company_id:
            raise HTTPException(status_code=400, detail="Company ID missing")

        company_id = ObjectId(company_id)
        search_pipeline = copy.deepcopy(ar_receipt_details_pipeline)

        match_stage = {}
        if company_id:
            match_stage['company_id'] = company_id
        if filter_receipts.account:
            match_stage["account"] = filter_receipts.account
        if filter_receipts.bank_name:
            match_stage["bank_name"] = filter_receipts.bank_name
        if filter_receipts.receipt_type:
            match_stage["receipt_type"] = filter_receipts.receipt_type
        if filter_receipts.receipt_number:
            match_stage["receipt_number"] = {
                "$regex": filter_receipts.receipt_number, "$options": "i"
            }
        if filter_receipts.cheque_number:
            match_stage["cheque_number"] = {
                "$regex": filter_receipts.cheque_number, "$options": "i"
            }
        if filter_receipts.customer_name:
            match_stage["customer"] = filter_receipts.customer_name
        if filter_receipts.status:
            match_stage["status"] = filter_receipts.status

        # 2️⃣ Handle date filters
        now = datetime.now(timezone.utc)
        date_field = "receipt_date"
        date_filter = {}

        if filter_receipts.today:
            start = datetime(now.year, now.month, now.day, tzinfo=timezone.utc)
            end = start + timedelta(days=1)
            date_filter[date_field] = {"$gte": start, "$lt": end}

        elif filter_receipts.this_month:
            start = datetime(now.year, now.month, 1, tzinfo=timezone.utc)
            if now.month == 12:
                end = datetime(now.year + 1, 1, 1, tzinfo=timezone.utc)
            else:
                end = datetime(now.year, now.month + 1, 1, tzinfo=timezone.utc)
            date_filter[date_field] = {"$gte": start, "$lt": end}

        elif filter_receipts.this_year:
            start = datetime(now.year, 1, 1, tzinfo=timezone.utc)
            end = datetime(now.year + 1, 1, 1, tzinfo=timezone.utc)
            date_filter[date_field] = {"$gte": start, "$lt": end}

        elif filter_receipts.from_date or filter_receipts.to_date:
            date_filter[date_field] = {}
            if filter_receipts.from_date:
                date_filter[date_field]["$gte"] = filter_receipts.from_date
            if filter_receipts.to_date:
                date_filter[date_field]["$lte"] = filter_receipts.to_date

        # Merge both filters into one $match
        if date_filter:
            match_stage.update(date_filter)

        search_pipeline.insert(1, {"$match": match_stage})

        # 3️⃣ Add computed field
        search_pipeline.append({
            "$addFields": {
                "total_received": {"$sum": "$invoices_details.receipt_amount"}
            }
        })
        search_pipeline.append({
            "$facet": {
                "receipts": [
                    {"$sort": {"receipt_number": -1}},
                ],
                "grand_totals": [
                    {
                        "$group": {
                            "_id": None,
                            "grand_received": {"$sum": "$total_received"},
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

        cursor = await receipts_collection.aggregate(search_pipeline)
        result = await cursor.to_list(None)

        if result and len(result) > 0:
            data = result[0]
            receipts = [serializer(r) for r in data.get("receipts", [])]
            totals = data.get("grand_totals", [])
            grand_totals = totals[0] if totals else {"grand_received": 0}
        else:
            receipts = []
            grand_totals = {"grand_received": 0}

        return {
            "receipts": receipts,
            "grand_totals": grand_totals
        }


    except HTTPException:
        raise
    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")

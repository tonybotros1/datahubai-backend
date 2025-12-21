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
ap_invoices_collection = get_collection("ap_invoices")
ap_payment_collection = get_collection("all_payments")
ap_payment_invoices_collection = get_collection("all_payments_invoices")

ap_payment_details_pipeline: list[dict[str, Any]] = [
    {
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
            'from': 'all_lists_values',
            'localField': 'payment_type',
            'foreignField': '_id',
            'as': 'payment_type_details'
        }
    }, {
        '$unwind': {
            'path': '$payment_type_details',
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
            'from': 'all_payments_invoices',
            'let': {
                'payment_id': '$_id'
            },
            'pipeline': [
                {
                    '$match': {
                        '$expr': {
                            '$eq': [
                                '$payment_id', '$$payment_id'
                            ]
                        }
                    }
                }, {
                    '$project': {
                        'payment_invoice_id': '$_id',
                        'ap_invoices_id': 1,
                        'amount': 1,
                        'payment_id': 1
                    }
                }, {
                    '$lookup': {
                        'from': 'all_payments_invoices',
                        'let': {
                            'apInvoiceId': '$ap_invoices_id'
                        },
                        'pipeline': [
                            {
                                '$match': {
                                    '$expr': {
                                        '$eq': [
                                            '$ap_invoices_id', '$$apInvoiceId'
                                        ]
                                    }
                                }
                            }
                        ],
                        'as': 'all_payments_for_ap_invoice'
                    }
                }, {
                    '$addFields': {
                        'total_paid_for_payment': {
                            '$sum': '$all_payments_for_ap_invoice.amount'
                        }
                    }
                }, {
                    '$lookup': {
                        'from': 'ap_invoices',
                        'let': {
                            'ap_inv_id': '$ap_invoices_id'
                        },
                        'pipeline': [
                            {
                                '$match': {
                                    '$expr': {
                                        '$eq': [
                                            '$_id', '$$ap_inv_id'
                                        ]
                                    }
                                }
                            }, {
                                '$project': {
                                    'invoice_number': 1,
                                    'invoice_date': 1,
                                    'vendor': 1
                                }
                            }
                        ],
                        'as': 'ap_inv_details'
                    }
                }, {
                    '$unwind': {
                        'path': '$ap_inv_details',
                        'preserveNullAndEmptyArrays': True
                    }
                }, {
                    '$lookup': {
                        'from': 'entity_information',
                        'let': {
                            'vendor_id': '$ap_inv_details.vendor'
                        },
                        'pipeline': [
                            {
                                '$match': {
                                    '$expr': {
                                        '$eq': [
                                            '$_id', '$$vendor_id'
                                        ]
                                    }
                                }
                            }, {
                                '$project': {
                                    'entity_name': 1
                                }
                            }
                        ],
                        'as': 'vendor_details'
                    }
                }, {
                    '$unwind': {
                        'path': '$vendor_details',
                        'preserveNullAndEmptyArrays': True
                    }
                }, {
                    '$lookup': {
                        'from': 'ap_invoices_items',
                        'let': {
                            'ap_inv_id': '$ap_inv_details._id'
                        },
                        'pipeline': [
                            {
                                '$match': {
                                    '$expr': {
                                        '$eq': [
                                            '$ap_invoice_id', '$$ap_inv_id'
                                        ]
                                    }
                                }
                            }, {
                                '$project': {
                                    'amount': 1
                                }
                            }
                        ],
                        'as': 'ap_invoice_items_details'
                    }
                }, {
                    '$addFields': {
                        'amounts': {
                            '$sum': '$ap_invoice_items_details.amount'
                        },
                        'payment_amount': {
                            '$ifNull': [
                                '$amount', 0
                            ]
                        },
                        'invoice_date_str': {
                            '$dateToString': {
                                'format': '%d-%m-%Y',
                                'date': '$ap_inv_details.invoice_date'
                            }
                        }
                    }
                }, {
                    '$addFields': {
                        'outstanding_amount': {
                            '$subtract': [
                                '$amounts', {
                                    '$ifNull': [
                                        '$total_paid_for_payment', 0
                                    ]
                                }
                            ]
                        }
                    }
                }, {
                    '$addFields': {
                        '_id': '$payment_invoice_id'
                    }
                }, {
                    '$project': {
                        '_id': 1,
                        'is_selected': {
                            '$literal': True
                        },
                        'payment_id': 1,
                        'ap_invoice_id': '$ap_inv_details._id',
                        'invoice_number': '$ap_inv_details.invoice_number',
                        'invoice_date': '$invoice_date_str',
                        'invoice_amount': {
                            '$ifNull': [
                                '$amounts', 0
                            ]
                        },
                        'payment_amount': 1,
                        'outstanding_amount': 1,
                        'notes': {
                            '$concat': [
                                'Invoice Number: ', {
                                    '$ifNull': [
                                        '$ap_inv_details.invoice_number', ''
                                    ]
                                }, ', Invoice Date: ', '$invoice_date_str', ', Vendor: ', {
                                    '$ifNull': [
                                        '$vendor_details.entity_name', ''
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
            'vendor_name': {
                '$ifNull': [
                    '$vendor_details.entity_name', None
                ]
            },
            'payment_type_name': {
                '$ifNull': [
                    '$payment_type_details.name', None
                ]
            },
            'account_number': {
                '$ifNull': [
                    '$account_details.account_number', None
                ]
            }
        }
    }, {
        '$project': {
            'vendor_details': 0,
            'payment_type_details': 0,
            'account_details': 0
        }
    }
]


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
    ap_invoices_id: Optional[str] = None
    amount: Optional[float] = None
    payment_id: Optional[str] = None
    is_added: Optional[bool] = None
    is_modified: Optional[bool] = None
    is_deleted: Optional[bool] = None


class PaymentModel(BaseModel):
    status: Optional[str] = None
    payment_date: Optional[datetime] = None
    vendor: Optional[str] = None
    note: Optional[str] = None
    payment_type: Optional[str] = None
    cheque_number: Optional[str] = None
    cheque_date: Optional[datetime] = None
    account: Optional[str] = None
    currency: Optional[str] = None
    rate: Optional[float] = None
    invoices: Optional[List[Invoices]] = None


class PaymentSearch(BaseModel):
    payment_number: Optional[str] = None
    payment_type: Optional[PyObjectId] = None
    vendor_name: Optional[PyObjectId] = None
    account: Optional[PyObjectId] = None
    cheque_number: Optional[str] = None
    status: Optional[str] = None
    from_date: Optional[datetime] = None
    to_date: Optional[datetime] = None
    all: Optional[bool] = False
    today: Optional[bool] = False
    this_month: Optional[bool] = False
    this_year: Optional[bool] = False


@router.get("/get_all_vendor_invoices/{vendor_id}")
async def get_all_vendor_invoices(vendor_id: str, data: dict = Depends(security.get_current_user)):
    try:
        vendor_id = ObjectId(vendor_id)
        company_id = ObjectId(data.get("company_id"))
        vendor_invoices_pipeline = [
            {
                '$match': {
                    'company_id': company_id,
                    'vendor': vendor_id,
                    'status': 'Posted'
                }
            }, {
                '$lookup': {
                    'from': 'ap_invoices_items',
                    'localField': '_id',
                    'foreignField': 'ap_invoice_id',
                    'as': 'invoice_items_details'
                }
            }, {
                '$addFields': {
                    'amounts': {
                        '$sum': {
                            '$map': {
                                'input': '$invoice_items_details',
                                'as': 'item',
                                'in': {
                                    '$ifNull': [
                                        '$$item.amount', 0
                                    ]
                                }
                            }
                        }
                    }
                }
            }, {
                '$lookup': {
                    'from': 'all_payments_invoices',
                    'localField': '_id',
                    'foreignField': 'ap_invoices_id',
                    'as': 'payments_invoices_details'
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
                '$addFields': {
                    'given': {
                        '$sum': {
                            '$map': {
                                'input': '$payments_invoices_details',
                                'as': 'payment',
                                'in': {
                                    '$ifNull': [
                                        '$$payment.amount', 0
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
                            '$amounts', '$given'
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
                    'ap_invoice_id': '$_id',
                    'invoice_number': 1,
                    'invoice_date': {
                        '$dateToString': {
                            'format': '%d-%m-%Y',
                            'date': '$invoice_date'
                        }
                    },
                    'invoice_amount': {
                        '$ifNull': [
                            '$amounts', None
                        ]
                    },
                    'payment_amount': {
                        '$ifNull': [
                            '$given', None
                        ]
                    },
                    'outstanding_amount': {
                        '$ifNull': [
                            '$final_outstanding', None
                        ]
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
                            }, ', ', 'Vendor: ', {
                                '$ifNull': [
                                    '$vendor_details.entity_name', ''
                                ]
                            }
                        ]
                    }
                }
            }
        ]

        cursor = await ap_invoices_collection.aggregate(vendor_invoices_pipeline)
        results = await cursor.to_list(None)
        serialized = [serializer(r) for r in results]
        return {"invoices": serialized}

    except HTTPException:
        raise
    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")


async def get_payment_details(receipt_id: ObjectId):
    new_pipeline = ap_payment_details_pipeline.copy()
    new_pipeline.insert(0, {
        "$match": {
            "_id": receipt_id
        }
    })
    cursor = await ap_payment_collection.aggregate(new_pipeline)
    result = await cursor.next()
    return result


@router.post("/add_new_payment")
async def add_new_payment(
        payment: PaymentModel,
        data: dict = Depends(security.get_current_user)
):
    async with database.client.start_session() as session:
        try:
            await session.start_transaction()
            company_id = ObjectId(data.get("company_id"))
            new_payment_counter = await create_custom_counter("PN", "P", data, session)

            payment_dict = payment.model_dump(exclude_unset=True)
            payment_invoices = payment_dict.pop("invoices", None)

            id_fields = ["vendor", "payment_type", "account"]
            for field in id_fields:
                if payment_dict.get(field):
                    payment_dict[field] = ObjectId(payment_dict[field]) if payment_dict[field] else None

            payment_dict["company_id"] = company_id
            payment_dict["createdAt"] = security.now_utc()
            payment_dict["updatedAt"] = security.now_utc()
            payment_dict["payment_number"] = new_payment_counter['final_counter'] if new_payment_counter[
                'success'] else None
            result = await ap_payment_collection.insert_one(payment_dict, session=session)
            if not result.inserted_id:
                raise HTTPException(status_code=500, detail="Failed to insert payment")

            if payment_invoices:
                for inv in payment_invoices:
                    print(inv)
                    if inv.get("ap_invoices_id"):
                        inv["ap_invoices_id"] = ObjectId(inv["ap_invoices_id"])
                        inv["createdAt"] = security.now_utc()
                        inv["updatedAt"] = security.now_utc()
                        inv['company_id'] = company_id
                        inv['payment_id'] = result.inserted_id
                        inv.pop("id", None)
                        inv.pop("is_added", None)
                        inv.pop("is_deleted", None)
                        inv.pop("is_modified", None)

            else:
                payment_invoices = []

            if payment_invoices:
                new_invoices = await ap_payment_invoices_collection.insert_many(payment_invoices, session=session)
                if not new_invoices.inserted_ids:
                    raise HTTPException(status_code=500, detail="Failed to insert receipt invoices")

            await session.commit_transaction()
            new_payment = await get_payment_details(result.inserted_id)
            serialized = serializer(new_payment)
            return {"payment": serialized}

        except Exception as e:
            await session.abort_transaction()
            raise HTTPException(status_code=500, detail=str(e))


@router.patch("/update_payment_invoices")
async def update_payment_invoices(
        items: list[Invoices],
        data: dict = Depends(security.get_current_user)
):
    async with  database.client.start_session() as s:
        try:
            await s.start_transaction()
            company_id = ObjectId(data["company_id"])
            items = [item.model_dump(exclude_unset=True) for item in items]
            print(items)
            payment_id = ObjectId(items[0].get('payment_id', None)) if items else None

            added_list = []
            deleted_list = []
            modified_list = []
            updated_list = []

            for item in items:
                if item.get("is_deleted"):
                    print('yes')
                    if not item.get('id'):
                        continue
                    deleted_list.append(ObjectId(item["id"]))

                elif item.get("is_added") and not item.get("is_deleted"):
                    item.pop("id", None)
                    item['payment_id'] = ObjectId(item['payment_id']) if item['payment_id'] else None
                    item['company_id'] = company_id
                    item['ap_invoices_id'] = ObjectId(item['ap_invoices_id']) if item['ap_invoices_id'] else None
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
                    item["updatedAt"] = security.now_utc()
                    if item.get("ap_invoices_id"):
                        item.pop("ap_invoices_id", None)
                    if "payment_id" in item:
                        item.pop("payment_id", None)
                    item["amount"] = item["amount"] if item["amount"] else None
                    item.pop("is_deleted", None)
                    item.pop("is_added", None)
                    item.pop("is_modified", None)
                    modified_list.append((item_id, item))

                if deleted_list:
                    print(deleted_list)
                    await ap_payment_invoices_collection.delete_many(
                        {"_id": {"$in": deleted_list}}, session=s
                    )

                if added_list:
                    added_invoices = await ap_payment_invoices_collection.insert_many(
                        added_list, session=s
                    )
                    inserted_ids = added_invoices.inserted_ids
                    for item, new_id in zip(added_list, inserted_ids):
                        response_item = {
                            "_id": str(new_id),
                            "ap_invoice_id": str(item.get("ap_invoice_id")),
                        }
                        updated_list.append(response_item)

                for item_id, item_data in modified_list:
                    item_data.pop("id", None)
                    await ap_payment_invoices_collection.update_one(
                        {"_id": item_id},
                        {"$set": item_data},
                        session=s
                    )
                    updated_list.append(
                        {"_id": str(item_id),
                         "ap_invoice_id": str(item_data["ap_invoice_id"]) if item_data.get("ap_invoice_id") else None})

                await s.commit_transaction()
            if payment_id:
                new_payment = await get_payment_details(payment_id)
                serialized = serializer(new_payment)
                return {"payment": serialized}
            else:
                return {"payment": {}}

        except Exception as e:
            print(e)
            await s.abort_transaction()
            raise HTTPException(status_code=500, detail=str(e))


@router.patch("/update_ap_payment/{payment_id}")
async def update_ar_receipt(payment_id: str, payment: PaymentModel, _: dict = Depends(security.get_current_user)):
    try:
        payment_id = ObjectId(payment_id)
        payment_data_dict = payment.model_dump(exclude_unset=True)

        id_fields = ["vendor", "payment_type", "account"]
        for field in id_fields:
            if payment_data_dict.get(field):
                payment_data_dict[field] = ObjectId(payment_data_dict[field])

        payment_data_dict.update({
            "updatedAt": security.now_utc(),
        })
        result = await ap_payment_collection.update_one({"_id": payment_id}, {"$set": payment_data_dict})
        if result.modified_count == 0:
            raise HTTPException(status_code=404)

    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/get_ap_payment_status/{payment_id}")
async def get_ap_payment_status(payment_id: str, _: dict = Depends(security.get_current_user)):
    try:
        if not ObjectId.is_valid(payment_id):
            raise HTTPException(status_code=400, detail="Invalid payment id format")

        payment_id = ObjectId(payment_id)

        result = await ap_payment_collection.find_one(
            {"_id": payment_id},
            {"_id": 0, "status": 1}
        )

        if not result:
            raise HTTPException(status_code=404, detail="AP Payment not found")

        return {"status": "success", "data": result}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")


@router.delete("/delete_payment/{payment_id}")
async def delete_payment(payment_id: str, _: dict = Depends(security.get_current_user)):
    async with database.client.start_session() as session:
        try:
            await session.start_transaction()
            payment_id = ObjectId(payment_id)
            if not payment_id:
                raise HTTPException(status_code=404, detail="Payment ID not found")
            current_payment = await ap_payment_collection.find_one({"_id": payment_id}, session=session)
            if not current_payment:
                raise HTTPException(status_code=404, detail="Receipt not found")
            if current_payment['status'] != "New":
                raise HTTPException(status_code=403, detail="Only New Payments allowed")
            result = await ap_payment_collection.delete_one({"_id": payment_id}, session=session)
            if result.deleted_count == 0:
                raise HTTPException(status_code=404, detail="Payment not found or already deleted")
            await ap_payment_invoices_collection.delete_many({"payment_id": payment_id}, session=session)

            await session.commit_transaction()
            return {"message": "Payment deleted successfully", "payment_id": str(payment_id)}

        except HTTPException:
            await session.abort_transaction()
            raise

        except Exception as e:
            print(e)
            await session.abort_transaction()
            raise HTTPException(status_code=500, detail=f"Delete failed: {str(e)}")


@router.post("/search_engine_for_ap_payments")
async def search_engine_for_ap_payments(
        filter_payments: PaymentSearch,
        data: dict = Depends(security.get_current_user),
):
    try:
        company_id = data.get("company_id")
        if not company_id:
            raise HTTPException(status_code=400, detail="Company ID missing")

        company_id = ObjectId(company_id)
        search_pipeline = copy.deepcopy(ap_payment_details_pipeline)

        match_stage = {}
        if company_id:
            match_stage['company_id'] = company_id
        if filter_payments.account:
            match_stage["account"] = filter_payments.account
        if filter_payments.payment_type:
            match_stage["payment_type"] = filter_payments.payment_type
        if filter_payments.payment_number:
            match_stage["payment_number"] = {
                "$regex": filter_payments.payment_number, "$options": "i"
            }
        if filter_payments.cheque_number:
            match_stage["cheque_number"] = {
                "$regex": filter_payments.cheque_number, "$options": "i"
            }
        if filter_payments.vendor_name:
            match_stage["vendor"] = filter_payments.vendor_name
        if filter_payments.status:
            match_stage["status"] = filter_payments.status

        # 2️⃣ Handle date filters
        now = datetime.now(timezone.utc)
        date_field = "payment_date"
        date_filter = {}

        if filter_payments.today:
            start = datetime(now.year, now.month, now.day, tzinfo=timezone.utc)
            end = start + timedelta(days=1)
            date_filter[date_field] = {"$gte": start, "$lt": end}

        elif filter_payments.this_month:
            start = datetime(now.year, now.month, 1, tzinfo=timezone.utc)
            if now.month == 12:
                end = datetime(now.year + 1, 1, 1, tzinfo=timezone.utc)
            else:
                end = datetime(now.year, now.month + 1, 1, tzinfo=timezone.utc)
            date_filter[date_field] = {"$gte": start, "$lt": end}

        elif filter_payments.this_year:
            start = datetime(now.year, 1, 1, tzinfo=timezone.utc)
            end = datetime(now.year + 1, 1, 1, tzinfo=timezone.utc)
            date_filter[date_field] = {"$gte": start, "$lt": end}

        elif filter_payments.from_date or filter_payments.to_date:
            date_filter[date_field] = {}
            if filter_payments.from_date:
                date_filter[date_field]["$gte"] = filter_payments.from_date
            if filter_payments.to_date:
                date_filter[date_field]["$lte"] = filter_payments.to_date

        # Merge both filters into one $match
        if date_filter:
            match_stage.update(date_filter)

        search_pipeline.insert(0, {"$match": match_stage})

        # 3️⃣ Add computed field
        search_pipeline.append({
            "$addFields": {
                "total_given": {"$sum": "$invoices_details.payment_amount"}
            }
        })
        search_pipeline.append({
            "$facet": {
                "payments": [
                    {"$sort": {"payment_number": -1}},
                ],
                "grand_totals": [
                    {
                        "$group": {
                            "_id": None,
                            "grand_given": {"$sum": "$total_given"},
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

        cursor = await ap_payment_collection.aggregate(search_pipeline)
        result = await cursor.to_list(None)

        if result and len(result) > 0:
            data = result[0]
            payments = [serializer(r) for r in data.get("payments", [])]
            totals = data.get("grand_totals", [])
            grand_totals = totals[0] if totals else {"grand_given": 0}
        else:
            payments = []
            grand_totals = {"grand_given": 0} # fixed

        return {
            "payments": payments,
            "grand_totals": grand_totals
        }


    except HTTPException:
        raise
    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")

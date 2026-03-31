import asyncio
from typing import Optional, Any
from bson import ObjectId
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from starlette import status
from app import database
from app.core import security
from app.database import get_collection
from datetime import datetime
from app.routes.counters import create_custom_counter

router = APIRouter()
batch_payment_process_collection = get_collection("batch_payment_process")
batch_payment_process_items_collection = get_collection("batch_payment_process_items")
ap_invoices_collection = get_collection("ap_invoices")
ap_invoices_items_collection = get_collection("ap_invoices_items")
ap_payment_collection = get_collection("all_payments")
ap_payment_invoices_collection = get_collection("all_payments_invoices")


class BatchPaymentProcessModel(BaseModel):
    batch_date: Optional[datetime] = None
    note: Optional[str] = None
    payment_type: Optional[str] = None
    cheque_number: Optional[str] = None
    cheque_date: Optional[datetime] = None
    account: Optional[str] = None
    currency: Optional[str] = None
    rate: Optional[float] = None
    status: Optional[str] = None


class BatchPaymentProcessItemModel(BaseModel):
    transaction_type: Optional[str] = None
    received_number: Optional[str] = None
    vendor: Optional[str] = None
    amount: Optional[float] = None
    vat: Optional[float] = None
    invoice_number: Optional[str] = None
    invoice_date: Optional[datetime] = None
    job_number_id: Optional[str] = None
    note: Optional[str] = None


class BatchSearch(BaseModel):
    batch_number: Optional[str] = None
    note: Optional[str] = None
    status: Optional[str] = None
    from_date: Optional[datetime] = None
    to_date: Optional[datetime] = None


@router.post("/add_new_batch_payment_process")
async def add_new_batch_payment_process(batch_model: BatchPaymentProcessModel,
                                        data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        batch_model = batch_model.model_dump(exclude_unset=True)
        new_batch_counter = await create_custom_counter("BPC", "B", data=data,
                                                        description='Batch Payment Process Number')

        batch_dict = {
            "company_id": company_id,
            "batch_date": batch_model['batch_date'],
            "note": batch_model['note'],
            "payment_type": ObjectId(batch_model['payment_type']) if batch_model['payment_type'] else None,
            "cheque_number": batch_model['cheque_number'],
            "cheque_date": batch_model['cheque_date'],
            "account": ObjectId(batch_model['account']) if batch_model['account'] else None,
            "currency": batch_model['currency'],
            "rate": batch_model['rate'],
            "status": "New",
            "batch_number": new_batch_counter['final_counter'] if new_batch_counter['success'] else None,
        }
        new_batch = await batch_payment_process_collection.insert_one(batch_dict)
        new_batch_id = new_batch.inserted_id
        return {"_id": str(new_batch_id),
                "batch_number": new_batch_counter['final_counter'] if new_batch_counter['success'] else None,
                "status": "New"}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.patch("/update_batch_payment_process/{batch_id}")
async def update_batch_payment_process(batch_id: str, batch_model: BatchPaymentProcessModel,
                                       data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        batch_id = ObjectId(batch_id)
        batch_model = batch_model.model_dump(exclude_unset=True)

        batch_dict = {
            "company_id": company_id,
            "batch_date": batch_model['batch_date'],
            "note": batch_model['note'],
            "payment_type": ObjectId(batch_model['payment_type']) if batch_model['payment_type'] else None,
            "cheque_number": batch_model['cheque_number'],
            "cheque_date": batch_model['cheque_date'],
            "account": ObjectId(batch_model['account']) if batch_model['account'] else None,
            "currency": batch_model['currency'],
            "rate": batch_model['rate'],
            "status": batch_model['status'],
        }
        await batch_payment_process_collection.update_one({"_id": batch_id}, {"$set": batch_dict})


    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/get_batch_payment_process_details/{batch_id}")
async def get_batch_payment_process_details(batch_id: str, _: dict = Depends(security.get_current_user)):
    try:
        if not ObjectId.is_valid(batch_id):
            raise HTTPException(
                status_code=400,
                detail="Invalid batch_id format"
            )
        details_pipeline = [
            {
                "$match": {
                    "_id": ObjectId(batch_id)
                }
            },
            {
                '$lookup': {
                    'from': 'all_lists_values',
                    'let': {
                        'payment_type_id': '$payment_type'
                    },
                    'pipeline': [
                        {
                            '$match': {
                                '$expr': {
                                    '$eq': [
                                        '$_id', '$$payment_type_id'
                                    ]
                                }
                            }
                        }, {
                            '$project': {
                                'name': 1
                            }
                        }
                    ],
                    'as': 'payment_details'
                }
            }, {
                '$lookup': {
                    'from': 'all_banks',
                    'let': {
                        'account_id': '$account'
                    },
                    'pipeline': [
                        {
                            '$match': {
                                '$expr': {
                                    '$eq': [
                                        '$_id', '$$account_id'
                                    ]
                                }
                            }
                        }, {
                            '$project': {
                                'account_number': 1
                            }
                        }
                    ],
                    'as': 'account_details'
                }
            }, {
                '$lookup': {
                    'from': 'batch_payment_process_items',
                    'let': {
                        'batchId': '$_id'
                    },
                    'pipeline': [
                        {
                            '$match': {
                                '$expr': {
                                    '$eq': [
                                        '$batch_id', '$$batchId'
                                    ]
                                }
                            }
                        }, {
                            '$lookup': {
                                'from': 'ap_payment_types',
                                'let': {
                                    'type_id': '$transaction_type'
                                },
                                'pipeline': [
                                    {
                                        '$match': {
                                            '$expr': {
                                                '$eq': [
                                                    '$_id', '$$type_id'
                                                ]
                                            }
                                        }
                                    }, {
                                        '$project': {
                                            'type': 1
                                        }
                                    }
                                ],
                                'as': 'transaction_type_details'
                            }
                        }, {
                            '$lookup': {
                                'from': 'job_cards',
                                'let': {
                                    'job_id': '$job_number_id'
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
                                            'job_number': 1
                                        }
                                    }
                                ],
                                'as': 'job_details'
                            }
                        }, {
                            '$lookup': {
                                'from': 'receiving',
                                'let': {
                                    'rec_id': '$received_number'
                                },
                                'pipeline': [
                                    {
                                        '$match': {
                                            '$expr': {
                                                '$eq': [
                                                    '$_id', '$$rec_id'
                                                ]
                                            }
                                        }
                                    }, {
                                        '$project': {
                                            'receiving_number': 1
                                        }
                                    }
                                ],
                                'as': 'received_number_details'
                            }
                        }, {
                            '$lookup': {
                                'from': 'entity_information',
                                'let': {
                                    'vendor_id': '$vendor'
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
                            '$set': {
                                '_id': {
                                    '$toString': '$_id'
                                },
                                'batch_id': {
                                    '$toString': '$batch_id'
                                },
                                'company_id': {
                                    '$toString': '$company_id'
                                },
                                'transaction_type': {
                                    '$toString': '$transaction_type'
                                },
                                'received_number': {
                                    '$toString': '$received_number'
                                },
                                'vendor': {
                                    '$toString': '$vendor'
                                },
                                'job_number_id': {
                                    '$toString': '$job_number_id'
                                },
                                'transaction_type_name': {
                                    '$first': '$transaction_type_details.type'
                                },
                                'job_number': {
                                    '$first': '$job_details.job_number'
                                },
                                'receiving_number': {
                                    '$first': '$received_number_details.receiving_number'
                                },
                                'vendor_name': {
                                    '$first': '$vendor_details.entity_name'
                                }
                            }
                        }, {
                            '$project': {
                                'transaction_type_details': 0,
                                'job_details': 0,
                                'received_number_details': 0,
                                'vendor_details': 0
                            }
                        }
                    ],
                    'as': 'items_details'
                }
            }, {
                '$set': {
                    'account_name': {
                        '$first': '$account_details.account_number'
                    },
                    'payment_type_name': {
                        '$first': '$payment_details.name'
                    },
                    '_id': {
                        '$toString': '$_id'
                    },
                    'company_id': {
                        '$toString': '$company_id'
                    },
                    'payment_type': {
                        '$toString': '$payment_type'
                    },
                    'account': {
                        '$toString': '$account'
                    }
                }
            }, {
                '$project': {
                    'payment_details': 0,
                    'account_details': 0
                }
            }
        ]
        cursor = await batch_payment_process_collection.aggregate(details_pipeline)
        result = await cursor.to_list(length=1)
        if not result:
            raise HTTPException(status_code=404, detail="Batch payment process not found")
        return {
            "batch_details": result[0]
        }

    except HTTPException:
        raise
    except Exception:
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/get_batch_status/{batch_id}")
async def get_batch_status(batch_id: str, _: dict = Depends(security.get_current_user)):
    try:
        if not ObjectId.is_valid(batch_id):
            raise HTTPException(status_code=400, detail="Invalid batch_id format")

        batch_id = ObjectId(batch_id)

        result = await batch_payment_process_collection.find_one(
            {"_id": batch_id},
            {"_id": 0, "status": 1}
        )

        if not result:
            raise HTTPException(status_code=404, detail="Job card not found")

        return {"status": "success", "data": result}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")


@router.post("/add_batch_item/{batch_id}")
async def add_batch_item(batch_id: str, batch_item_model: BatchPaymentProcessItemModel,
                         data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        batch_id = ObjectId(batch_id)
        item_model = batch_item_model.model_dump(exclude_unset=True)
        item_dict = {
            "batch_id": batch_id,
            "company_id": company_id,
            "transaction_type": ObjectId(item_model['transaction_type']) if item_model['transaction_type'] else None,
            "received_number": ObjectId(item_model['received_number']) if item_model['received_number'] else None,
            "vendor": ObjectId(item_model['vendor']) if item_model['vendor'] else None,
            "amount": item_model['amount'],
            "vat": item_model['vat'],
            "invoice_number": item_model['invoice_number'],
            "invoice_date": item_model['invoice_date'],
            "job_number_id": ObjectId(item_model['job_number_id']) if item_model['job_number_id'] else None,
            "note": item_model['note'],
            "createdAt": security.now_utc(),
            "updatedAt": security.now_utc(),
        }
        new_batch_item = await batch_payment_process_items_collection.insert_one(item_dict)
        new_batch_item_id = new_batch_item.inserted_id
        return {"_id": str(new_batch_item_id), }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.patch("/update_batch_item/{batch_id}")
async def update_batch_item(batch_id: str, batch_item_model: BatchPaymentProcessItemModel,
                            _: dict = Depends(security.get_current_user)):
    try:
        batch_id = ObjectId(batch_id)
        item_model = batch_item_model.model_dump(exclude_unset=True)
        item_dict = {
            "transaction_type": ObjectId(item_model['transaction_type']) if item_model['transaction_type'] else None,
            "received_number": ObjectId(item_model['received_number']) if item_model['received_number'] else None,
            "vendor": ObjectId(item_model['vendor']) if item_model['vendor'] else None,
            "amount": item_model['amount'],
            "vat": item_model['vat'],
            "invoice_number": item_model['invoice_number'],
            "invoice_date": item_model['invoice_date'],
            "job_number_id": ObjectId(item_model['job_number_id']) if item_model['job_number_id'] else None,
            "note": item_model['note'],
            "updatedAt": security.now_utc(),
        }
        await batch_payment_process_items_collection.update_one({"_id": batch_id}, {"$set": item_dict})

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/delete_batch_item/{item_id}", status_code=status.HTTP_200_OK)
async def delete_batch_item(
        item_id: str,
        _: dict = Depends(security.get_current_user)
):
    try:
        if not ObjectId.is_valid(item_id):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid item_id format"
            )
        obj_id = ObjectId(item_id)
        result = await batch_payment_process_items_collection.delete_one({"_id": obj_id})
        if result.deleted_count == 0:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Batch item not found")
        return {"message": "Batch item deleted successfully", "_id": item_id}
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Internal server error")


@router.post("/search_engine_for_batch_payment_process")
async def search_engine_for_batch_payment_process(filter_batch: BatchSearch,
                                                  data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        match_stage: Any = {}
        if company_id:
            match_stage = {"company_id": company_id}
        if filter_batch.from_date or filter_batch.to_date:
            match_stage['batch_date'] = {}
            if filter_batch.from_date:
                match_stage['batch_date']["$gte"] = filter_batch.from_date
            if filter_batch.to_date:
                match_stage['batch_date']["$lte"] = filter_batch.to_date

        if filter_batch.status:
            match_stage["status"] = filter_batch.status
        if filter_batch.batch_number:
            match_stage["batch_number"] = filter_batch.batch_number
        if filter_batch.note:
            match_stage["note"] = {"$regex": filter_batch.note, "$options": "i"}

        batch_search_pipeline = [
            {"$match": match_stage},
            {"$sort": {'batch_date': -1}},
            {
                "$project": {
                    "_id": {"$toString": "$_id"},
                    "batch_date": 1,
                    "note": 1,
                    "status": 1,
                    "batch_number": 1
                }
            }
        ]
        cursor = await batch_payment_process_collection.aggregate(batch_search_pipeline)
        batches = await cursor.to_list(None)
        return {
            "batches": batches if batches else [],
        }


    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/post_all_new_batch_payment_process")
async def post_all_new_batch_payment_process(data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))

        # GET ALL NEW BATCHES
        new_batches = await batch_payment_process_collection.find({
            "company_id": company_id,
            "status": "New"
        }).to_list(length=None)

        if not new_batches:
            return {"message": "No new batches found"}

        for i, row in enumerate(new_batches, start=1):
            if row['_id']:
                await post_batch(str(row['_id']), data=data)
                print(i)

        return {"message": f"{len(new_batches)} batches processed successfully"}

    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/post_batch/{batch_id}")
async def post_batch(batch_id: str, data: dict = Depends(security.get_current_user)):
    async with database.client.start_session() as session:
        try:
            await session.start_transaction()
            company_id = ObjectId(data.get("company_id"))
            batch_id = ObjectId(batch_id)
            current_batch = await get_batch_payment_process_details(str(batch_id))
            current_batch_details = current_batch['batch_details']
            items_details = current_batch_details.get('items_details', [])
            if items_details:
                for item in items_details:
                    new_invoice_counter = await create_custom_counter("APIN", "AI", description='AP Invoice Number',
                                                                      data=data,
                                                                      session=session)
                    ap_invoice_dict = {
                        "batch_id": batch_id,
                        "company_id": company_id,
                        "reference_number": new_invoice_counter['final_counter'] if new_invoice_counter[
                            'success'] else None,
                        "status": 'Posted',
                        "transaction_date": current_batch_details.get('batch_date', None),
                        "invoice_date": item.get('invoice_date', None),
                        "invoice_type": None,
                        "invoice_number": item.get('invoice_number', None),
                        "vendor": ObjectId(item['vendor']) if 'vendor' in item else None,
                        "description": current_batch_details.get('note', ''),
                        "createdAt": security.now_utc(),
                        "updatedAt": security.now_utc(),
                    }
                    new_ap_invoice = await ap_invoices_collection.insert_one(ap_invoice_dict, session=session)
                    ap_invoice_id = new_ap_invoice.inserted_id

                    ap_invoice_item_dict = {
                        "company_id": company_id,
                        "ap_invoice_id": ap_invoice_id,
                        "transaction_type": ObjectId(item['transaction_type']) if 'transaction_type' in item else None,
                        "amount": item.get('amount', 0),
                        "vat": item.get('vat', 0),
                        "job_number_id": ObjectId(item['job_number_id']) if 'job_number_id' in item else None,
                        "received_number_id": ObjectId(item['received_number']) if 'received_number' in item else None,
                        "note": item.get('note', None),
                        "createdAt": security.now_utc(),
                        "updatedAt": security.now_utc(),
                    }
                    await ap_invoices_items_collection.insert_one(ap_invoice_item_dict, session=session)

                    new_payment_counter = await create_custom_counter("PN", "P", description='AP Payments Number',
                                                                      data=data,
                                                                      session=session)

                    ap_payment_dict = {
                        "batch_id": batch_id,
                        "company_id": company_id,
                        "payment_type": ObjectId(
                            current_batch_details['payment_type']) if 'payment_type' in current_batch_details else None,
                        "payment_date": current_batch_details.get('batch_date', None),
                        "status": 'Posted',
                        "vendor": ObjectId(item['vendor']) if 'vendor' in item else None,
                        "note": current_batch_details.get('note', ''),
                        "cheque_number": current_batch_details.get('cheque_number', None),
                        "account": ObjectId(
                            current_batch_details['account']) if 'account' in current_batch_details else None,
                        "currency": current_batch_details.get('currency', ""),
                        "rate": current_batch_details.get('rate', 1),
                        "cheque_date": current_batch_details.get('cheque_date', None),
                        "payment_number": new_payment_counter['final_counter'] if new_payment_counter[
                            'success'] else None,
                        "createdAt": security.now_utc(),
                        "updatedAt": security.now_utc(),
                    }
                    new_payment = await ap_payment_collection.insert_one(ap_payment_dict, session=session)

                    ap_payment_invoice_dict = {
                        "company_id": company_id,
                        "ap_invoices_id": new_ap_invoice.inserted_id,
                        "amount": item.get('amount', 0),
                        "payment_id": new_payment.inserted_id,
                        "createdAt": security.now_utc(),
                        "updatedAt": security.now_utc(),
                    }
                    await ap_payment_invoices_collection.insert_one(ap_payment_invoice_dict, session=session)
            await batch_payment_process_collection.update_one({"_id": batch_id}, {
                "$set": {"status": "Posted", "updatedAt": security.now_utc()}}, session=session)
            await session.commit_transaction()
            return {"status": "Posted"}


        except Exception as e:
            await session.abort_transaction()
            raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")

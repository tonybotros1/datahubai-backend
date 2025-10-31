import copy
from typing import Optional, List, Any
from bson import ObjectId
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel, ValidationError
from app import database
from app.core import security
from app.database import get_collection
from datetime import datetime, timezone, timedelta
from app.routes.car_trading import PyObjectId
from app.routes.counters import create_custom_counter

router = APIRouter()
ap_invoices_collection = get_collection("ap_invoices")
ap_invoices_items_collection = get_collection("ap_invoices_items")


class InvoiceItem(BaseModel):
    id: Optional[str] = None
    uuid: Optional[str] = None
    transaction_type: Optional[str] = None
    amount: Optional[float] = None
    vat: Optional[float] = None
    job_number: Optional[str] = None
    note: Optional[str] = None
    is_added: Optional[bool] = None
    is_deleted: Optional[bool] = None
    is_modified: Optional[bool] = None
    ap_invoice_id: Optional[str] = None


class APInvoicesModel(BaseModel):
    status: Optional[str] = None
    invoice_type: Optional[str] = None
    transaction_date: Optional[datetime] = None
    invoice_number: Optional[str] = None
    invoice_date: Optional[datetime] = None
    vendor: Optional[str] = None
    description: Optional[str] = None
    items: List[InvoiceItem] = None


class APInvoicesSearch(BaseModel):
    invoice_type: Optional[PyObjectId] = None
    reference_number: Optional[str] = None
    vendor: Optional[PyObjectId] = None
    status: Optional[str] = None
    from_date: Optional[datetime] = None
    to_date: Optional[datetime] = None
    all: Optional[bool] = False
    today: Optional[bool] = False
    this_month: Optional[bool] = False
    this_year: Optional[bool] = False


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


pipeline: List[dict[str, Any]] = [
    {
        '$lookup': {
            'from': 'all_lists_values',
            'localField': 'invoice_type',
            'foreignField': '_id',
            'as': 'invoice_type_details'
        }
    }, {
        '$unwind': {
            'path': '$invoice_type_details',
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
            'from': 'ap_invoices_items',
            'let': {
                'payment_id': '$_id'
            },
            'pipeline': [
                {
                    '$match': {
                        '$expr': {
                            '$eq': [
                                '$ap_invoice_id', '$$payment_id'
                            ]
                        }
                    }
                }, {
                    '$lookup': {
                        'from': 'ap_payment_types',
                        'localField': 'transaction_type',
                        'foreignField': '_id',
                        'as': 'transaction_type_details'
                    }
                }, {
                    '$unwind': {
                        'path': '$transaction_type_details',
                        'preserveNullAndEmptyArrays': True
                    }
                }, {
                    '$addFields': {
                        'transaction_type_name': {
                            '$ifNull': [
                                '$transaction_type_details.type', None
                            ]
                        }
                    }
                }, {
                    '$project': {
                        'transaction_type_details': 0
                    }
                }
            ],
            'as': 'items'
        }
    }, {
        '$addFields': {
            'vendor_name': {
                '$ifNull': [
                    '$vendor_details.entity_name', None
                ]
            },
            'invoice_type_name': {
                '$ifNull': [
                    '$invoice_type_details.name', None
                ]
            }
        }
    }, {
        '$project': {
            'vendor_details': 0,
            'invoice_type_details': 0
        }
    }
]


async def get_ap_invoice_details(invoice_id: ObjectId):
    try:
        new_pipeline = copy.deepcopy(pipeline)
        new_pipeline.insert(1, {
            "$match": {
                "_id": invoice_id
            }
        })
        cursor = await ap_invoices_collection.aggregate(new_pipeline)
        result = await cursor.next()
        return result

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/add_new_ap_invoice")
async def add_new_ap_invoice(invoices: APInvoicesModel, data: dict = Depends(security.get_current_user)):
    async with database.client.start_session() as session:
        try:
            await session.start_transaction()
            company_id = ObjectId(data.get("company_id"))
            new_invoice_counter = await create_custom_counter("APIN", "AI", data, session)
            invoices_dict = invoices.model_dump(exclude_unset=True)
            invoice_items = invoices_dict.pop("items", None)

            id_fields = ["invoice_type", "vendor"]
            for field in id_fields:
                if invoices_dict.get(field):
                    invoices_dict[field] = ObjectId(invoices_dict[field]) if invoices_dict[field] else None

            invoices_dict["company_id"] = company_id
            invoices_dict["createdAt"] = security.now_utc()
            invoices_dict["updatedAt"] = security.now_utc()
            invoices_dict["reference_number"] = new_invoice_counter['final_counter'] if new_invoice_counter[
                'success'] else None
            result = await ap_invoices_collection.insert_one(invoices_dict, session=session)
            if not result.inserted_id:
                raise HTTPException(status_code=500, detail="Failed to insert ap invoice")

            if invoice_items:
                for inv in invoice_items:
                    inv["createdAt"] = security.now_utc()
                    inv["updatedAt"] = security.now_utc()
                    inv['company_id'] = company_id
                    inv['ap_invoice_id'] = result.inserted_id
                    inv.pop("id", None)
                    inv.pop("uuid", None)
                    inv.pop("is_added", None)
                    inv.pop("is_deleted", None)
                    inv.pop("is_modified", None)

            else:
                invoice_items = []

            if invoice_items:
                new_invoices = await ap_invoices_items_collection.insert_many(invoice_items, session=session)
                if not new_invoices.inserted_ids:
                    raise HTTPException(status_code=500, detail="Failed to insert ap invoice items")

            await session.commit_transaction()
            new_receipt = await get_ap_invoice_details(result.inserted_id)
            serialized = serializer(new_receipt)
            return {"invoice": serialized}

        except Exception as e:
            await session.abort_transaction()
            raise HTTPException(status_code=500, detail=str(e))


@router.patch("/update_ap_invoice/{invoice_id}")
async def update_ap_invoice(invoice_id: str, invoice: APInvoicesModel, _: dict = Depends(security.get_current_user)):
    try:
        invoice_id = ObjectId(invoice_id)
        invoice_data_dict = invoice.model_dump(exclude_unset=True)
        print(invoice_data_dict)

        id_fields = ["invoice_type", "vendor"]
        for field in id_fields:
            if invoice_data_dict.get(field):
                invoice_data_dict[field] = ObjectId(invoice_data_dict[field])

        invoice_data_dict.update({
            "updatedAt": security.now_utc(),
        })
        result = await ap_invoices_collection.update_one({"_id": invoice_id}, {"$set": invoice_data_dict})
        if result.modified_count == 0:
            raise HTTPException(status_code=404)

    except ValidationError as e:
        print(e)
        raise HTTPException(status_code=422, detail=e.errors())

    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=str(e))


@router.patch("/update_invoice_items")
async def update_invoice_items(
        items: list[InvoiceItem],
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
            if item.get("is_deleted"):
                if "id" not in item:
                    continue
                print('yes deleted')
                print(item['id'])
                deleted_list.append(ObjectId(item["id"]))

            elif item.get("is_added") and not item.get("is_deleted"):
                print('yes added')
                item.pop("id", None)
                item['ap_invoice_id'] = ObjectId(item['ap_invoice_id']) if item['ap_invoice_id'] else None
                item['transaction_type'] = ObjectId(item['transaction_type']) if item['transaction_type'] else None
                item['company_id'] = company_id
                item["createdAt"] = security.now_utc()
                item["updatedAt"] = security.now_utc()
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
                if "ap_invoice_id" in item:
                    item.pop("ap_invoice_id", None)
                item.pop("is_deleted", None)
                item.pop("is_added", None)
                item.pop("is_modified", None)
                modified_list.append((item_id, item))

        async with  database.client.start_session() as s:
            await s.start_transaction()
            if deleted_list:
                await ap_invoices_items_collection.delete_many(
                    {"_id": {"$in": deleted_list}}, session=s
                )

            if added_list:
                added_invoices = await ap_invoices_items_collection.insert_many(
                    added_list, session=s
                )
                inserted_ids = added_invoices.inserted_ids
                for item, new_id in zip(added_list, inserted_ids):
                    response_item = {
                        "_id": str(new_id),
                        "uuid": str(item.get("uuid")),
                    }
                    updated_list.append(response_item)

            for item_id, item_data in modified_list:
                item_data.pop("id", None)
                await ap_invoices_items_collection.update_one(
                    {"_id": item_id},
                    {"$set": item_data},
                    session=s
                )
                updated_list.append(
                    {"_id": str(item_id), "uuid": str(item_data["uuid"]) if item_data.get("uuid") else None})

            await s.commit_transaction()
        return {"updated_items": updated_list, "deleted_items": [str(d) for d in deleted_list]}

    except Exception as e:
        print(e)
        await s.abort_transaction()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/get_ap_invoice_status/{invoice_id}")
async def get_ap_invoice_status(invoice_id: str, _: dict = Depends(security.get_current_user)):
    try:
        if not ObjectId.is_valid(invoice_id):
            raise HTTPException(status_code=400, detail="Invalid invoice_id format")

        invoice_id = ObjectId(invoice_id)

        result = await ap_invoices_collection.find_one(
            {"_id": invoice_id},
            {"_id": 0, "status": 1, }
        )

        if not result:
            raise HTTPException(status_code=404, detail="Job card not found")

        return {"status": "success", "data": result}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")


@router.delete("/delete_ap_invoice/{invoice_id}")
async def delete_ap_invoice(invoice_id: str, _: dict = Depends(security.get_current_user)):
    async with database.client.start_session() as session:
        try:
            await session.start_transaction()
            invoice_id = ObjectId(invoice_id)
            if not invoice_id:
                raise HTTPException(status_code=404, detail="Receipt ID not found")
            current_receipt = await ap_invoices_collection.find_one({"_id": invoice_id}, session=session)
            if not current_receipt:
                raise HTTPException(status_code=404, detail="Invoice not found")
            if current_receipt['status'] != "New":
                raise HTTPException(status_code=403, detail="Only New AP Invoices allowed")
            result = await ap_invoices_collection.delete_one({"_id": invoice_id}, session=session)
            if result.deleted_count == 0:
                raise HTTPException(status_code=404, detail="Invoice not found or already deleted")
            await ap_invoices_items_collection.delete_many({"ap_invoice_id": invoice_id}, session=session)

            await session.commit_transaction()
            return {"message": "Invoice deleted successfully", "invoice_id": str(invoice_id)}

        except HTTPException:
            await session.abort_transaction()
            raise

        except Exception as e:
            print(e)
            await session.abort_transaction()
            raise HTTPException(status_code=500, detail=f"Delete failed: {str(e)}")


@router.post("/search_engine")
async def search_engine(filtered_invoices: APInvoicesSearch, data: dict = Depends(security.get_current_user)):
    try:
        company_id = data.get("company_id")
        if not company_id:
            raise HTTPException(status_code=400, detail="Company ID missing")
        company_id = ObjectId(company_id)
        search_pipeline = copy.deepcopy(pipeline)

        match_stage = {}
        if company_id:
            match_stage['company_id'] = company_id
        if filtered_invoices.invoice_type:
            match_stage["invoice_type"] = filtered_invoices.invoice_type
        if filtered_invoices.reference_number:
            match_stage["reference_number"] = {
                "$regex": filtered_invoices.reference_number, "$options": "i"
            }
        if filtered_invoices.vendor:
            match_stage["vendor"] = filtered_invoices.vendor
        if filtered_invoices.status:
            match_stage["status"] = filtered_invoices.status

        # 2️⃣ Handle date filters
        now = datetime.now()
        date_field = "transaction_date"
        date_filter = {}

        if filtered_invoices.today:
            start = datetime(now.year, now.month, now.day, tzinfo=timezone.utc)
            end = start + timedelta(days=1)
            date_filter[date_field] = {"$gte": start, "$lt": end}

        elif filtered_invoices.this_month:
            start = datetime(now.year, now.month, 1, tzinfo=timezone.utc)
            if now.month == 12:
                end = datetime(now.year + 1, 1, 1, tzinfo=timezone.utc)
            else:
                end = datetime(now.year, now.month + 1, 1, tzinfo=timezone.utc)
            date_filter[date_field] = {"$gte": start, "$lt": end}

        elif filtered_invoices.this_year:
            start = datetime(now.year, 1, 1, tzinfo=timezone.utc)
            end = datetime(now.year + 1, 1, 1, tzinfo=timezone.utc)
            date_filter[date_field] = {"$gte": start, "$lt": end}

        elif filtered_invoices.from_date or filtered_invoices.to_date:
            date_filter[date_field] = {}
            if filtered_invoices.from_date:
                date_filter[date_field]["$gte"] = filtered_invoices.from_date
            if filtered_invoices.to_date:
                date_filter[date_field]["$lte"] = filtered_invoices.to_date

            # Merge both filters into one $match
        if date_filter:
            match_stage.update(date_filter)

        search_pipeline.insert(1, {"$match": match_stage})

        search_pipeline.append({
            '$addFields': {
                'total_amounts': {
                    '$sum': '$items.amount'
                },
                'total_vats': {
                    '$sum': '$items.vat'
                }
            }
        })
        search_pipeline.append({
            '$facet': {
                'invoices': [
                    {
                        '$sort': {
                            'reference_number': -1
                        }
                    }
                ],
                'grand_totals': [
                    {
                        '$group': {
                            '_id': None,
                            'grand_amounts': {
                                '$sum': '$total_amounts'
                            },
                            'grand_vats': {
                                '$sum': '$total_vats'
                            }
                        }
                    }, {
                        '$project': {
                            '_id': 0
                        }
                    }
                ]
            }
        })
        cursor = await ap_invoices_collection.aggregate(search_pipeline)
        result = await cursor.to_list(None)
        if result and len(result) > 0:
            data = result[0]
            invoices = [serializer(r) for r in data.get("invoices", [])]
            totals = data.get("grand_totals", [])
            grand_totals = totals[0] if totals else {"grand_amounts": 0, "grand_vats": 0}
        else:
            invoices = []
            grand_totals = {"grand_amounts": 0, "grand_vats": 0}

        return {
            "invoices": invoices,
            "grand_totals": grand_totals
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")

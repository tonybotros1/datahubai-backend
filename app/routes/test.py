from typing import List

from fastapi import APIRouter, HTTPException, Body
from pydantic import BaseModel

from app.database import get_collection
from datetime import datetime
# import logging
from bson import ObjectId

# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)

router = APIRouter()
job_cards_collection = get_collection("job_cards")


def serialize_job(job: dict) -> dict:
    job["_id"] = str(job["_id"])
    if "car_brand" in job:
        job["car_brand"] = str(job["car_brand"])
    return job


@router.get("/job_cards/{brand_id}")
async def get_job_cards(brand_id: str):
    try:
        jobs = [serialize_job(job) for job in job_cards_collection.find({"car_brand": brand_id})]
        return {"jobs": jobs}
    except Exception as e:
        return {"error": str(e)}


class InvoiceRequest(BaseModel):
    customer_id: str
    job_ids: List[str]


# ===================================== All Customer Invoices =====================================

@router.post("/get_customer_invoices")
async def get_customer_invoices(customer_id: str = Body(..., embed=True)):
    try:
        if not customer_id:
            raise HTTPException(status_code=400, detail="No customer id")

        pipeline = [
            # 1️⃣ فلترة الـ jobs المطلوبة
            {
                "$match": {
                    "customer_id": customer_id,
                    "job_status_1": "Posted"
                }
            },
            # 2️⃣ Join مع invoice items
            {
                "$lookup": {
                    "from": "job_cards_invoice_items",
                    "localField": "_id",  # job id
                    "foreignField": "job_card_id",  # الربط بالـ job_id في invoice items
                    "as": "invoice_items"
                }
            },
            # 3️⃣ حساب مجموع net لكل job
            {
                "$addFields": {
                    "total_net_amount": {
                        "$sum": "$invoice_items.net"
                    }
                }
            },
            # 4️⃣ Join مع receipts
            {
                "$lookup": {
                    "from": "all_receipts",
                    "let": {"job_id": "$_id"},  # ObjectId تبع job_card
                    "pipeline": [
                        {
                            "$match": {
                                "$expr": {
                                    "$gt": [
                                        {
                                            "$size": {
                                                "$filter": {
                                                    "input": "$jobs",
                                                    "cond": {"$eq": ["$$this.job_id", "$$job_id"]}
                                                }
                                            }
                                        },
                                        0
                                    ]
                                }
                            }
                        }
                    ],
                    "as": "receipts_docs"
                }
            }
            ,
            {
                "$addFields": {
                    "receipt_amount": {
                        "$sum": {
                            "$map": {
                                "input": "$receipts_docs",  # كل receipt document
                                "as": "receipt_doc",
                                "in": {
                                    "$sum": {
                                        "$map": {
                                            "input": {
                                                "$filter": {
                                                    "input": "$$receipt_doc.jobs",  # مصفوفة jobs داخل receipt
                                                    "as": "job_item",
                                                    "cond": {"$eq": ["$$job_item.job_id", "$_id"]}
                                                    # بس العناصر الخاصة بالـ job الحالي
                                                }
                                            },
                                            "as": "filtered_job",
                                            "in": "$$filtered_job.amount"  # جمع الـ amount
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            ,
            # 5️⃣ Join مع brand
            {
                "$lookup": {
                    "from": "all_brands",
                    "localField": "car_brand",
                    "foreignField": "_id",
                    "as": "brand"
                }
            },
            {"$unwind": {"path": "$brand", "preserveNullAndEmptyArrays": True}},
            # 6️⃣ Join مع model
            {
                "$lookup": {
                    "from": "all_brand_models",
                    "let": {"brand_id": "$car_brand", "model_id": "$car_model"},
                    "pipeline": [
                        {
                            "$match": {
                                "$expr": {
                                    "$and": [
                                        {"$eq": ["$brand_id", "$$brand_id"]},
                                        {"$eq": ["$_id", "$$model_id"]}
                                    ]
                                }
                            }
                        }
                    ],
                    "as": "model"
                }
            },
            {"$unwind": {"path": "$model", "preserveNullAndEmptyArrays": True}},
            # 7️⃣ Projection نهائي
            {
                "$project": {
                    "_id": 1,
                    "invoice_number": 1,
                    "invoice_date": 1,
                    "total_net_amount": 1,
                    "receipt_amount": 1,
                    "outstanding_amount": {"$subtract": ["$total_net_amount", "$receipt_amount"]},
                    "plate_number": 1,
                    "brand_name": "$brand.name",
                    "model_name": "$model.name"
                }
            }

        ]

        docs = await job_cards_collection.aggregate(pipeline).to_list(length=None)
        result = []
        for doc in docs:
            raw_date = doc.get("invoice_date", "")
            formatted_date = ""
            if isinstance(raw_date, datetime):
                formatted_date = raw_date.strftime("%d-%m-%Y")
            elif isinstance(raw_date, str):
                try:
                    parsed_date = datetime.fromisoformat(raw_date)
                    formatted_date = parsed_date.strftime("%d-%m-%Y")
                except ValueError:
                    formatted_date = raw_date

            notes_str = (
                f"Invoice Number: {doc.get('invoice_number', '')}, "
                f"Invoice Date: {formatted_date}, "
                f"Brand: {doc.get('brand_name', '')}, "
                f"Model: {doc.get('model_name', '')}, "
                f"Plate Number: {doc.get('plate_number', '')}"
            )

            result.append({
                "is_selected": False,
                "job_id": str(doc["_id"]),
                "invoice_number": doc.get("invoice_number", ""),
                "invoice_date": formatted_date,
                "invoice_amount": str(doc.get("total_net_amount", 0)),
                "receipt_amount": str(doc.get("receipt_amount", 0)),
                "outstanding_amount": str(doc.get("outstanding_amount", 0)),
                "notes": notes_str
            })

        return {"invoices": result}


    except Exception as e:
        return {"error": str(e)}


# ===================================== Current Customer Invoices =====================================

@router.post("/get_current_customer_invoices")
async def get_current_customer_invoices(customer_id: str = Body(..., embed=True),
                                        job_ids: List[str] = Body(..., embed=True)):
    try:

        if not customer_id:
            raise HTTPException(status_code=400, detail="Missing customer_id")
        if not job_ids:
            return {"invoices": []}

        new_job_ids = []
        for jid in job_ids:
            if isinstance(jid, str):
                new_job_ids.append(ObjectId(jid))
            elif isinstance(jid, ObjectId):
                new_job_ids.append(jid)
            else:
                raise ValueError(f"Unsupported job id type: {type(jid)}")

        job_ids = new_job_ids

        pipeline = [
            # 1️⃣ فلترة الـ jobs المطلوبة
            {
                "$match": {
                    "_id": {"$in": job_ids},
                    "customer_id": customer_id,
                    "job_status_1": "Posted"
                }
            },
            # 2️⃣ Join مع invoice items
            {
                "$lookup": {
                    "from": "job_cards_invoice_items",
                    "localField": "_id",  # job id
                    "foreignField": "job_card_id",  # الربط بالـ job_id في invoice items
                    "as": "invoice_items"
                }
            },
            # 3️⃣ حساب مجموع net لكل job
            {
                "$addFields": {
                    "total_net_amount": {
                        "$sum": "$invoice_items.net"
                    }
                }
            },
            # 4️⃣ Join مع receipts
            {
                "$lookup": {
                    "from": "all_receipts",
                    "let": {"job_id": "$_id"},  # ObjectId تبع job_card
                    "pipeline": [
                        {
                            "$match": {
                                "$expr": {
                                    "$gt": [
                                        {
                                            "$size": {
                                                "$filter": {
                                                    "input": "$jobs",
                                                    "cond": {"$eq": ["$$this.job_id", "$$job_id"]}
                                                }
                                            }
                                        },
                                        0
                                    ]
                                }
                            }
                        }
                    ],
                    "as": "receipts_docs"
                }
            }
            ,
            {
                "$addFields": {
                    "receipt_amount": {
                        "$sum": {
                            "$map": {
                                "input": "$receipts_docs",  # كل receipt document
                                "as": "receipt_doc",
                                "in": {
                                    "$sum": {
                                        "$map": {
                                            "input": {
                                                "$filter": {
                                                    "input": "$$receipt_doc.jobs",  # مصفوفة jobs داخل receipt
                                                    "as": "job_item",
                                                    "cond": {"$eq": ["$$job_item.job_id", "$_id"]}
                                                    # بس العناصر الخاصة بالـ job الحالي
                                                }
                                            },
                                            "as": "filtered_job",
                                            "in": "$$filtered_job.amount"  # جمع الـ amount
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            ,
            # 5️⃣ Join مع brand
            {
                "$lookup": {
                    "from": "all_brands",
                    "localField": "car_brand",
                    "foreignField": "_id",
                    "as": "brand"
                }
            },
            {"$unwind": {"path": "$brand", "preserveNullAndEmptyArrays": True}},
            # 6️⃣ Join مع model
            {
                "$lookup": {
                    "from": "all_brand_models",
                    "let": {"brand_id": "$car_brand", "model_id": "$car_model"},
                    "pipeline": [
                        {
                            "$match": {
                                "$expr": {
                                    "$and": [
                                        {"$eq": ["$brand_id", "$$brand_id"]},
                                        {"$eq": ["$_id", "$$model_id"]}
                                    ]
                                }
                            }
                        }
                    ],
                    "as": "model"
                }
            },
            {"$unwind": {"path": "$model", "preserveNullAndEmptyArrays": True}},
            # 7️⃣ Projection نهائي
            {
                "$project": {
                    "_id": 1,
                    "invoice_number": 1,
                    "invoice_date": 1,
                    "total_net_amount": 1,
                    "receipt_amount": 1,
                    "outstanding_amount": {"$subtract": ["$total_net_amount", "$receipt_amount"]},
                    "plate_number": 1,
                    "brand_name": "$brand.name",
                    "model_name": "$model.name"
                }
            }

        ]

        docs = await job_cards_collection.aggregate(pipeline).to_list(length=None)

        result = []
        for doc in docs:
            raw_date = doc.get("invoice_date", "")
            formatted_date = ""
            if isinstance(raw_date, datetime):
                formatted_date = raw_date.strftime("%d-%m-%Y")
            elif isinstance(raw_date, str):
                try:
                    parsed_date = datetime.fromisoformat(raw_date)
                    formatted_date = parsed_date.strftime("%d-%m-%Y")
                except ValueError:
                    formatted_date = raw_date

            notes_str = (
                f"Invoice Number: {doc.get('invoice_number', '')}, "
                f"Invoice Date: {formatted_date}, "
                f"Brand: {doc.get('brand_name', '')}, "
                f"Model: {doc.get('model_name', '')}, "
                f"Plate Number: {doc.get('plate_number', '')}"
            )

            result.append({
                "is_selected": True,
                "job_id": str(doc["_id"]),
                "invoice_number": doc.get("invoice_number", ""),
                "invoice_date": formatted_date,
                "invoice_amount": str(doc.get("total_net_amount", 0)),
                "receipt_amount": str(doc.get("receipt_amount", 0)),
                "outstanding_amount": str(doc.get("outstanding_amount", 0)),
                "notes": notes_str
            })

        return {"invoices": result}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")

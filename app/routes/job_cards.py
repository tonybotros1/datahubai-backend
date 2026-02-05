import copy
from typing import Optional, List, Any
from bson import ObjectId
from fastapi import APIRouter, HTTPException, Depends, UploadFile, Form, File
from pydantic import BaseModel
from app import database
from app.core import security
from app.database import get_collection
from datetime import datetime
from app.routes.car_trading import PyObjectId
from app.routes.counters import create_custom_counter
from app.routes.quotation_cards import get_quotation_card_details
from app.widgets.check_date import is_date_equals_today_or_older
from app.widgets.upload_files import upload_file, delete_file_from_server
from app.widgets.upload_images import upload_image, delete_image_from_server

router = APIRouter()
job_cards_collection = get_collection("job_cards")
quotation_cards_collection = get_collection("quotation_cards")
job_cards_invoice_items_collection = get_collection("job_cards_invoice_items")
job_cards_internal_notes_collection = get_collection("job_cards_internal_notes")
job_cards_inspection_reports_collection = get_collection("job_cards_inspection_reports")
quotation_cards_invoice_items_collection = get_collection("quotation_cards_invoice_items")
quotation_cards_internal_notes_collection = get_collection("quotation_cards_internal_notes")
users_collection = get_collection("sys-users")


class InvoiceItems(BaseModel):
    uid: Optional[str] = None
    id: Optional[str] = None
    line_number: Optional[int] = None
    name: Optional[str] = None
    description: Optional[str] = None
    quantity: Optional[float] = None
    price: Optional[float] = None
    discount: Optional[float] = None
    amount: Optional[float] = None
    vat: Optional[float] = None
    net: Optional[float] = None
    total: Optional[float] = None
    is_modified: Optional[bool] = False
    deleted: Optional[bool] = False
    added: Optional[bool] = False
    job_card_id: Optional[str] = None


class JobCard(BaseModel):
    label: Optional[str] = None
    type: Optional[str] = None
    job_status_1: Optional[str] = None
    job_status_2: Optional[str] = None
    car_brand_logo: Optional[str] = None
    car_brand: Optional[str] = None
    car_model: Optional[str] = None
    plate_number: Optional[str] = None
    plate_code: Optional[str] = None
    country: Optional[str] = None
    city: Optional[str] = None
    year: Optional[str] = None
    color: Optional[str] = None
    engine_type: Optional[str] = None
    vehicle_identification_number: Optional[str] = None
    transmission_type: Optional[str] = None
    mileage_in: Optional[float] = None
    mileage_out: Optional[float] = None
    mileage_in_out_diff: Optional[float] = None
    fuel_amount: Optional[float] = None
    customer: Optional[str] = None
    contact_name: Optional[str] = None
    contact_email: Optional[str] = None
    contact_number: Optional[str] = None
    credit_limit: Optional[float] = None
    outstanding: Optional[float] = None
    salesman: Optional[str] = None
    branch: Optional[str] = None
    currency: Optional[str] = None
    rate: Optional[float] = None
    payment_method: Optional[str] = None
    lpo_number: Optional[str] = None
    job_approval_date: Optional[datetime] = None
    job_start_date: Optional[datetime] = None
    job_cancellation_date: Optional[datetime] = None
    job_finish_date: Optional[datetime] = None
    job_delivery_date: Optional[datetime] = None
    job_warranty_days: Optional[int] = None
    job_warranty_km: Optional[float] = None
    job_warranty_end_date: Optional[datetime] = None
    job_min_test_km: Optional[float] = None
    job_reference_1: Optional[str] = None
    job_reference_2: Optional[str] = None
    delivery_time: Optional[str] = None
    job_notes: Optional[str] = None
    job_delivery_notes: Optional[str] = None
    job_date: Optional[datetime] = None
    invoice_date: Optional[datetime] = None
    invoice_items: Optional[List[InvoiceItems]] = None


class JobCardSearch(BaseModel):
    label: Optional[str] = None
    job_number: Optional[str] = None
    invoice_number: Optional[str] = None
    car_brand: Optional[PyObjectId] = None
    car_model: Optional[PyObjectId] = None
    plate_number: Optional[str] = None
    vin: Optional[str] = None
    lpo: Optional[str] = None
    branch: Optional[PyObjectId] = None
    customer_name: Optional[PyObjectId] = None
    status: Optional[str] = None
    type: Optional[str] = None
    from_date: Optional[datetime] = None
    to_date: Optional[datetime] = None
    all: Optional[bool] = False
    today: Optional[bool] = False
    this_month: Optional[bool] = False
    this_year: Optional[bool] = False


pipeline: list[dict[str, Any]] = [
    {
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
                        '_id': 1,
                        'name': 1,
                        'logo': 1
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
            'from': 'quotation_cards',
            'let': {
                'quotation_id': '$quotation_id'
            },
            'pipeline': [
                {
                    '$match': {
                        '$expr': {
                            '$eq': [
                                '$_id', '$$quotation_id'
                            ]
                        }
                    }
                }, {
                    '$project': {
                        '_id': 1,
                        'quotation_number': 1
                    }
                }
            ],
            'as': 'quotation_details'
        }
    }, {
        '$unwind': {
            'path': '$quotation_details',
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
                        '_id': 1,
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
            'from': 'all_countries',
            'let': {
                'country_id': '$country'
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
                }, {
                    '$project': {
                        '_id': 1,
                        'name': 1
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
    }, {
        '$lookup': {
            'from': 'all_countries_cities',
            'let': {
                'city_id': '$city'
            },
            'pipeline': [
                {
                    '$match': {
                        '$expr': {
                            '$eq': [
                                '$_id', '$$city_id'
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
            'as': 'city_details'
        }
    }, {
        '$unwind': {
            'path': '$city_details',
            'preserveNullAndEmptyArrays': True
        }
    }, {
        '$lookup': {
            'from': 'all_lists_values',
            'let': {
                'color_id': '$color'
            },
            'pipeline': [
                {
                    '$match': {
                        '$expr': {
                            '$eq': [
                                '$_id', '$$color_id'
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
            'as': 'color_details'
        }
    }, {
        '$unwind': {
            'path': '$color_details',
            'preserveNullAndEmptyArrays': True
        }
    }, {
        '$lookup': {
            'from': 'all_lists_values',
            'let': {
                'engine_type_id': '$engine_type'
            },
            'pipeline': [
                {
                    '$match': {
                        '$expr': {
                            '$eq': [
                                '$_id', '$$engine_type_id'
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
            'as': 'engine_type_details'
        }
    }, {
        '$unwind': {
            'path': '$engine_type_details',
            'preserveNullAndEmptyArrays': True
        }
    }, {
        '$lookup': {
            'from': 'entity_information',
            'let': {
                'customer_id': '$customer'
            },
            'pipeline': [
                {
                    '$match': {
                        '$expr': {
                            '$eq': [
                                '$_id', '$$customer_id'
                            ]
                        }
                    }
                }, {
                    '$project': {
                        '_id': 1,
                        'entity_name': 1
                    }
                }
            ],
            'as': 'customer_details'
        }
    }, {
        '$unwind': {
            'path': '$customer_details',
            'preserveNullAndEmptyArrays': True
        }
    }, {
        '$lookup': {
            'from': 'sales_man',
            'let': {
                'salesman_id': '$salesman'
            },
            'pipeline': [
                {
                    '$match': {
                        '$expr': {
                            '$eq': [
                                '$_id', '$$salesman_id'
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
            'as': 'salesman_details'
        }
    }, {
        '$unwind': {
            'path': '$salesman_details',
            'preserveNullAndEmptyArrays': True
        }
    }, {
        '$lookup': {
            'from': 'branches',
            'let': {
                'branch_id': '$branch'
            },
            'pipeline': [
                {
                    '$match': {
                        '$expr': {
                            '$eq': [
                                '$_id', '$$branch_id'
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
                    '$project': {
                        '_id': 1,
                        'country_id': 1
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
            'from': 'all_countries',
            'let': {
                'currency_country_id': '$currency_details.country_id'
            },
            'pipeline': [
                {
                    '$match': {
                        '$expr': {
                            '$eq': [
                                '$_id', '$$currency_country_id'
                            ]
                        }
                    }
                }, {
                    '$project': {
                        '_id': 1,
                        'currency_code': 1
                    }
                }
            ],
            'as': 'currency_country_details'
        }
    }, {
        '$unwind': {
            'path': '$currency_country_details',
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
                    '$lookup': {
                        'from': 'invoice_items',
                        'let': {
                            'nameId': '$name'
                        },
                        'pipeline': [
                            {
                                '$match': {
                                    '$expr': {
                                        '$eq': [
                                            '$_id', '$$nameId'
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
                        'as': 'name_details'
                    }
                }, {
                    '$unwind': {
                        'path': '$name_details',
                        'preserveNullAndEmptyArrays': True
                    }
                }, {
                    '$addFields': {
                        'name_text': {
                            '$ifNull': [
                                '$name_details.name', None
                            ]
                        }
                    }
                }, {
                    '$project': {
                        'name_details': 0
                    }
                }
            ],
            'as': 'invoice_items_details'
        }
    }, {
        '$addFields': {
            'total_amount': {
                '$sum': {
                    '$map': {
                        'input': '$invoice_items_details',
                        'as': 'item',
                        'in': {
                            '$ifNull': [
                                '$$item.total', 0
                            ]
                        }
                    }
                }
            },
            'total_vat': {
                '$sum': {
                    '$map': {
                        'input': '$invoice_items_details',
                        'as': 'item',
                        'in': {
                            '$ifNull': [
                                '$$item.vat', 0
                            ]
                        }
                    }
                }
            },
            'total_net': {
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
        '$addFields': {
            'car_brand_name': {
                '$ifNull': [
                    '$brand_details.name', None
                ]
            },
            'car_brand_logo': {
                '$ifNull': [
                    '$brand_details.logo', None
                ]
            },
            'car_model_name': {
                '$ifNull': [
                    '$model_details.name', None
                ]
            },
            'country_name': {
                '$ifNull': [
                    '$country_details.name', None
                ]
            },
            'city_name': {
                '$ifNull': [
                    '$city_details.name', None
                ]
            },
            'color_name': {
                '$ifNull': [
                    '$color_details.name', None
                ]
            },
            'engine_type_name': {
                '$ifNull': [
                    '$engine_type_details.name', None
                ]
            },
            'customer_name': {
                '$ifNull': [
                    '$customer_details.entity_name', None
                ]
            },
            'salesman_name': {
                '$ifNull': [
                    '$salesman_details.name', None
                ]
            },
            'branch_name': {
                '$ifNull': [
                    '$branch_details.name', None
                ]
            },
            'currency_code': {
                '$ifNull': [
                    '$currency_country_details.currency_code', None
                ]
            },
            'quotation_number': {
                '$ifNull': [
                    '$quotation_details.quotation_number', None
                ]
            }
        }
    }, {
        '$project': {
            'brand_details': 0,
            'model_details': 0,
            'country_details': 0,
            'city_details': 0,
            'color_details': 0,
            'engine_type_details': 0,
            'customer_details': 0,
            'salesman_details': 0,
            'branch_details': 0,
            'currency_details': 0,
            'currency_country_details': 0,
            'quotation_details': 0
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


async def get_job_card_details(job_card_id: ObjectId):
    new_pipeline = copy.deepcopy(pipeline)
    new_pipeline.insert(0, {
        "$match": {
            "_id": job_card_id
        }
    })
    cursor = await job_cards_collection.aggregate(new_pipeline)
    result = await cursor.to_list(None)
    return result[0]


@router.post("/add_new_job_card")
async def add_new_job_card(job_data: JobCard, data: dict = Depends(security.get_current_user)):
    async with database.client.start_session() as session:
        try:
            await session.start_transaction()
            company_id = ObjectId(data.get("company_id"))

            job_data_dict = job_data.model_dump(exclude_unset=True)
            new_job_counter = await create_custom_counter("JCN", "J", data=data, description='Job Number',
                                                          session=session)

            invoices = []
            if job_data_dict.get("invoice_items"):
                invoices = job_data_dict["invoice_items"]
                job_data_dict.pop("invoice_items")

            job_data_dict.update({
                "company_id": company_id,
                "createdAt": security.now_utc(),
                "updatedAt": security.now_utc(),
                "car_brand": ObjectId(job_data_dict["car_brand"]) if job_data_dict["car_brand"] else None,
                "car_model": ObjectId(job_data_dict["car_model"]) if job_data_dict["car_model"] else None,
                "country": ObjectId(job_data_dict["country"]) if job_data_dict["country"] else None,
                "city": ObjectId(job_data_dict["city"]) if job_data_dict["city"] else None,
                "color": ObjectId(job_data_dict["color"]) if job_data_dict["color"] else None,
                "engine_type": ObjectId(job_data_dict["engine_type"]) if job_data_dict["engine_type"] else None,
                "customer": ObjectId(job_data_dict["customer"]) if job_data_dict["customer"] else None,
                "salesman": ObjectId(job_data_dict["salesman"]) if job_data_dict["salesman"] else None,
                "branch": ObjectId(job_data_dict["branch"]) if job_data_dict["branch"] else None,
                "currency": ObjectId(job_data_dict["currency"]) if job_data_dict["currency"] else None,
                "job_number": new_job_counter['final_counter'] if new_job_counter['success'] else None,
            })

            result = await job_cards_collection.insert_one(job_data_dict, session=session)
            if not result.inserted_id:
                raise HTTPException(status_code=500, detail="Failed to insert job card")

            if invoices:
                items_dict = [{
                    "job_card_id": result.inserted_id,
                    "company_id": company_id,
                    "line_number": invoice["line_number"] or 0,
                    "quantity": invoice["quantity"] or 0,
                    "price": invoice["price"] or 0,
                    "total": invoice["total"] or 0,
                    "net": invoice["net"] or 0,
                    "vat": invoice["vat"] or 0,
                    "name": ObjectId(invoice["name"]) if invoice["name"] else None,
                    "description": invoice["description"] or None,
                    "amount": invoice["amount"] or 0,
                    "discount": invoice["discount"] or 0,
                    "createdAt": security.now_utc(),
                    "updatedAt": security.now_utc(),
                } for invoice in invoices if not invoice["deleted"]]
                new_invoices = await job_cards_invoice_items_collection.insert_many(items_dict, session=session)
                if not new_invoices.inserted_ids:
                    raise HTTPException(status_code=500, detail="Failed to insert job items")
            await session.commit_transaction()
            new_job = await get_job_card_details(result.inserted_id)
            serialized = serializer(new_job)
            return {"job_card": serialized}


        except Exception as e:
            await session.abort_transaction()
            print(e)
            raise HTTPException(status_code=500, detail=str(e))


@router.delete("/delete_job_card/{job_id}")
async def delete_job_card(job_id: str, _: dict = Depends(security.get_current_user)):
    async with database.client.start_session() as session:
        try:
            await session.start_transaction()
            try:
                job_id = ObjectId(job_id)
            except Exception:
                raise HTTPException(status_code=400, detail="Invalid job_id format")
            if not job_id:
                raise HTTPException(status_code=404, detail="Job card not found")
            current_job = await job_cards_collection.find_one({"_id": job_id}, session=session)
            if not current_job:
                raise HTTPException(status_code=404, detail="Job card not found")
            if current_job['job_status_1'] != "New":
                raise HTTPException(status_code=403, detail="Only New Job Cards allowed")
            result = await job_cards_collection.delete_one({"_id": job_id}, session=session)
            if result.deleted_count == 0:
                raise HTTPException(status_code=404, detail="Job card not found or already deleted")
            await job_cards_invoice_items_collection.delete_many({"job_card_id": job_id}, session=session)
            job_notes = await job_cards_internal_notes_collection.find({"job_card_id": job_id},
                                                                       session=session).to_list(None)
            if job_notes:
                for job_note in job_notes:
                    if "note_public_id" in job_note and job_note["note_public_id"]:
                        await delete_file_from_server(job_note["note_public_id"])
                await job_cards_internal_notes_collection.delete_many({"job_card_id": job_id}, session=session)
            inspection_report = await job_cards_inspection_reports_collection.find_one({"job_card_id": job_id},
                                                                                       session=session)
            if inspection_report:
                if 'car_images' in inspection_report:
                    for image in inspection_report['car_images']:
                        if 'image_public_id' in image and image['image_public_id']:
                            await delete_image_from_server(image['image_public_id'])
                if 'customer_signature_public_id' in inspection_report and inspection_report[
                    'customer_signature_public_id']:
                    await delete_image_from_server(inspection_report['customer_signature_public_id'])
                if 'advisor_signature_public_id' in inspection_report and inspection_report[
                    'advisor_signature_public_id']:
                    await delete_image_from_server(inspection_report['advisor_signature_public_id'])
                if 'car_dialog_public_id' in inspection_report and inspection_report['car_dialog_public_id']:
                    await delete_image_from_server(inspection_report['car_dialog_public_id'])
                await job_cards_inspection_reports_collection.delete_one({"job_card_id": job_id}, session=session)
            await session.commit_transaction()
            return {"message": "Job card deleted successfully", "job_id": str(job_id)}

        except HTTPException:
            await session.abort_transaction()
            raise

        except Exception as e:
            print(e)
            await session.abort_transaction()
            raise HTTPException(status_code=500, detail=f"Delete failed: {str(e)}")


@router.post("/copy_job_card/{job_id}")
async def copy_job_card(job_id: str, data: dict = Depends(security.get_current_user)):
    async with database.client.start_session() as session:
        try:
            await session.start_transaction()
            job_id = ObjectId(job_id)
            if not job_id:
                raise HTTPException(status_code=404, detail="Job card not found")
            original_job = await job_cards_collection.find_one({"_id": job_id}, session=session)
            if not original_job:
                raise HTTPException(status_code=404, detail="Job card not found")
            print(original_job['job_status_1'])
            if original_job['job_status_1'] not in ["Posted", "Cancelled"]:
                raise HTTPException(status_code=403, detail="Only Posted / Cancelled Job Cards allowed")
            original_job.pop("_id", None)
            original_job['job_status_1'] = "New"
            original_job['job_status_2'] = "New"
            original_job['invoice_number'] = ""
            original_job['invoice_date'] = None
            new_job_counter = await create_custom_counter("JCN", "J", description='Job Cards Number', date=data,
                                                          session=session)
            original_job["job_number"] = new_job_counter["final_counter"] if new_job_counter["success"] else None
            warranty_end_date = original_job['job_warranty_end_date'] if original_job["job_warranty_end_date"] else None
            if warranty_end_date:
                result = is_date_equals_today_or_older(warranty_end_date)
                if result:
                    original_job['label'] = ""
                else:
                    original_job['label'] = "Returned"
            new_job = await job_cards_collection.insert_one(original_job, session=session)
            new_job_id = new_job.inserted_id
            related_items = await job_cards_invoice_items_collection.find({"job_card_id": job_id}).to_list(None)
            for item in related_items:
                item.pop("_id", None)
                item["job_card_id"] = new_job_id
                await job_cards_invoice_items_collection.insert_one(item, session=session)

            await session.commit_transaction()
            new_job_details = await get_job_card_details(new_job_id)
            serialized = serializer(new_job_details)

            return {"message": "Job card copied successfully", "copied_job": serialized}


        except HTTPException:
            await session.abort_transaction()
            raise

        except Exception as e:
            print(e)
            await session.abort_transaction()
            raise HTTPException(status_code=500, detail=f"Delete failed: {str(e)}")


@router.patch("/update_job_card/{job_id}")
async def update_job_card(job_id: str, job_data: JobCard, data: dict = Depends(security.get_current_user)):
    try:
        new_invoice_counter = ""
        job_id = ObjectId(job_id)
        job_data_dict = job_data.model_dump(exclude_unset=True)
        if job_data_dict["job_status_1"] == "Posted":
            new_invoice_counter_result = await create_custom_counter("JCI", "I", description='Job Card Invoice Number',
                                                                     data=data)
            new_invoice_counter = new_invoice_counter_result["final_counter"]

        job_data_dict.update({
            "invoice_number": new_invoice_counter,
            "updatedAt": security.now_utc(),
            "car_brand": ObjectId(job_data_dict["car_brand"]) if job_data_dict["car_brand"] else None,
            "car_model": ObjectId(job_data_dict["car_model"]) if job_data_dict["car_model"] else None,
            "country": ObjectId(job_data_dict["country"]) if job_data_dict["country"] else None,
            "city": ObjectId(job_data_dict["city"]) if job_data_dict["city"] else None,
            "color": ObjectId(job_data_dict["color"]) if job_data_dict["color"] else None,
            "engine_type": ObjectId(job_data_dict["engine_type"]) if job_data_dict["engine_type"] else None,
            "customer": ObjectId(job_data_dict["customer"]) if job_data_dict["customer"] else None,
            "salesman": ObjectId(job_data_dict["salesman"]) if job_data_dict["salesman"] else None,
            "branch": ObjectId(job_data_dict["branch"]) if job_data_dict["branch"] else None,
            "currency": ObjectId(job_data_dict["currency"]) if job_data_dict["currency"] else None,
        })
        result = await job_cards_collection.update_one({"_id": job_id}, {"$set": job_data_dict})
        if result.modified_count == 0:
            raise HTTPException(status_code=404)

        updated = await get_job_card_details(job_id)
        serialized = serializer(updated)
        return {"job_card": serialized}



    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.patch("/update_job_invoice_items")
async def update_job_invoice_items(
        items: list[InvoiceItems],
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
            if item.get("deleted"):
                if "id" not in item:
                    continue
                print('yes deleted')
                print(item['id'])
                deleted_list.append(ObjectId(item["id"]))

            elif item.get("added") and not item.get("deleted"):
                print('yes added')
                item.pop("id", None)
                # item.pop("uid", None)
                item['company_id'] = company_id
                item['job_card_id'] = ObjectId(item['job_card_id']) if item['job_card_id'] else None
                item["createdAt"] = security.now_utc()
                item["updatedAt"] = security.now_utc()
                item['name'] = ObjectId(item["name"]) if item["name"] else None
                item.pop("deleted", None)
                item.pop("added", None)
                item.pop("is_modified", None)
                added_list.append(item)


            elif item.get("is_modified") and not item.get("deleted") and not item.get("added"):
                if "id" not in item:
                    continue
                item_id = ObjectId(item["id"])
                print('yes modified')
                print(item_id)
                item["updatedAt"] = security.now_utc()
                if "job_card_id" in item:
                    item.pop("job_card_id", None)
                item["name"] = ObjectId(item["name"]) if item["name"] else None
                item.pop("deleted", None)
                item.pop("added", None)
                item.pop("is_modified", None)
                modified_list.append((item_id, item))

        async with  database.client.start_session() as s:
            await s.start_transaction()
            if deleted_list:
                await job_cards_invoice_items_collection.delete_many(
                    {"_id": {"$in": deleted_list}}, session=s
                )

            if added_list:
                added_invoices = await job_cards_invoice_items_collection.insert_many(
                    added_list, session=s
                )
                inserted_ids = added_invoices.inserted_ids
                for item, new_id in zip(added_list, inserted_ids):
                    response_item = {
                        "_id": str(new_id),
                        "uid": item.get("uid"),
                    }
                    updated_list.append(response_item)

            for item_id, item_data in modified_list:
                item_data.pop("id", None)
                await job_cards_invoice_items_collection.update_one(
                    {"_id": item_id},
                    {"$set": item_data},
                    session=s
                )
                updated_list.append({"_id": str(item_id), "uid": item_data["uid"]})

            await s.commit_transaction()
        return {"updated_items": updated_list, "deleted_items": [str(d) for d in deleted_list]}

    except Exception as e:
        print(e)
        await s.abort_transaction()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/get_job_card_status/{job_id}")
async def get_job_card_status(job_id: str, _: dict = Depends(security.get_current_user)):
    try:
        if not ObjectId.is_valid(job_id):
            raise HTTPException(status_code=400, detail="Invalid job_id format")

        job_object_id = ObjectId(job_id)

        result = await job_cards_collection.find_one(
            {"_id": job_object_id},
            {"_id": 0, "job_status_1": 1, "job_status_2": 1}
        )

        if not result:
            raise HTTPException(status_code=404, detail="Job card not found")

        return {"status": "success", "data": result}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")


@router.post("/search_engine_for_job_cards_3")
async def search_engine_for_job_cards_3(
        filter_jobs: JobCardSearch,
        data: dict = Depends(security.get_current_user)
):
    try:
        company_id = ObjectId(data.get("company_id"))
        user_id = ObjectId(data.get("sub"))

        user_branches = await get_user_branches(user_id)
        if user_branches:
            branch_filters = [{"branch": b, "company_id": company_id} for b in user_branches]
            match_stage: Any = {"$or": branch_filters}
        else:
            match_stage = {"company_id": company_id}

        if filter_jobs.from_date or filter_jobs.to_date:
            match_stage['date_field_to_filter'] = {}
            if filter_jobs.from_date:
                match_stage['date_field_to_filter']["$gte"] = filter_jobs.from_date
            if filter_jobs.to_date:
                match_stage['date_field_to_filter']["$lte"] = filter_jobs.to_date

        if filter_jobs.car_brand:
            match_stage["car_brand"] = filter_jobs.car_brand
        if filter_jobs.car_model:
            match_stage["car_model"] = filter_jobs.car_model
        if filter_jobs.branch:
            match_stage["branch"] = filter_jobs.branch

        if filter_jobs.label:
            match_stage["label"] = (
                filter_jobs.label if filter_jobs.label == "Returned" else "Not Returned"
            )

        if filter_jobs.job_number:
            match_stage["job_number"] = filter_jobs.job_number
        if filter_jobs.invoice_number:
            match_stage["invoice_number"] = filter_jobs.invoice_number
        if filter_jobs.plate_number:
            match_stage["plate_number"] = filter_jobs.plate_number

        if filter_jobs.type:
            match_stage["type"] = "SALES" if filter_jobs.type == "SALE" else "JOB"
            print(match_stage)

        if filter_jobs.lpo:
            match_stage["lpo_number"] = {"$regex": filter_jobs.lpo, "$options": "i"}
        if filter_jobs.vin:
            match_stage["vehicle_identification_number"] = {
                "$regex": filter_jobs.vin,
                "$options": "i"
            }

        if filter_jobs.customer_name:
            match_stage["customer"] = filter_jobs.customer_name

        if filter_jobs.status:
            if filter_jobs.status == "Posted" or filter_jobs.status == 'New':
                match_stage["job_status_1"] = filter_jobs.status
            elif filter_jobs.status.lower() == "not approved":
                match_stage["job_status_2"] = 'New'
            else:
                match_stage["job_status_2"] = filter_jobs.status

        pipeline_special = [
            {
                "$addFields": {
                    "date_field_to_filter": {
                        "$switch": {
                            "branches": [
                                {
                                    "case": {"$eq": [{"$toLower": "$job_status_1"}, "new"]},
                                    "then": "$job_date"
                                },
                                {
                                    "case": {"$eq": [{"$toLower": "$job_status_1"}, "cancelled"]},
                                    "then": "$job_cancellation_date"
                                },
                                {
                                    "case": {"$eq": [{"$toLower": "$job_status_1"}, "posted"]},
                                    "then": "$invoice_date"
                                }
                            ],
                            "default": "$job_date"
                        }
                    }
                }
            },
            {"$match": match_stage},
            {"$sort": {'date_field_to_filter': -1}},
            {
                "$lookup": {
                    "from": "all_brands",
                    "localField": "car_brand",
                    "foreignField": "_id",
                    "as": "car_brand_details"
                }
            },
            {
                "$lookup": {
                    "from": "all_brand_models",
                    "localField": "car_model",
                    "foreignField": "_id",
                    "as": "car_model_details"
                }
            },
            {
                "$lookup": {
                    "from": "all_lists_values",
                    "let": {
                        "colorId": "$color",
                        "engineTypeId": "$engine_type"
                    },
                    "pipeline": [
                        {
                            "$match": {
                                "$expr": {
                                    "$or": [
                                        {"$eq": ["$_id", "$$colorId"]},
                                        {"$eq": ["$_id", "$$engineTypeId"]}
                                    ]
                                }
                            }
                        },
                        {"$project": {"name": 1}}
                    ],
                    "as": "list_values"
                }
            },
            {
                "$addFields": {
                    "color_details": {
                        "$first": {
                            "$filter": {
                                "input": "$list_values",
                                "cond": {"$eq": ["$$this._id", "$color"]}
                            }
                        }
                    },
                    "engine_type_details": {
                        "$first": {
                            "$filter": {
                                "input": "$list_values",
                                "cond": {"$eq": ["$$this._id", "$engine_type"]}
                            }
                        }
                    }
                }
            },
            {"$lookup": {
                "from": "all_countries",
                "localField": "country",
                "foreignField": "_id",
                "as": "country_details"
            }},
            {"$lookup": {
                "from": "all_countries_cities",
                "localField": "city",
                "foreignField": "_id",
                "as": "city_details"
            }},
            {"$lookup": {
                "from": "sales_man",
                "localField": "salesman",
                "foreignField": "_id",
                "as": "salesman_details"
            }},
            {"$lookup": {
                "from": "branches",
                "localField": "branch",
                "foreignField": "_id",
                "as": "branch_details"
            }},
            {"$lookup": {
                "from": "entity_information",
                "localField": "customer",
                "foreignField": "_id",
                "as": "customer_details"
            }},
            {"$lookup": {
                "from": "quotation_cards",
                "localField": "quotation_id",
                "foreignField": "_id",
                "as": "quotation_details"
            }},
            {
                "$lookup": {
                    "from": "currencies",
                    "let": {"currency_id": "$currency"},
                    "pipeline": [
                        {"$match": {"$expr": {"$eq": ["$_id", "$$currency_id"]}}},
                        {"$project": {"country_id": 1}}
                    ],
                    "as": "currency_details"
                }
            },
            {"$set": {"currency_details": {"$arrayElemAt": ["$currency_details", 0]}}},
            {
                "$lookup": {
                    "from": "all_countries",
                    "let": {"cid": "$currency_details.country_id"},
                    "pipeline": [
                        {"$match": {"$expr": {"$eq": ["$_id", "$$cid"]}}},
                        {"$project": {"currency_code": 1}}
                    ],
                    "as": "currency_country_details"
                }
            },
            {
                "$lookup": {
                    "from": "job_cards_invoice_items",
                    "let": {"job_id": "$_id"},
                    "pipeline": [
                        {"$match": {"$expr": {"$eq": ["$job_card_id", "$$job_id"]}}},
                        {
                            "$lookup": {
                                "from": "invoice_items",
                                "let": {"nameId": "$name"},
                                "pipeline": [
                                    {"$match": {"$expr": {"$eq": ["$_id", "$$nameId"]}}},
                                    {"$project": {"name": 1}}
                                ],
                                "as": "name_details"
                            }
                        },
                        {
                            "$addFields": {
                                "name_text": {"$arrayElemAt": ["$name_details.name", 0]},
                                '_id': {
                                    '$toString': '$_id'
                                },
                                'company_id': {
                                    '$toString': '$company_id'
                                },
                                'job_card_id': {
                                    '$toString': '$job_card_id'
                                },
                                'name': {
                                    '$toString': '$name'
                                }
                            }
                        },
                        {"$project": {"name_details": 0}}
                    ],
                    "as": "invoice_items_details"
                }
            },
            {
                "$addFields": {
                    "total_amount": {"$sum": "$invoice_items_details.total"},
                    "total_vat": {"$sum": "$invoice_items_details.vat"},
                    "total_net": {"$sum": "$invoice_items_details.net"}
                }
            },
            {
                "$lookup": {
                    "from": "all_receipts_invoices",
                    "localField": "_id",
                    "foreignField": "job_id",
                    "as": "receipts_invoices_details"
                }
            },
            {
                "$addFields": {
                    "paid": {"$sum": "$receipts_invoices_details.amount"},

                }
            },
            {
                '$addFields': {
                    "final_outstanding": {"$subtract": ["$total_net", "$paid"]}
                }
            },
            {
                "$addFields": {
                    "car_brand_name": {"$arrayElemAt": ["$car_brand_details.name", 0]},
                    "car_brand_logo": {"$arrayElemAt": ["$car_brand_details.logo", 0]},
                    "car_model_name": {"$arrayElemAt": ["$car_model_details.name", 0]},
                    "country_name": {"$arrayElemAt": ["$country_details.name", 0]},
                    "city_name": {"$arrayElemAt": ["$city_details.name", 0]},
                    "color_name": "$color_details.name",
                    "engine_type_name": "$engine_type_details.name",
                    "customer_name": {"$arrayElemAt": ["$customer_details.entity_name", 0]},
                    "salesman_name": {"$arrayElemAt": ["$salesman_details.name", 0]},
                    "branch_name": {"$arrayElemAt": ["$branch_details.name", 0]},
                    "currency_code": {
                        "$arrayElemAt": ["$currency_country_details.currency_code", 0]
                    },
                    "quotation_number": {
                        "$arrayElemAt": ["$quotation_details.quotation_number", 0]
                    }
                }
            },
            {
                "$project": {
                    "car_brand_details": 0,
                    "car_model_details": 0,
                    "country_details": 0,
                    "city_details": 0,
                    "color_details": 0,
                    "engine_type_details": 0,
                    "customer_details": 0,
                    "salesman_details": 0,
                    "branch_details": 0,
                    "currency_details": 0,
                    "currency_country_details": 0,
                    "quotation_details": 0,
                    "list_values": 0,
                    "receipts_invoices_details": 0
                }
            },
            {
                "$addFields": {
                    "_id": {"$toString": "$_id"},
                    "company_id": {"$toString": "$company_id"},
                    "car_brand": {"$toString": "$car_brand"},
                    "car_model": {"$toString": "$car_model"},
                    "color": {"$toString": "$color"},
                    "engine_type": {"$toString": "$engine_type"},
                    "country": {"$toString": "$country"},
                    "city": {"$toString": "$city"},
                    "salesman": {"$toString": "$salesman"},
                    "branch": {"$toString": "$branch"},
                    "currency": {"$toString": "$currency"},
                    "customer": {"$toString": "$customer"},
                    "quotation_id": {"$toString": "$quotation_id"},
                }
            },
            {
                "$facet": {
                    "job_cards": [
                        {"$limit": 200},
                        {"$project": {
                            'car_brand_details': 0,
                            'car_model_details': 0,
                            'country_details': 0,
                            'city_details': 0,
                            'color_details': 0,
                            'engine_type_details': 0,
                            'customer_details': 0,
                            'salesman_details': 0,
                            'branch_details': 0,
                            'currency_details': 0,
                            'currency_country_details': 0,
                            'quotation_details': 0,
                            'user_details': 0,
                            'list_values': 0,
                            'receipts_invoices_details': 0
                        }}
                    ],
                    "grand_totals": [
                        {
                            "$group": {
                                "_id": None,
                                "grand_total": {"$sum": "$total_amount"},
                                "grand_vat": {"$sum": "$total_vat"},
                                "grand_net": {"$sum": "$total_net"},
                                "grand_paid": {"$sum": "$paid"},
                                "grand_outstanding": {"$sum": "$final_outstanding"},
                                "grand_count": {"$sum": 1}  # عدد كل الوثائق
                            }
                        },
                        {"$project": {"_id": 0}}  # إزالة _id
                    ]
                }
            }
        ]

        cursor = await job_cards_collection.aggregate(pipeline_special)
        job_cards = await cursor.next()
        if job_cards.get("job_cards"):
            all_job_cards = job_cards["job_cards"]
        else:
            all_job_cards = []
        if job_cards.get('grand_totals'):
            all_grand_totals = job_cards['grand_totals'][0]
        else:
            all_grand_totals = {}
        return {"job_cards": all_job_cards, "grand_totals": all_grand_totals}

    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=str(e))


async def get_user_branches(user_id: ObjectId):
    branches = await users_collection.find_one({"_id": user_id}, {
        "branches": 1
    })
    return branches.get('branches')


@router.post("/search_engine_for_job_card_in_ap_invoices_screen")
async def search_engine_for_job_card_in_ap_invoices_screen(filter_jobs: JobCardSearch,
                                                           data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        user_id = ObjectId(data.get("sub"))
        base_search_pipeline: list[dict] = []
        match_stage = {}

        base_search_pipeline.append(
            {
                '$lookup': {
                    'from': 'sys-users',
                    'let': {
                        'user_id': user_id
                    },
                    'pipeline': [
                        {
                            '$match': {
                                '$expr': {
                                    '$eq': [
                                        '$_id', '$$user_id'
                                    ]
                                }
                            }
                        }
                    ],
                    'as': 'user_details'
                }
            },
        )
        base_search_pipeline.append(
            {
                '$addFields': {
                    'user_branches': {
                        '$arrayElemAt': [
                            '$user_details.branches', 0
                        ]
                    }
                }
            },
        )

        if company_id:
            match_stage["company_id"] = company_id
        if filter_jobs.car_brand:
            match_stage["car_brand"] = filter_jobs.car_brand
        if filter_jobs.car_model:
            match_stage["car_model"] = filter_jobs.car_model
        if filter_jobs.job_number:
            match_stage["job_number"] = filter_jobs.job_number
        if filter_jobs.plate_number:
            match_stage["plate_number"] = filter_jobs.plate_number
        if filter_jobs.vin:
            match_stage["vehicle_identification_number"] = {"$regex": filter_jobs.vin, "$options": "i"}
        if filter_jobs.customer_name:
            match_stage["customer"] = filter_jobs.customer_name
        match_stage['type'] = "JOB"
        base_search_pipeline.append({"$match": match_stage})
        base_search_pipeline.append({"$sort": {'job_date': -1}})

        lookups = [
            ("car_brand", "all_brands"),
            ("car_model", "all_brand_models"),
        ]

        for local_field, collection in lookups:
            base_search_pipeline.append({
                "$lookup": {
                    "from": collection,
                    "let": {"field_id": f"${local_field}"},
                    "pipeline": [
                        {"$match": {"$expr": {"$eq": ["$_id", "$$field_id"]}}},
                        {"$project": {"name": 1, "logo": 1}}
                    ],
                    "as": f"{local_field}_details"
                }
            })

        base_search_pipeline.append({
            "$lookup": {
                "from": "entity_information",
                "localField": "customer",
                "foreignField": "_id",
                "as": "customer_details"
            }
        })

        base_search_pipeline.append({
            '$addFields': {
                'car_brand_name': {
                    '$arrayElemAt': [
                        '$car_brand_details.name', 0
                    ]
                },
                'car_model_name': {
                    '$arrayElemAt': [
                        '$car_model_details.name', 0
                    ]
                },
                'customer_name': {
                    '$arrayElemAt': [
                        '$customer_details.entity_name', 0
                    ]
                },
            }
        })

        job_cards_pipeline = base_search_pipeline + [
            {
                "$project": {
                    'car_brand_details': 0,
                    'car_model_details': 0,
                    'customer_details': 0,
                }
            }
        ]
        job_cards_pipeline.append(
            {"$limit": 200},

        )

        job_cards_cursor = await job_cards_collection.aggregate(job_cards_pipeline)
        job_cards_raw = await job_cards_cursor.to_list(None)
        job_cards = [serializer(r) for r in job_cards_raw]
        return {
            "job_cards": job_cards,
        }

    except HTTPException:
        raise
    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")


def returning_internal_note_pipeline(company_id: ObjectId, user_id: ObjectId,
                                     job_id: ObjectId):  # this function to assign the ids to the pipeline
    internal_note_pipeline = [
        {
            "$match": {
                "job_card_id": job_id,
                "company_id": company_id,
            }
        },
        {
            '$lookup': {
                'from': 'sys-users',
                'let': {
                    'user_id': '$user_id'
                },
                'pipeline': [
                    {
                        '$match': {
                            '$expr': {
                                '$eq': [
                                    '$_id', '$$user_id'
                                ]
                            }
                        }
                    }, {
                        '$project': {
                            'user_name': 1
                        }
                    }
                ],
                'as': 'user_details'
            }
        }, {
            '$unwind': {
                'path': '$user_details',
                'preserveNullAndEmptyArrays': True
            }
        }, {
            '$addFields': {
                'user_name': {
                    '$ifNull': [
                        '$user_details.user_name', None
                    ]
                },
                'is_this_user_is_the_current_user': {
                    '$cond': {
                        'if': {
                            '$eq': [
                                '$user_id', user_id
                            ]
                        },
                        'then': True,
                        'else': False
                    }
                }
            }
        }, {
            '$project': {
                "user_details": 0
            }
        }
    ]
    return internal_note_pipeline


@router.get("/get_all_internal_notes_for_job_card/{job_id}")
async def get_all_internal_notes_for_job_card(job_id: str, data: dict = Depends(security.get_current_user)):
    try:
        user_id = ObjectId(data.get('sub'))
        company_id = ObjectId(data.get('company_id'))
        job_id = ObjectId(job_id)
        internal_notes_pipeline_copy = returning_internal_note_pipeline(company_id=company_id, user_id=user_id,
                                                                        job_id=job_id)
        cursor = await job_cards_internal_notes_collection.aggregate(internal_notes_pipeline_copy)
        results = await cursor.to_list(None)
        serialized = [serializer(r) for r in results]
        return {"internal_notes": serialized}

    except HTTPException:
        raise
    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")


@router.post("/add_new_internal_note_for_job_card/{job_id}")
async def add_new_internal_note_for_job_card(job_id: str, note_type: str = Form(None), note: str = Form(None),
                                             media_note: UploadFile = File(None), file_name: str = Form(None),
                                             data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        user_id = ObjectId(data.get("sub"))
        note_public_id = None
        if note_type and note_type.lower() != 'text' and media_note is not None:
            if note_type.lower() != 'image':
                result = await upload_file(media_note, folder="job cards internal notes")
                file_name = result["file_name"]
                note = result["url"] if "url" in result else None
                note_public_id = result['public_id'] if "public_id" in result else None
            else:
                result = await upload_image(media_note, folder="job cards internal notes")
                file_name = result["file_name"]
                note = result["url"] if "url" in result else None
                note_public_id = result['public_id'] if "public_id" in result else None

        internal_note_dict = {
            "job_card_id": ObjectId(job_id),
            "company_id": company_id,
            "user_id": user_id,
            "type": note_type,
            "note": note,
            'createdAt': security.now_utc(),
            'updatedAt': security.now_utc(),
            "file_name": file_name,
            "note_public_id": note_public_id,
        }
        new_note = await job_cards_internal_notes_collection.insert_one(internal_note_dict)
        new_internal_note_pipeline = returning_internal_note_pipeline(company_id=company_id, user_id=user_id,
                                                                      job_id=ObjectId(job_id))
        new_internal_note_pipeline.insert(1, {
            "$match": {
                "_id": new_note.inserted_id
            }
        })
        cursor = await job_cards_internal_notes_collection.aggregate(new_internal_note_pipeline)
        result = await cursor.to_list(None)
        serialized = serializer(result[0])

        return {"new_internal_note": serialized}

    except HTTPException:
        raise
    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")


@router.post("/create_quotation_card_for_current_job/{job_id}")
async def create_job_card_for_current_quotation(job_id: str, data: dict = Depends(security.get_current_user)):
    async with database.client.start_session() as session:
        try:
            await session.start_transaction()
            job_id = ObjectId(job_id)
            if not job_id:
                raise HTTPException(status_code=404, detail="Job card not found")
            already_created_quotation = await quotation_cards_collection.find_one({"job_card_id": job_id})
            if already_created_quotation:
                raise HTTPException(status_code=409, detail="Quotation is already created")
            original_job = await job_cards_collection.find_one({"_id": job_id}, session=session)
            if not original_job:
                raise HTTPException(status_code=404, detail="Job card not found")
            if original_job['job_status_1'] != "Posted":
                raise HTTPException(status_code=403, detail="Only Posted Jobs Cards allowed")
            job_warranty_days = original_job['job_warranty_days']
            job_warranty_kms = original_job['job_warranty_km']
            original_job.update({
                "job_card_id": original_job['_id']
            })
            original_job.pop("_id", None)
            original_job.pop("job_status_1", None)
            original_job.pop("job_status_2", None)
            original_job.pop("invoice_number", None)
            original_job.pop("lpo_number", None)
            original_job.pop("job_date", None)
            original_job.pop("invoice_date", None)
            original_job.pop("job_approval_date", None)
            original_job.pop("job_start_date", None)
            original_job.pop("job_cancellation_date", None)
            original_job.pop("job_finish_date", None)
            original_job.pop("job_delivery_date", None)
            original_job.pop("job_warranty_km", None)
            original_job.pop("job_warranty_days", None)
            original_job.pop("job_min_test_km", None)
            original_job.pop("job_reference_1", None)
            original_job.pop("job_reference_2", None)
            original_job.pop("job_reference_3", None)
            original_job.pop("job_notes", None)
            original_job.pop("job_delivery_notes", None)
            original_job.pop("mileage_out", None)
            original_job.pop("mileage_in_out_diff", None)
            original_job.update({
                "job_card_id": ObjectId(job_id),
                'quotation_status': 'New',
                'quotation_date': security.now_utc(),
                'quotation_warranty_days': job_warranty_days,
                'quotation_warranty_km': job_warranty_kms,
            })
            new_quotation_counter = await create_custom_counter("QN", "Q", description='Quotation Number', data=data,
                                                                session=session)
            original_job["quotation_number"] = new_quotation_counter["final_counter"] if new_quotation_counter[
                "success"] else None

            new_quotation = await quotation_cards_collection.insert_one(original_job, session=session)
            new_quotation_id = new_quotation.inserted_id
            related_items = await job_cards_invoice_items_collection.find(
                {"job_card_id": job_id}).to_list(None)
            for item in related_items:
                item.pop("_id", None)
                item["quotation_card_id"] = new_quotation_id
                await quotation_cards_invoice_items_collection.insert_one(item, session=session)
            await job_cards_collection.update_one({"_id": job_id}, {"$set": {
                "quotation_id": new_quotation_id,
            }}, session=session)
            await session.commit_transaction()
            return {"quotation_number": new_quotation_counter["final_counter"],
                    "quotation_card_id": str(new_quotation_id)}

        except HTTPException:
            await session.abort_transaction()
            raise
        except Exception as e:
            print(e)
            await session.abort_transaction()
            raise HTTPException(status_code=500, detail=f"failed: {str(e)}")


@router.get("/open_quotation_card_screen_by_quotation_number_for_job/{quotation_id}")
async def open_quotation_card_screen_by_quotation_number_for_job(quotation_id: str,
                                                                 _: dict = Depends(security.get_current_user)):
    try:
        required_quotation = await get_quotation_card_details(ObjectId(quotation_id))
        serialized = serializer(required_quotation)
        return {"required_quotation": serialized}


    except HTTPException:
        raise
    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=f"failed: {str(e)}")


@router.get("/get_customer_outstanding/{customer_id}")
async def get_customer_outstanding(customer_id: str, data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        customer_id = ObjectId(customer_id)
        customer_outstanding_pipeline = [
            {
                '$match': {
                    'company_id': company_id,
                    'customer': customer_id,
                    'job_status_1': 'Posted'
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
                '$group': {
                    '_id': None,
                    'total_net_amount': {
                        '$sum': '$net_amount'
                    },
                    'total_received': {
                        '$sum': '$received'
                    }
                }
            }, {
                '$addFields': {
                    'outstanding': {
                        '$subtract': [
                            '$total_net_amount', '$total_received'
                        ]
                    }
                }
            }, {
                '$project': {
                    '_id': 0,
                    'outstanding': 1
                }
            },
            {
                '$facet': {
                    'data': [{'$project': {'outstanding': 1}}],
                    'default': [
                        {
                            '$project': {
                                '_id': 0,
                                'outstanding': {'$literal': 0},
                            }
                        }
                    ]
                }
            },
            {
                '$project': {
                    'merged': {
                        '$concatArrays': ['$data', '$default']
                    }
                }
            },
            {
                '$project': {
                    'result': {'$arrayElemAt': ['$merged', 0]}
                }
            },
            {
                '$replaceRoot': {
                    'newRoot': {'$ifNull': ['$result', {'outstanding': 0}]}
                }
            }
        ]
        cursor = await job_cards_collection.aggregate(customer_outstanding_pipeline)
        result = await cursor.next()
        print(result)
        return result

    except HTTPException:
        raise
    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=f"failed: {str(e)}")


@router.get("/get_all_job_cards")
async def get_all_job_cards(data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data["company_id"])
        new_pipeline_for_all_jobs = copy.deepcopy(pipeline)
        new_pipeline_for_all_jobs.insert(0, {
            "$match": {
                "company_id": company_id
            }
        })
        new_pipeline_for_all_jobs.insert(1, {
            "$sort": {
                "job_number": -1
            }
        })
        cursor = await job_cards_collection.aggregate(new_pipeline_for_all_jobs)
        results = await cursor.to_list(None)
        serialized = [serializer(r) for r in results]
        return {"all_jobs": serialized}


    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"failed: {str(e)}")


@router.get("/get_job_items_summary_table/{job_id}")
async def get_job_items_summary_table(job_id: str, _: dict = Depends(security.get_current_user)):
    try:
        items_summary_table_pipeline = [
            {
                '$match': {
                    '_id': ObjectId(job_id)
                }
            }, {
                '$lookup': {
                    'from': 'ap_invoices_items',
                    'let': {
                        'job_id': '$_id'
                    },
                    'pipeline': [
                        {
                            '$match': {
                                '$expr': {
                                    '$eq': [
                                        '$job_number_id', '$$job_id'
                                    ]
                                }
                            }
                        }, {
                            '$lookup': {
                                'from': 'ap_invoices',
                                'localField': 'ap_invoice_id',
                                'foreignField': '_id',
                                'as': 'main_ap_invoice_doc'
                            }
                        }, {
                            '$addFields': {
                                'invoice_number': {
                                    '$ifNull': [
                                        {
                                            '$arrayElemAt': [
                                                '$main_ap_invoice_doc.reference_number', 0
                                            ]
                                        }, None
                                    ]
                                },
                                'issue_date': {
                                    '$arrayElemAt': [
                                        '$main_ap_invoice_doc.invoice_date', 0
                                    ]
                                },
                                'item_code': 'DIRECT PURCHASE',
                                'item_name': {
                                    '$ifNull': [
                                        '$note', None
                                    ]
                                },
                                'quantity': 0,
                                'price': 0,
                                'total': '$amount',
                                'net': {
                                    '$add': [
                                        '$amount', '$vat'
                                    ]
                                }
                            }
                        }, {
                            '$project': {
                                'company_id': 0,
                                'ap_invoice_id': 0,
                                'transaction_type': 0,
                                'main_ap_invoice_doc': 0,
                                'transaction_type_details': 0,
                                'job_number_id': 0,
                                'note': 0,
                                'createdAt': 0,
                                'updatedAt': 0,
                                'received_number': 0,
                                'amount': 0,
                                '_id': 0
                            }
                        }, {
                            '$unionWith': {
                                'coll': 'issuing',
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
                                                    '$addFields': {
                                                        'item_code': {
                                                            '$ifNull': [
                                                                {
                                                                    '$arrayElemAt': [
                                                                        '$inventory_items_details.code', 0
                                                                    ]
                                                                }, None
                                                            ]
                                                        },
                                                        'item_name': {
                                                            '$ifNull': [
                                                                {
                                                                    '$arrayElemAt': [
                                                                        '$inventory_items_details.name', 0
                                                                    ]
                                                                }, None
                                                            ]
                                                        }
                                                    }
                                                }
                                            ],
                                            'as': 'issuing_items'
                                        }
                                    }, {
                                        '$unwind': {
                                            'path': '$issuing_items',
                                            'preserveNullAndEmptyArrays': True
                                        }
                                    }, {
                                        '$addFields': {
                                            'invoice_number': '$issuing_number',
                                            'issue_date': '$date',
                                            'item_code': '$issuing_items.item_code',
                                            'item_name': '$issuing_items.item_name',
                                            'quantity': '$issuing_items.quantity',
                                            'price': '$issuing_items.price',
                                            'vat': 0,
                                            'total': {
                                                '$multiply': [
                                                    '$issuing_items.quantity', '$issuing_items.price'
                                                ]
                                            },
                                            'net': {
                                                '$multiply': [
                                                    '$issuing_items.quantity', '$issuing_items.price'
                                                ]
                                            }
                                        }
                                    }, {
                                        '$project': {
                                            'issuing_items': 0,
                                            'date': 0,
                                            'branch': 0,
                                            'issue_type': 0,
                                            'job_card_id': 0,
                                            'converter_id': 0,
                                            'note': 0,
                                            'received_by': 0,
                                            'status': 0,
                                            'createdAt': 0,
                                            'updatedAt': 0,
                                            'company_id': 0,
                                            'issuing_number': 0,
                                            '_id': 0
                                        }
                                    }
                                ]
                            }
                        }
                    ],
                    'as': 'items_summary'
                }
            }, {
                '$project': {
                    'items_summary': 1,
                    '_id': 0
                }
            }
        ]
        cursor = await job_cards_collection.aggregate(items_summary_table_pipeline)
        result = await cursor.next()
        return {"summary_table": result['items_summary']}
    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=f"failed: {str(e)}")


@router.get("/get_time_sheets_table_summary/{job_id}")
async def get_time_sheets_table_summary(job_id: str, _: dict = Depends(security.get_current_user)):
    try:
        time_sheets_summary_pipeline = [
            {
                '$match': {
                    '_id': ObjectId(job_id)
                }
            }, {
                '$lookup': {
                    'from': 'time_sheets',
                    'let': {
                        'job_id': '$_id'
                    },
                    'pipeline': [
                        {
                            '$match': {
                                '$expr': {
                                    '$and': [
                                        {
                                            '$eq': [
                                                '$job_id', '$$job_id'
                                            ]
                                        }, {
                                            '$gt': [
                                                {
                                                    '$size': {
                                                        '$filter': {
                                                            'input': '$active_periods',
                                                            'as': 'period',
                                                            'cond': {
                                                                '$ne': [
                                                                    '$$period.to', None
                                                                ]
                                                            }
                                                        }
                                                    }
                                                }, 0
                                            ]
                                        }
                                    ]
                                }
                            }
                        }, {
                            '$lookup': {
                                'from': 'all_job_tasks',
                                'localField': 'task_id',
                                'foreignField': '_id',
                                'as': 'task_details'
                            }
                        }, {
                            '$lookup': {
                                'from': 'all_lists_values',
                                'localField': 'employee_id',
                                'foreignField': '_id',
                                'as': 'employees_details'
                            }
                        }, {
                            '$addFields': {
                                'task_name_ar': {
                                    '$ifNull': [
                                        {
                                            '$arrayElemAt': [
                                                '$task_details.name_ar', 0
                                            ]
                                        }, None
                                    ]
                                },
                                'task_name_en': {
                                    '$ifNull': [
                                        {
                                            '$arrayElemAt': [
                                                '$task_details.name_en', 0
                                            ]
                                        }, None
                                    ]
                                },
                                'employee_name': {
                                    '$ifNull': [
                                        {
                                            '$arrayElemAt': [
                                                '$employees_details.name', 0
                                            ]
                                        }, None
                                    ]
                                },
                                'start_date': {
                                    '$arrayElemAt': [
                                        '$active_periods.from', 0
                                    ]
                                },
                                'end_date': {
                                    '$arrayElemAt': [
                                        '$active_periods.to', -1
                                    ]
                                },
                                'time_in_hours': {
                                    '$round': [
                                        {
                                            '$divide': [
                                                {
                                                    '$sum': {
                                                        '$map': {
                                                            'input': '$active_periods',
                                                            'as': 'period',
                                                            'in': {
                                                                '$cond': [
                                                                    {
                                                                        '$and': [
                                                                            {
                                                                                '$ne': [
                                                                                    '$$period.from', None
                                                                                ]
                                                                            }, {
                                                                                '$ne': [
                                                                                    '$$period.to', None
                                                                                ]
                                                                            }
                                                                        ]
                                                                    }, {
                                                                        '$subtract': [
                                                                            '$$period.to', '$$period.from'
                                                                        ]
                                                                    }, 0
                                                                ]
                                                            }
                                                        }
                                                    }
                                                }, 1000 * 60 * 60
                                            ]
                                        }, 2
                                    ]
                                }
                            }
                        }, {
                            '$project': {
                                'task_details': 0,
                                'company_id': 0,
                                'job_id': 0,
                                'task_id': 0,
                                'employee_id': 0,
                                '_id': 0,
                                'employees_details': 0,
                                'active_periods': 0
                            }
                        }
                    ],
                    'as': 'time_sheets_details'
                }
            }, {
                '$project': {
                    '_id': 0,
                    'time_sheets_details': 1
                }
            }
        ]
        cursor = await job_cards_collection.aggregate(time_sheets_summary_pipeline)
        result = await cursor.next()
        return {"time_sheets_summary": result['time_sheets_details']}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"failed: {str(e)}")

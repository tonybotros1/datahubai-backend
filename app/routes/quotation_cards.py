from typing import Optional, List, Any
from bson import ObjectId
from fastapi import APIRouter, HTTPException, Depends, UploadFile, Form, File
from pydantic import BaseModel, ValidationError
from starlette.responses import JSONResponse

from app import database
from app.core import security
from app.database import get_collection
from datetime import datetime, timezone, timedelta

from app.routes.car_trading import PyObjectId
from app.routes.counters import create_custom_counter
from app.widgets.check_date import is_date_equals_today_or_older
from app.widgets.upload_files import upload_file, delete_file_from_server
from app.widgets.upload_images import upload_image

router = APIRouter()
quotation_cards_collection = get_collection("quotation_cards")
job_cards_collection = get_collection("job_cards")
quotation_cards_invoice_items_collection = get_collection("quotation_cards_invoice_items")


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
    quotation_card_id: Optional[str] = None


class QuotationCard(BaseModel):
    # job_number: Optional[str] = None
    quotation_status: Optional[str] = None
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
    validity_days: Optional[int] = None
    validity_end_date: Optional[datetime] = None
    quotation_date: Optional[datetime] = None
    reference_number: Optional[str] = None
    delivery_time: Optional[str] = None
    quotation_warranty_days: Optional[int] = None
    quotation_warranty_km: Optional[float] = None
    quotation_notes: Optional[str] = None
    invoice_items: Optional[List[InvoiceItems]] = None


class QuotationCardSearch(BaseModel):
    quotation_number: Optional[str] = None
    car_brand: Optional[PyObjectId] = None
    car_model: Optional[PyObjectId] = None
    plate_number: Optional[str] = None
    vin: Optional[str] = None
    customer_name: Optional[PyObjectId] = None
    status: Optional[str] = None
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
                        '_id': 1,
                        'job_number': 1
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
            'from': 'quotation_cards_invoice_items',
            'let': {
                'quotation_id': '$_id'
            },
            'pipeline': [
                {
                    '$match': {
                        '$expr': {
                            '$eq': [
                                '$quotation_card_id', '$$quotation_id'
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


async def get_quotation_card_details(quotation_card_id: ObjectId):
    new_pipeline = pipeline.copy()
    new_pipeline.insert(1, {
        "$match": {
            "_id": quotation_card_id
        }
    })
    cursor = await quotation_cards_collection.aggregate(new_pipeline)
    result = await cursor.to_list(None)
    return result[0]


@router.post("/add_new_quotation_card")
async def add_new_quotation_card(quotation_data: QuotationCard, data: dict = Depends(security.get_current_user)):
    async with database.client.start_session() as session:
        try:
            await session.start_transaction()
            company_id = ObjectId(data.get("company_id"))

            quotation_data_dict = quotation_data.model_dump(exclude_unset=True)
            new_quotation_counter = await create_custom_counter("QN", "R", data, session)

            invoices = []
            if quotation_data_dict.get("invoice_items"):
                invoices = quotation_data_dict["invoice_items"]
                quotation_data_dict.pop("invoice_items")

            quotation_data_dict.update({
                "company_id": company_id,
                "createdAt": security.now_utc(),
                "updatedAt": security.now_utc(),
                "car_brand": ObjectId(quotation_data_dict["car_brand"]) if quotation_data_dict["car_brand"] else None,
                "car_model": ObjectId(quotation_data_dict["car_model"]) if quotation_data_dict["car_model"] else None,
                "country": ObjectId(quotation_data_dict["country"]) if quotation_data_dict["country"] else None,
                "city": ObjectId(quotation_data_dict["city"]) if quotation_data_dict["city"] else None,
                "color": ObjectId(quotation_data_dict["color"]) if quotation_data_dict["color"] else None,
                "engine_type": ObjectId(quotation_data_dict["engine_type"]) if quotation_data_dict["engine_type"] else None,
                "customer": ObjectId(quotation_data_dict["customer"]) if quotation_data_dict["customer"] else None,
                "salesman": ObjectId(quotation_data_dict["salesman"]) if quotation_data_dict["salesman"] else None,
                "branch": ObjectId(quotation_data_dict["branch"]) if quotation_data_dict["branch"] else None,
                "currency": ObjectId(quotation_data_dict["currency"]) if quotation_data_dict["currency"] else None,
                "quotation_number": new_quotation_counter['final_counter'] if new_quotation_counter[
                    'success'] else None,
            })

            result = await quotation_cards_collection.insert_one(quotation_data_dict, session=session)
            if not result.inserted_id:
                raise HTTPException(status_code=500, detail="Failed to insert quotation card")

            if invoices:
                items_dict = [{
                    "quotation_card_id": result.inserted_id,
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
                new_invoices = await quotation_cards_invoice_items_collection.insert_many(items_dict, session=session)
                if not new_invoices.inserted_ids:
                    raise HTTPException(status_code=500, detail="Failed to insert quotation items")
            await session.commit_transaction()
            new_quotation = await get_quotation_card_details(result.inserted_id)
            serialized = serializer(new_quotation)
            return {"quotation_card": serialized}

        except ValidationError as e:
            print("\n=== VALIDATION ERROR ===")
            print(e.errors())
            print("========================\n")
            return JSONResponse(status_code=422, content={"detail": e.errors()})
        except Exception as e:
            print(e)
            await session.abort_transaction()
            raise HTTPException(status_code=500, detail=str(e))


@router.delete("/delete_quotation_card/{quotation_id}")
async def delete_quotation_card(quotation_id: str, _: dict = Depends(security.get_current_user)):
    async with database.client.start_session() as session:
        try:
            await session.start_transaction()
            quotation_id = ObjectId(quotation_id)
            if not quotation_id:
                raise HTTPException(status_code=404, detail="Quotation card not found")
            current_quotation = await quotation_cards_collection.find_one({"_id": quotation_id}, session=session)
            if not current_quotation:
                raise HTTPException(status_code=404, detail="Quotation card not found")
            if current_quotation['quotation_status'] != "New":
                raise HTTPException(status_code=403, detail="Only New Quotation Cards allowed")
            result = await quotation_cards_collection.delete_one({"_id": quotation_id}, session=session)
            if result.deleted_count == 0:
                raise HTTPException(status_code=404, detail="Quotation card not found or already deleted")
            await quotation_cards_invoice_items_collection.delete_many({"quotation_card_id": quotation_id},
                                                                       session=session)
            quotation_notes = await quotation_cards_invoice_items_collection.find({"quotation_card_id": quotation_id},
                                                                                  session=session).to_list(None)
            if quotation_notes:
                for quotation_note in quotation_notes:
                    if "note_public_id" in quotation_note and quotation_note["note_public_id"]:
                        await delete_file_from_server(quotation_note["note_public_id"])
                await quotation_cards_invoice_items_collection.delete_many({"quotation_card_id": quotation_id},
                                                                           session=session)
            await session.commit_transaction()
            return {"message": "Quotation card deleted successfully", "quotation_id": str(quotation_id)}

        except HTTPException:
            await session.abort_transaction()
            raise

        except Exception as e:
            print(e)
            await session.abort_transaction()
            raise HTTPException(status_code=500, detail=f"Delete failed: {str(e)}")


@router.patch("/update_quotation_card/{quotation_id}")
async def update_quotation_card(quotation_id: str, quotation_data: QuotationCard,
                                _: dict = Depends(security.get_current_user)):
    try:
        quotation_id = ObjectId(quotation_id)
        quotation_data_dict = quotation_data.model_dump(exclude_unset=True)

        quotation_data_dict.update({
            "updatedAt": security.now_utc(),
            "car_brand": ObjectId(quotation_data_dict["car_brand"]) if quotation_data_dict["car_brand"] else None,
            "car_model": ObjectId(quotation_data_dict["car_model"]) if quotation_data_dict["car_model"] else None,
            "country": ObjectId(quotation_data_dict["country"]) if quotation_data_dict["country"] else None,
            "city": ObjectId(quotation_data_dict["city"]) if quotation_data_dict["city"] else None,
            "color": ObjectId(quotation_data_dict["color"]) if quotation_data_dict["color"] else None,
            "engine_type": ObjectId(quotation_data_dict["engine_type"]) if quotation_data_dict["engine_type"] else None,
            "customer": ObjectId(quotation_data_dict["customer"]) if quotation_data_dict["customer"] else None,
            "salesman": ObjectId(quotation_data_dict["salesman"]) if quotation_data_dict["salesman"] else None,
            "branch": ObjectId(quotation_data_dict["branch"]) if quotation_data_dict["branch"] else None,
            "currency": ObjectId(quotation_data_dict["currency"]) if quotation_data_dict["currency"] else None,
        })
        result = await quotation_cards_collection.update_one({"_id": quotation_id}, {"$set": quotation_data_dict})
        if result.modified_count == 0:
            raise HTTPException(status_code=404)


    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.patch("/update_quotation_invoice_items")
async def update_quotation_invoice_items(
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
            if item.get("deleted"):
                if "id" not in item:
                    continue
                deleted_list.append(ObjectId(item["id"]))

            elif item.get("added") and not item.get("deleted"):
                item.pop("id", None)
                # item.pop("uid", None)
                item['company_id'] = company_id
                item['quotation_card_id'] = ObjectId(item['quotation_card_id']) if item['quotation_card_id'] else None
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
                item["updatedAt"] = security.now_utc()
                if "quotation_card_id" in item:
                    item.pop("quotation_card_id", None)
                item["name"] = ObjectId(item["name"]) if item["name"] else None
                item.pop("deleted", None)
                item.pop("added", None)
                item.pop("is_modified", None)
                modified_list.append((item_id, item))

        async with  database.client.start_session() as s:
            await s.start_transaction()
            if deleted_list:
                await quotation_cards_invoice_items_collection.delete_many(
                    {"_id": {"$in": deleted_list}}, session=s
                )

            if added_list:
                added_invoices = await quotation_cards_invoice_items_collection.insert_many(
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
                await quotation_cards_invoice_items_collection.update_one(
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


@router.get("/get_quotation_card_status/{quotation_id}")
async def get_quotation_card_status(quotation_id: str, _: dict = Depends(security.get_current_user)):
    try:
        if not ObjectId.is_valid(quotation_id):
            raise HTTPException(status_code=400, detail="Invalid job_id format")

        quotation_id = ObjectId(quotation_id)

        result = await quotation_cards_collection.find_one(
            {"_id": quotation_id},
            {"_id": 0, "quotation_status": 1}
        )

        if not result:
            raise HTTPException(status_code=404, detail="Quotation card not found")

        return {"status": "success", "data": result}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")

# delet this when you are sure that's noo need for it
# @router.get("/get_job_number_for_quotation/{quotation_id}")
# async def get_job_number_for_quotation(quotation_id: str, _: dict = Depends(security.get_current_user)):
#     try:
#         quotation_id = ObjectId(quotation_id)
#         result = await job_cards_collection.find_one({"quotation§_id": quotation_id}, {
#             "job_number": 1
#         })
#         return {"job_number": result}
#
#
#
#     except HTTPException:
#         raise
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")
#
#


@router.post("/search_engine_for_quotation_cards")
async def search_engine_for_quotation_cards(filter_quotations: QuotationCardSearch, data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        search_pipeline: list[dict] = []
        match_stage = {}

        if company_id:
            match_stage["company_id"] = company_id
        if filter_quotations.car_brand:
            match_stage["car_brand"] = filter_quotations.car_brand
        if filter_quotations.car_model:
            match_stage["car_model"] = filter_quotations.car_model
        if filter_quotations.quotation_number:
            match_stage["quotation_number"] = {"$regex": filter_quotations.quotation_number, "$options": "i"}
        if filter_quotations.plate_number:
            match_stage["plate_number"] = {"$regex": filter_quotations.plate_number, "$options": "i"}
        if filter_quotations.vin:
            match_stage["vehicle_identification_number"] = {"$regex": filter_quotations.vin, "$options": "i"}
        if filter_quotations.customer_name:
            match_stage["customer"] = filter_quotations.customer_name
        if filter_quotations.status:
            match_stage["quotation_status"] = filter_quotations.status

        search_pipeline.append({"$match": match_stage})

        lookups = [
            ("car_brand", "all_brands"),
            ("car_model", "all_brand_models"),
            ("color", "all_lists_values"),
            ("engine_type", "all_lists_values"),
            ("country", "all_countries"),
            ("city", "all_countries_cities"),
            ("salesman", "sales_man"),
            ("branch", "branches"),
        ]

        for local_field, collection in lookups:
            search_pipeline.append({
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
            search_pipeline.append({"$unwind": {"path": f"${local_field}_details", "preserveNullAndEmptyArrays": True}})

        search_pipeline.append({
            "$lookup": {
                "from": "entity_information",
                "localField": "customer",
                "foreignField": "_id",
                "as": "customer_details"
            }
        })
        search_pipeline.append({"$unwind": {"path": "$customer_details", "preserveNullAndEmptyArrays": True}})

        search_pipeline.append({
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
                            '_id': 1,
                            'job_number': 1
                        }
                    }
                ],
                'as': 'job_details'
            }
        })

        search_pipeline.append({
            '$unwind': {
                'path': '$job_details',
                'preserveNullAndEmptyArrays': True
            }
        })

        search_pipeline.append({
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
        }, )

        search_pipeline.append({
            '$unwind': {
                'path': '$currency_details',
                'preserveNullAndEmptyArrays': True
            }
        }, )

        search_pipeline.append({
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
        }, )

        search_pipeline.append({
            '$unwind': {
                'path': '$currency_country_details',
                'preserveNullAndEmptyArrays': True
            }
        })

        search_pipeline.append({
            '$lookup': {
                'from': 'quotation_cards_invoice_items',
                'let': {
                    'quotation_id': '$_id'
                },
                'pipeline': [
                    {
                        '$match': {
                            '$expr': {
                                '$eq': [
                                    '$quotation_card_id', '$$quotation_id'
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
        }, )

        now = datetime.now(timezone.utc)
        date_field = "quotation_date"
        date_filter = {}
        if filter_quotations.today:
            start = datetime(now.year, now.month, now.day, tzinfo=timezone.utc)
            end = start + timedelta(days=1)
            print(start, end)
            date_filter[date_field] = {"$gte": start, "$lt": end}

        elif filter_quotations.this_month:
            start = datetime(now.year, now.month, 1, tzinfo=timezone.utc)
            end = datetime(now.year + (now.month // 12), ((now.month % 12) + 1), 1)
            date_filter[date_field] = {"$gte": start, "$lt": end}

        elif filter_quotations.this_year:
            start = datetime(now.year, 1, 1, tzinfo=timezone.utc)
            end = datetime(now.year + 1, 1, 1)
            date_filter[date_field] = {"$gte": start, "$lt": end}

        elif filter_quotations.from_date or filter_quotations.to_date:
            date_filter[date_field] = {}
            if filter_quotations.from_date:
                print("from date")
                date_filter[date_field]["$gte"] = filter_quotations.from_date
            if filter_quotations.to_date:
                print("to date")
                date_filter[date_field]["$lte"] = filter_quotations.to_date
            print(date_filter)

        if date_filter:
            search_pipeline.append({"$match": date_filter})

        search_pipeline.append({
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
        })

        search_pipeline.append({
            '$addFields': {
                'car_brand_name': {
                    '$ifNull': [
                        '$car_brand_details.name', None
                    ]
                },
                'car_brand_logo': {
                    '$ifNull': [
                        '$car_brand_details.logo', None
                    ]
                },
                'car_model_name': {
                    '$ifNull': [
                        '$car_model_details.name', None
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
                'job_number': {
                    '$ifNull': [
                        '$job_details.job_number', None
                    ]
                }
            }
        })
        search_pipeline.append({
            "$facet": {
                "quotation_cards": [
                    {"$sort": {"job_date": -1}},
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
                        'job_details': 0
                    }}
                ],
                "grand_totals": [
                    {
                        "$group": {
                            "_id": None,
                            "grand_total": {"$sum": "$total_amount"},
                            "grand_vat": {"$sum": "$total_vat"},
                            "grand_net": {"$sum": "$total_net"}
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

        cursor = await quotation_cards_collection.aggregate(search_pipeline)
        result = await cursor.to_list(None)

        if result and len(result) > 0:
            data = result[0]
            quotation_cards = [serializer(r) for r in data.get("quotation_cards", [])]
            totals = data.get("grand_totals", [])
            grand_totals = totals[0] if totals else {"grand_total": 0, "grand_vat": 0, "grand_net": 0}
        else:
            quotation_cards = []
            grand_totals = {"grand_total": 0, "grand_vat": 0, "grand_net": 0}

        return {
            "quotation_cards": quotation_cards,
            "grand_totals": grand_totals
        }


    except HTTPException:
        raise
    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")

from typing import Optional, List, Any
from bson import ObjectId
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from pymongo import ReturnDocument

from app import database
from app.core import security
from app.database import get_collection
from datetime import datetime

from app.routes.counters import create_custom_counter
from app.websocket_config import manager

router = APIRouter()
job_cards_collection = get_collection("job_cards")
job_cards_invoice_items_collection = get_collection("job_cards_invoice_items")


class InvoiceItems(BaseModel):
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


class JobCard(BaseModel):
    label: Optional[str] = None
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
                    '$project': {
                        '_id': 1,
                        'line_number': 1,
                        'quantity': 1,
                        'price': 1,
                        'total': 1,
                        'net': 1,
                        'vat': 1,
                        'name': 1,
                        'description': 1,
                        'amount': 1,
                        'discount': 1,
                        'createdAt': 1,
                        'updatedAt': 1
                    }
                }
            ],
            'as': 'invoice_items_details'
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
            'quotation_details': 0,
        }
    }
]


async def get_job_card_details(job_card_id: ObjectId):
    new_pipeline = pipeline.copy()
    new_pipeline.insert(1, {
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
            new_job_counter = await create_custom_counter("JCN", "J", data, session=session)

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
                "job_number": new_job_counter['final_counter'] if new_job_counter['success'] else None
            })

            result = await job_cards_collection.insert_one(job_data_dict, session=session)
            if not result.inserted_id:
                raise HTTPException(status_code=500, detail="Failed to insert trade")

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

            return {
                "status": "success",
                "message": "Job card added successfully",
                "job_card_id": str(result.inserted_id)
            }


        except Exception as e:
            await session.abort_transaction()
            print(e)
            raise HTTPException(status_code=500, detail=str(e))

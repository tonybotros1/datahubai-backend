from typing import Optional

from bson import ObjectId
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from pymongo import ReturnDocument
from app.core import security
from app.database import get_collection
from datetime import datetime
from app.websocket_config import manager

router = APIRouter()
companies_collection = get_collection("companies")


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


class Variables(BaseModel):
    incentive_percentage: Optional[float] = None
    vat_percentage: Optional[float] = None
    tax_number: Optional[str] = None


@router.get("/get_company_variables_and_details")
async def get_company_variables_and_details(data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        pipeline = [
            {
                '$match': {
                    '_id': company_id
                }
            }, {
                '$lookup': {
                    'from': 'all_lists_values',
                    'let': {
                        'industry_id': '$industry'
                    },
                    'pipeline': [
                        {
                            '$match': {
                                '$expr': {
                                    '$eq': [
                                        '$_id', '$$industry_id'
                                    ]
                                }
                            }
                        }, {
                            '$project': {
                                'name': 1
                            }
                        }
                    ],
                    'as': 'industry_details'
                }
            }, {
                '$unwind': {
                    'path': '$industry_details',
                    'preserveNullAndEmptyArrays': True
                }
            }, {
                '$lookup': {
                    'from': 'sys-users',
                    'localField': 'owner_id',
                    'foreignField': '_id',
                    'as': 'owner_details'
                }
            }, {
                '$unwind': {
                    'path': '$owner_details',
                    'preserveNullAndEmptyArrays': True
                }
            }, {
                '$lookup': {
                    'from': 'all_countries',
                    'let': {
                        'country_id': '$owner_details.country'
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
                        'city_id': '$owner_details.city'
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
                    'from': 'sys-roles',
                    'let': {
                        'roles_ids': '$owner_details.roles'
                    },
                    'pipeline': [
                        {
                            '$match': {
                                '$expr': {
                                    '$in': [
                                        '$_id', '$$roles_ids'
                                    ]
                                }
                            }
                        }, {
                            '$project': {
                                'role_name': 1
                            }
                        }
                    ],
                    'as': 'roles_details'
                }
            }, {
                '$addFields': {
                    'industry_name': {
                        '$ifNull': [
                            '$industry_details.name', None
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
                    'owner_name': {
                        '$ifNull': [
                            '$owner_details.user_name', None
                        ]
                    },
                    'owner_email': {
                        '$ifNull': [
                            '$owner_details.email', None
                        ]
                    },
                    'owner_phone': {
                        '$ifNull': [
                            '$owner_details.phone_number', None
                        ]
                    },
                    'owner_address': {
                        '$ifNull': [
                            '$owner_details.address', None
                        ]
                    },
                    'incentive_percentage': {
                        '$ifNull': [
                            '$incentive_percentage', None
                        ]
                    },
                    'vat_percentage': {
                        '$ifNull': [
                            '$vat_percentage', None
                        ]
                    },
                    'tax_number': {
                        '$ifNull': [
                            '$tax_number', None
                        ]
                    }
                }
            }, {
                '$project': {
                    'industry_details': 0,
                    'country_details': 0,
                    'city_details': 0,
                    'owner_details': 0
                }
            }
        ]
        cursor = await companies_collection.aggregate(pipeline)
        result = await cursor.next()
        serialized = serializer(result)
        return {"company_variables": serialized}

    except HTTPException:
        raise
    except Exception as error:
        print(error)
        raise HTTPException(status_code=500, detail=str(error))


@router.patch("/update_company_variables")
async def update_company_variables(
        var: Variables,
        data: dict = Depends(security.get_current_user)
):
    try:
        company_id = data.get("company_id")
        var = var.model_dump(exclude_unset=True)
        if not company_id:
            raise HTTPException(status_code=400, detail="Missing company_id in user data")

        result = await companies_collection.update_one(
            {"_id": ObjectId(company_id)},
            {"$set": {"vat_percentage" : var['vat_percentage'], "tax_number" : var['tax_number'],"incentive_percentage" : var['incentive_percentage']}},
        )

        if result.matched_count == 0:
            raise HTTPException(status_code=404, detail="Company not found")

        return {"status": "success", "modified_count": result.modified_count}

    except HTTPException:
        raise
    except Exception as error:
        print(f"❌ Error updating company variables: {error}")
        raise HTTPException(status_code=500, detail=str(error))

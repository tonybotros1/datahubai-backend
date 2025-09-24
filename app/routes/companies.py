import json
from datetime import datetime
from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException, status, Form, UploadFile, File
from pymongo.errors import DuplicateKeyError
from app import database
from app.core import security
from app.database import get_collection
from app.websocket_config import manager
from app.widgets import upload_images

router = APIRouter()
users_collection = get_collection("sys-users")
companies_collection = get_collection("companies")

pipeline = [
    {
        '$lookup': {
            'from': 'sys-users',
            'localField': 'owner_id',
            'foreignField': '_id',
            'as': 'main_user'
        }
    }, {
        '$unwind': {
            'path': '$main_user',
            'preserveNullAndEmptyArrays': True
        }
    }, {
        '$lookup': {
            'from': 'all_countries',
            'let': {
                'country_id': '$main_user.country'
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
            'as': 'main_user_country'
        }
    }, {
        '$unwind': {
            'path': '$main_user_country',
            'preserveNullAndEmptyArrays': True
        }
    }, {
        '$lookup': {
            'from': 'all_countries_cities',
            'let': {
                'city_id': '$main_user.city'
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
            'as': 'main_user_city'
        }
    }, {
        '$unwind': {
            'path': '$main_user_city',
            'preserveNullAndEmptyArrays': True
        }
    }, {
        '$lookup': {
            'from': 'sys-roles',
            'let': {
                'role_ids': '$main_user.roles'
            },
            'pipeline': [
                {
                    '$match': {
                        '$expr': {
                            '$in': [
                                '$_id', '$$role_ids'
                            ]
                        }
                    }
                }, {
                    '$project': {
                        '_id': 1,
                        'role_name': 1
                    }
                }
            ],
            'as': 'main_user_roles'
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
            'as': 'company_industry'
        }
    }, {
        '$unwind': {
            'path': '$company_industry',
            'preserveNullAndEmptyArrays': True
        }
    }, {
        '$project': {
            '_id': 1,
            'company_name': 1,
            'status': 1,
            'industry': '$company_industry.name',
            'industry_id': '$company_industry._id',
            'company_logo_url': 1,
            'company_logo_public_id': 1,
            'user_id': '$main_user._id',
            'user_email': '$main_user.email',
            'user_name': '$main_user.user_name',
            'user_phone_number': '$main_user.phone_number',
            'user_address': '$main_user.address',
            'user_expiry_date': '$main_user.expiry_date',
            'user_country': '$main_user_country.name',
            'user_country_id': '$main_user_country._id',
            'user_city': '$main_user_city.name',
            'user_city_id': '$main_user_city._id',
            'main_user_roles': '$main_user_roles',
            'createdAt': 1,
            'updatedAt': 1
        }
    }
]


def serialize_doc(doc):
    if isinstance(doc, ObjectId):
        return str(doc)
    elif isinstance(doc, dict):
        return {key: serialize_doc(value) for key, value in doc.items()}
    elif isinstance(doc, list):
        return [serialize_doc(item) for item in doc]
    elif isinstance(doc, datetime):
        return doc.isoformat()
    else:
        return doc


@router.get("/get_user_and_company_details")
async def get_user_and_company_details(user_data: dict = Depends(security.get_current_user)):
    try:
        user_id = user_data.get("sub")
        if not user_id:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User ID not found in token")
        main_pipeline = [
            {
                "$match": {"_id": ObjectId(user_id)}
            },
            {
                "$lookup": {
                    "from": "companies",
                    "localField": "company_id",
                    "foreignField": "_id",
                    "as": "company",
                }
            },
            {
                "$unwind": "$company"
            },
            {
                "$project": {
                    "_id": 1,
                    "user_name": 1,
                    "company_name": "$company.company_name",
                    "company_logo": "$company.company_logo_url",
                    "createdAt": 1,
                    "email": 1,
                    "expiry_date": 1
                }
            }

        ]
        cursor = await users_collection.aggregate(main_pipeline)
        result = await cursor.to_list()

        data = result[0]
        return {"data": serialize_doc(data)}


    except Exception as e:
        return {"message": str(e)}


async def get_company_details(company_id: ObjectId):
    try:

        new_pipeline = pipeline.copy()
        new_pipeline.insert(1, {
            "$match": {
                "_id": company_id
            }
        })
        cursor = await companies_collection.aggregate(new_pipeline)
        results = await cursor.to_list()
        return results

    except Exception as e:
        raise e


@router.get("/get_all_companies")
async def get_all_companies(_: dict = Depends(security.get_current_user)):
    try:
        new_pipeline = pipeline.copy()
        cursor = await companies_collection.aggregate(new_pipeline)
        results = await cursor.to_list()
        return {"companies": [serialize_doc(c) for c in results]}

    except Exception as e:
        raise e


@router.post("/register_company")
async def register_company(
        company_name: str = Form(None),
        admin_email: str = Form(None),
        admin_password: str = Form(None),
        industry: str = Form(None),
        company_logo: UploadFile = File(None),  #
        roles_ids: str = Form(None),  #
        admin_name: str = Form(None),
        phone_number: str = Form(None),
        address: str = Form(None),
        country: str = Form(None),
        city: str = Form(None),
        _: dict = Depends(security.get_current_user)
):
    async with database.client.start_session() as s:
        try:
            await s.start_transaction()

            company_logo_url = ""
            company_logo_public_id = ""
            if company_logo:
                result = await upload_images.upload_image(company_logo, 'companies')
                company_logo_url = result["url"]
                company_logo_public_id = result["public_id"]
            role_list = json.loads(roles_ids)

            role_ids_list = [ObjectId(r.strip()) for r in role_list]

            company_doc = {
                "company_name": company_name,
                "owner_id": None,
                "status": True,
                "createdAt": security.now_utc(),
                "updatedAt": security.now_utc(),
                "industry": ObjectId(industry) if industry else "",
                "company_logo_url": company_logo_url,
                "company_logo_public_id": company_logo_public_id,
            }
            res_company = await companies_collection.insert_one(company_doc, session=s)

            owner_doc = {
                "company_id": res_company.inserted_id,
                "email": admin_email.lower(),
                "user_name": admin_name,
                "password_hash": security.pwd_ctx.hash(admin_password),
                "roles": role_ids_list,
                "status": True,
                "expiry_date": security.one_month_from_now_utc(),
                "createdAt": security.now_utc(),
                "updatedAt": security.now_utc(),
                "phone_number": phone_number,
                "address": address,
                "country": ObjectId(country) if country else "",
                "city": ObjectId(city) if city else "",
            }
            res_owner = await users_collection.insert_one(owner_doc, session=s)

            # 3. تحديث الشركة وربطها بالـ owner
            await companies_collection.update_one(
                {"_id": res_company.inserted_id},
                {"$set": {"owner_id": res_owner.inserted_id}},
                session=s
            )

            await s.commit_transaction()
            details = await get_company_details(res_company.inserted_id)
            serialized = [serialize_doc(c) for c in details]
            await manager.broadcast({
                "type": "company_created",
                "data": serialized
            })

            return {
                "company_id": str(res_company.inserted_id),
                "owner_id": str(res_owner.inserted_id),
                "message": "Company and owner registered successfully"
            }

        except DuplicateKeyError as e:
            await s.abort_transaction()
            if "company_name" in str(e):
                raise HTTPException(status_code=400, detail="Company name already exists")
            elif "email" in str(e):
                raise HTTPException(status_code=400, detail="Email already exists")
            else:
                raise HTTPException(status_code=400, detail="Duplicate entry")

        except Exception as e:
            # 👇 rollback إذا صار خطأ
            await s.abort_transaction()
            raise HTTPException(status_code=500, detail=f"Registration failed: {str(e)}")

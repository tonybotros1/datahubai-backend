from datetime import datetime
from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException, status, Form, UploadFile, File, Body
from pymongo.errors import DuplicateKeyError, PyMongoError
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
        new_pipeline.insert(0, {
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
        roles_ids: list[str] = Form(None),  #
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
            role_ids_list = []
            if roles_ids:
                role_ids_list = [ObjectId(r.strip()) for r in roles_ids]

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
            serialized = serialize_doc(details[0])
            await manager.broadcast({
                "type": "company_created",
                "data": serialized
            })

            return {
                "company_id": str(res_company.inserted_id),
                "owner_id": str(res_owner.inserted_id),
                "message": "Company and owner registered successfully",
                "data": serialized
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
            print(e)
            # 👇 rollback إذا صار خطأ
            await s.abort_transaction()
            raise HTTPException(status_code=500, detail=f"Registration failed: {str(e)}")


@router.patch("/update_company/{company_id}/{user_id}")
async def update_company(company_id: str, user_id: str, company_name: str = Form(None),
                         admin_email: str = Form(None),
                         admin_password: str = Form(None),
                         industry: str = Form(None),
                         company_logo: UploadFile = File(None),  #
                         roles_ids: list[str] = Form(None),  #
                         admin_name: str = Form(None),
                         phone_number: str = Form(None),
                         address: str = Form(None),
                         country: str = Form(None),
                         city: str = Form(None), _: dict = Depends(security.get_current_user)):
    async with database.client.start_session() as s:
        try:
            await s.start_transaction()
            current_company = await companies_collection.find_one({"_id": ObjectId(company_id)}, session=s)
            if not current_company:
                raise HTTPException(status_code=404, detail="Company not found")
            company_doc = {
                "company_name": company_name,
                "updatedAt": security.now_utc(),
                "industry": ObjectId(industry) if industry else "",
            }
            if company_logo:
                if current_company.get("company_logo_url") and current_company["company_logo_url"]:
                    await upload_images.delete_image_from_server(current_company["company_logo_public_id"])
                result = await upload_images.upload_image(company_logo, 'companies')
                company_logo_url = result["url"]
                company_logo_public_id = result["public_id"]
                company_doc["company_logo_url"] = company_logo_url
                company_doc["company_logo_public_id"] = company_logo_public_id
            role_ids_list = []
            if roles_ids:
                role_ids_list = [ObjectId(r.strip()) for r in roles_ids]

            await companies_collection.update_one({"_id": ObjectId(company_id)}, {"$set": company_doc},
                                                  session=s)

            owner_doc = {
                "email": admin_email.lower(),
                "user_name": admin_name,
                "roles": role_ids_list,
                "updatedAt": security.now_utc(),
                "phone_number": phone_number,
                "address": address,
                "country": ObjectId(country) if country else "",
                "city": ObjectId(city) if city else "",
            }
            if admin_password:
                owner_doc["password_hash"] = security.pwd_ctx.hash(admin_password)
            await users_collection.update_one({"_id": ObjectId(user_id)}, {"$set": owner_doc}, session=s)
            await s.commit_transaction()
            details = await get_company_details(ObjectId(company_id))
            serialized = serialize_doc(details[0])
            await manager.broadcast({
                "type": "company_updated",
                "data": serialized
            })
        except DuplicateKeyError as e:
            await s.abort_transaction()
            if "company_name" in str(e):
                raise HTTPException(status_code=400, detail="Company name already exists")
            elif "email" in str(e):
                raise HTTPException(status_code=400, detail="Email already exists")
            else:
                raise HTTPException(status_code=400, detail="Duplicate entry")
        except Exception as e:
            print(e)
            await s.abort_transaction()
            raise HTTPException(status_code=500, detail=f"Registration failed: {str(e)}")


@router.delete("/delete_company/{company_id}/{user_id}")
async def delete_company(company_id: str, user_id: str, _: dict = Depends(security.get_current_user)):
    try:
        if not company_id:
            raise HTTPException(status_code=400, detail="Invalid Company ID")
        if not user_id:
            raise HTTPException(status_code=400, detail="Invalid User ID")

        company_id = ObjectId(company_id)
        user_id = ObjectId(user_id)

        async with database.client.start_session() as session:
            await session.start_transaction()

            # 1. Find and delete the company in one step
            company = await companies_collection.find_one_and_delete(
                {"_id": company_id}, session=session
            )

            if not company:
                raise HTTPException(status_code=404, detail="Company not found")

            # 2. Delete the owner
            await users_collection.delete_one({"_id": user_id}, session=session)

            # 3. Delete logo if exists
            if company.get("company_logo_public_id"):
                try:
                    await upload_images.delete_image_from_server(company["company_logo_public_id"])
                except Exception as e:
                    print(f"⚠️ Failed to delete logo: {e}")

            await session.commit_transaction()

            # 4. Broadcast deletion
            await manager.broadcast({
                "type": "company_deleted",
                "data": {"_id": str(company_id)},
            })

            return {"message": "Company and owner deleted successfully"}

    except PyMongoError as e:
        print(e)
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")


@router.patch("/change_company_status/{company_id}")
async def change_user_status(company_id: str, company_status: bool = Body(None),
                             _: dict = Depends(security.get_current_user)):
    try:
        result = await companies_collection.update_one(
            {"_id": ObjectId(company_id)}, {"$set": {"status": company_status, "updatedAt": security.now_utc()}},
        )
        if not result:
            raise HTTPException(status_code=404, detail="User not found")
        await manager.broadcast({
            "type": "company_status_updated",
            "data": {"status": company_status, "_id": company_id}
        })
    except Exception as error:
        return {"message": str(error)}


@router.get("/get_current_company_details")
async def get_current_company_details(data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        user_id = ObjectId(data.get("sub"))
        my_pipeline = [
            {
                '$match': {
                    '_id': company_id
                }
            }, {
                '$lookup': {
                    'from': 'sys-users',
                    'let': {
                        'current_user_id': user_id
                    },
                    'pipeline': [
                        {
                            '$match': {
                                '$expr': {
                                    '$eq': [
                                        '$_id', '$$current_user_id'
                                    ]
                                }
                            }
                        }, {
                            '$lookup': {
                                'from': 'branches',
                                'localField': 'primary_branch',
                                'foreignField': '_id',
                                'as': 'branch_details'
                            }
                        }, {
                            '$unwind': {
                                'path': '$branch_details',
                                'preserveNullAndEmptyArrays': True
                            }
                        }
                    ],
                    'as': 'current_user_details'
                }
            }, {
                '$unwind': {
                    'path': '$current_user_details',
                    'preserveNullAndEmptyArrays': True
                }
            }, {
                '$addFields': {
                    'current_user_branch_name': {
                        '$ifNull': [
                            '$current_user_details.branch_details.name', None
                        ]
                    },
                    'current_user_branch_id': {
                        '$ifNull': [
                            '$current_user_details.branch_details._id', None
                        ]
                    }
                }
            }, {
                '$lookup': {
                    'from': 'sys-users',
                    'let': {
                        'owner_id': '$owner_id'
                    },
                    'pipeline': [
                        {
                            '$match': {
                                '$expr': {
                                    '$eq': [
                                        '$_id', '$$owner_id'
                                    ]
                                }
                            }
                        }, {
                            '$project': {
                                '_id': 1,
                                'country': 1,
                                'city': 1
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
                '$lookup': {
                    'from': 'all_countries',
                    'let': {
                        'country_id': '$user_details.country'
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
                                'vat': 1,
                                'name': 1,
                                'currency_code': 1,
                                'subunit_name': 1
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
                        'city_id': '$user_details.city'
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
                    'from': 'currencies',
                    'let': {
                        'country_id': '$country_details._id'
                    },
                    'pipeline': [
                        {
                            '$match': {
                                '$expr': {
                                    '$eq': [
                                        '$country_id', '$$country_id'
                                    ]
                                }
                            }
                        }, {
                            '$project': {
                                '_id': 1,
                                'rate': 1
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
                '$addFields': {
                    'country': {
                        '$ifNull': [
                            '$country_details.name', None
                        ]
                    },
                    'country_id': {
                        '$ifNull': [
                            '$country_details._id', None
                        ]
                    },
                    'country_vat': {
                        '$ifNull': [
                            '$country_details.vat', None
                        ]
                    },
                    'city': {
                        '$ifNull': [
                            '$city_details.name', None
                        ]
                    },
                    'city_id': {
                        '$ifNull': [
                            '$city_details._id', None
                        ]
                    },
                    'currency_id': {
                        '$ifNull': [
                            '$currency_details._id', None
                        ]
                    },
                    'currency_rate': {
                        '$ifNull': [
                            '$currency_details.rate', None
                        ]
                    },
                    'currency_code': {
                        '$ifNull': [
                            '$country_details.currency_code', None
                        ]
                    },
                    'subunit_name': {
                        '$ifNull': [
                            '$country_details.subunit_name', None
                        ]
                    }
                }
            }, {
                '$project': {
                    'user_details': 0,
                    'country_details': 0,
                    'currency_details': 0,
                    'city_details': 0,
                    'current_user_details': 0
                }
            }
        ]
        cursor = await companies_collection.aggregate(my_pipeline)
        result = await cursor.to_list(None)
        serialized = serialize_doc(result[0])
        return {"company_details": serialized}


    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

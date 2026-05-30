from datetime import datetime
from bson import ObjectId
from bson.errors import InvalidId
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
roles_collection = get_collection("sys-roles")
refresh_tokens_collection = get_collection("refresh_tokens")

COMPANY_MANAGEMENT_ROUTE = "/defineCompany"
COMPANY_SCOPED_COLLECTION_NAMES = [
    "account_transfers",
    "all_banks",
    "all_brand_models",
    "all_brands",
    "all_capitals",
    "all_general_expenses",
    "all_lists",
    "all_lists_values",
    "all_outstanding",
    "all_payments",
    "all_payments_invoices",
    "all_receipts",
    "all_receipts_invoices",
    "all_technicians",
    "all_trades",
    "all_trades_items",
    "all_trades_purchase_agreement_items",
    "all_trades_transfers",
    "ap_invoices",
    "ap_invoices_items",
    "ap_payment_types",
    "attachment",
    "balances",
    "balances_based_elements",
    "batch_payment_process",
    "batch_payment_process_items",
    "branches",
    "converters",
    "counters",
    "currencies",
    "employees",
    "employees_address",
    "employees_bank_accounts",
    "employees_contacts_and_relatives",
    "employees_email",
    "employees_health_card",
    "employees_leaves",
    "employees_loan_and_advances",
    "employees_nationality",
    "employees_payrolls",
    "employees_phone",
    "entity_information",
    "favourite_screens",
    "inventory_items",
    "invoice_items",
    "issuing",
    "issuing_converters_details",
    "issuing_items_details",
    "job_cards",
    "job_cards_internal_notes",
    "job_cards_invoice_items",
    "job_cards_inspection_reports",
    "leave_types",
    "legislations",
    "loan_and_advances_types",
    "payroll",
    "payroll_elements",
    "payroll_elements_based_elements",
    "payroll_period_details",
    "payroll_runs",
    "payroll_runs_employees",
    "payroll_runs_employees_elements",
    "public_holidays",
    "quotation_cards",
    "quotation_cards_internal_notes",
    "quotation_cards_invoice_items",
    "receiving",
    "receiving_items",
    "sales_man",
    "system_variables",
    "time_sheets",
    "to_do_list",
    "to_do_list_description",
]


def _object_id(value: str | ObjectId | None, field_name: str) -> ObjectId:
    try:
        return value if isinstance(value, ObjectId) else ObjectId(str(value))
    except (InvalidId, TypeError):
        raise HTTPException(status_code=400, detail=f"Invalid {field_name}")


def _required(value: str | None, field_name: str) -> str:
    clean_value = (value or "").strip()
    if not clean_value:
        raise HTTPException(status_code=400, detail=f"{field_name} is required")
    return clean_value


def _object_id_from_required(value: str | None, field_name: str) -> ObjectId:
    return _object_id(_required(value, field_name), field_name)


def _role_object_ids(roles_ids: list[str] | str | None) -> list[ObjectId]:
    if not roles_ids:
        raise HTTPException(status_code=400, detail="At least one role is required")

    values = roles_ids if isinstance(roles_ids, list) else [roles_ids]
    role_ids: list[ObjectId] = []
    for item in values:
        for raw_id in str(item).split(","):
            clean_id = raw_id.strip()
            if clean_id:
                role_ids.append(_object_id(clean_id, "role ID"))

    if not role_ids:
        raise HTTPException(status_code=400, detail="At least one role is required")
    return role_ids


async def _ensure_company_management_access(user_data: dict) -> None:
    user_id = _object_id(user_data.get("sub"), "user ID")
    role_values = user_data.get("role") or []
    if isinstance(role_values, str):
        role_values = [role_values]

    user = await users_collection.find_one({"_id": user_id}, {"roles": 1, "is_admin": 1})
    if user and user.get("is_admin") is True:
        return

    if user and user.get("roles"):
        role_values = list({*role_values, *[str(role) for role in user.get("roles", [])]})

    role_ids = []
    for role_id in role_values:
        try:
            role_ids.append(ObjectId(str(role_id)))
        except (InvalidId, TypeError):
            continue

    if not role_ids:
        raise HTTPException(status_code=403, detail="Not allowed to manage companies")

    cursor = await roles_collection.aggregate([
        {"$match": {"_id": {"$in": role_ids}}},
        {
            "$lookup": {
                "from": "menus",
                "localField": "menu_id",
                "foreignField": "_id",
                "as": "menu",
            }
        },
        {"$unwind": {"path": "$menu", "preserveNullAndEmptyArrays": False}},
        {
            "$graphLookup": {
                "from": "menus",
                "startWith": "$menu.children",
                "connectFromField": "children",
                "connectToField": "_id",
                "as": "child_menus",
            }
        },
        {
            "$match": {"$or": [{"menu.route_name": COMPANY_MANAGEMENT_ROUTE}, {"child_menus.route_name": COMPANY_MANAGEMENT_ROUTE}]}
        },
        {"$limit": 1},
    ])
    allowed_roles = await cursor.to_list(length=1)
    if not allowed_roles:
        raise HTTPException(status_code=403, detail="Not allowed to manage companies")


def _company_payload(
        company_name: str | None,
        admin_email: str | None,
        industry: str | None,
        roles_ids: list[str] | str | None,
        admin_name: str | None,
        phone_number: str | None,
        address: str | None,
        country: str | None,
        city: str | None,
        admin_password: str | None = None,
        require_password: bool = False,
) -> dict:
    clean_password = (admin_password or "").strip()
    if require_password:
        clean_password = _required(clean_password, "Password")

    return {
        "company_name": _required(company_name, "Company name"),
        "admin_email": _required(admin_email, "Email").lower(),
        "admin_password": clean_password,
        "industry": _object_id_from_required(industry, "industry"),
        "roles": _role_object_ids(roles_ids),
        "admin_name": _required(admin_name, "Name"),
        "phone_number": _required(phone_number, "Phone number"),
        "address": _required(address, "Address"),
        "country": _object_id_from_required(country, "country"),
        "city": _object_id_from_required(city, "city"),
    }


async def _safe_delete_logo(public_id: str | None) -> None:
    if not public_id:
        return
    try:
        await upload_images.delete_image_from_server(public_id)
    except Exception as e:
        print(f"Failed to delete company logo: {e}")


async def _delete_company_scoped_documents(company_id: ObjectId, session) -> None:
    company_id_values = [company_id, str(company_id)]
    for collection_name in COMPANY_SCOPED_COLLECTION_NAMES:
        await get_collection(collection_name).delete_many(
            {"company_id": {"$in": company_id_values}},
            session=session,
        )

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
                'role_ids': {'$ifNull': ['$main_user.roles', []]}
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
async def get_all_companies(user_data: dict = Depends(security.get_current_user)):
    try:
        await _ensure_company_management_access(user_data)
        new_pipeline = pipeline.copy()
        cursor = await companies_collection.aggregate(new_pipeline)
        results = await cursor.to_list()
        return {"companies": [serialize_doc(c) for c in results]}

    except HTTPException:
        raise
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
        data: dict = Depends(security.get_current_user)
):
    await _ensure_company_management_access(data)
    payload = _company_payload(
        company_name=company_name,
        admin_email=admin_email,
        admin_password=admin_password,
        industry=industry,
        roles_ids=roles_ids,
        admin_name=admin_name,
        phone_number=phone_number,
        address=address,
        country=country,
        city=city,
        require_password=True,
    )
    uploaded_logo_public_id = None
    async with database.client.start_session() as s:
        try:
            await s.start_transaction()
            my_company_id = data.get("company_id")
            company_logo_url = ""
            company_logo_public_id = ""
            if company_logo:
                result = await upload_images.upload_image(company_logo, 'companies')
                company_logo_url = result["url"]
                company_logo_public_id = result["public_id"]
                uploaded_logo_public_id = company_logo_public_id

            company_doc = {
                "company_name": payload["company_name"],
                "owner_id": None,
                "status": True,
                "createdAt": security.now_utc(),
                "updatedAt": security.now_utc(),
                "industry": payload["industry"],
                "company_logo_url": company_logo_url,
                "company_logo_public_id": company_logo_public_id,
            }
            res_company = await companies_collection.insert_one(company_doc, session=s)

            owner_doc = {
                "company_id": res_company.inserted_id,
                "email": payload["admin_email"],
                "user_name": payload["admin_name"],
                "password_hash": security.pwd_ctx.hash(payload["admin_password"]),
                "roles": payload["roles"],
                "status": True,
                "expiry_date": security.one_month_from_now_utc(),
                "createdAt": security.now_utc(),
                "updatedAt": security.now_utc(),
                "phone_number": payload["phone_number"],
                "address": payload["address"],
                "country": payload["country"],
                "city": payload["city"],
            }
            res_owner = await users_collection.insert_one(owner_doc, session=s)

            await companies_collection.update_one(
                {"_id": res_company.inserted_id},
                {"$set": {"owner_id": res_owner.inserted_id}},
                session=s
            )

            await s.commit_transaction()
            details = await get_company_details(res_company.inserted_id)
            serialized = serialize_doc(details[0])
            await manager.send_to_company(my_company_id, {
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
            await _safe_delete_logo(uploaded_logo_public_id)
            if "company_name" in str(e):
                raise HTTPException(status_code=400, detail="Company name already exists")
            elif "email" in str(e):
                raise HTTPException(status_code=400, detail="Email already exists")
            else:
                raise HTTPException(status_code=400, detail="Duplicate entry")

        except HTTPException:
            await s.abort_transaction()
            await _safe_delete_logo(uploaded_logo_public_id)
            raise

        except Exception as e:
            await s.abort_transaction()
            await _safe_delete_logo(uploaded_logo_public_id)
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
                         city: str = Form(None), data: dict = Depends(security.get_current_user)):
    await _ensure_company_management_access(data)
    target_company_id = _object_id(company_id, "company ID")
    target_user_id = _object_id(user_id, "user ID")
    payload = _company_payload(
        company_name=company_name,
        admin_email=admin_email,
        admin_password=admin_password,
        industry=industry,
        roles_ids=roles_ids,
        admin_name=admin_name,
        phone_number=phone_number,
        address=address,
        country=country,
        city=city,
    )
    new_logo_public_id = None
    old_logo_public_id = None
    async with database.client.start_session() as s:
        try:
            await s.start_transaction()
            my_company_id = data.get("company_id")
            current_company = await companies_collection.find_one({"_id": target_company_id}, session=s)
            if not current_company:
                raise HTTPException(status_code=404, detail="Company not found")
            if current_company.get("owner_id") != target_user_id:
                raise HTTPException(status_code=400, detail="User is not the company owner")
            owner = await users_collection.find_one(
                {"_id": target_user_id, "company_id": target_company_id},
                {"_id": 1},
                session=s,
            )
            if not owner:
                raise HTTPException(status_code=404, detail="Company owner not found")

            company_doc = {
                "company_name": payload["company_name"],
                "updatedAt": security.now_utc(),
                "industry": payload["industry"],
            }
            if company_logo:
                result = await upload_images.upload_image(company_logo, 'companies')
                new_logo_public_id = result["public_id"]
                old_logo_public_id = current_company.get("company_logo_public_id")
                company_doc["company_logo_url"] = result["url"]
                company_doc["company_logo_public_id"] = new_logo_public_id

            await companies_collection.update_one({"_id": target_company_id}, {"$set": company_doc}, session=s)

            owner_doc = {
                "email": payload["admin_email"],
                "user_name": payload["admin_name"],
                "roles": payload["roles"],
                "updatedAt": security.now_utc(),
                "phone_number": payload["phone_number"],
                "address": payload["address"],
                "country": payload["country"],
                "city": payload["city"],
            }
            if payload["admin_password"]:
                owner_doc["password_hash"] = security.pwd_ctx.hash(payload["admin_password"])
            await users_collection.update_one({"_id": target_user_id}, {"$set": owner_doc}, session=s)
            await s.commit_transaction()

            if new_logo_public_id and old_logo_public_id:
                await _safe_delete_logo(old_logo_public_id)
            details = await get_company_details(target_company_id)
            serialized = serialize_doc(details[0])
            await manager.send_to_company(my_company_id, {
                "type": "company_updated",
                "data": serialized
            })
            return {"message": "Company updated successfully", "data": serialized}
        except DuplicateKeyError as e:
            await s.abort_transaction()
            await _safe_delete_logo(new_logo_public_id)
            if "company_name" in str(e):
                raise HTTPException(status_code=400, detail="Company name already exists")
            elif "email" in str(e):
                raise HTTPException(status_code=400, detail="Email already exists")
            else:
                raise HTTPException(status_code=400, detail="Duplicate entry")
        except HTTPException:
            await s.abort_transaction()
            await _safe_delete_logo(new_logo_public_id)
            raise
        except Exception as e:
            print(e)
            await s.abort_transaction()
            await _safe_delete_logo(new_logo_public_id)
            raise HTTPException(status_code=500, detail=f"Registration failed: {str(e)}")


@router.delete("/delete_company/{company_id}/{user_id}")
async def delete_company(company_id: str, user_id: str, data: dict = Depends(security.get_current_user)):
    await _ensure_company_management_access(data)
    target_company_id = _object_id(company_id, "company ID")
    target_user_id = _object_id(user_id, "user ID")
    company_logo_public_id = None
    try:
        my_company_id = data.get("company_id")
        async with database.client.start_session() as session:
            await session.start_transaction()

            company = await companies_collection.find_one({"_id": target_company_id}, session=session)
            if not company:
                raise HTTPException(status_code=404, detail="Company not found")
            if company.get("owner_id") != target_user_id:
                raise HTTPException(status_code=400, detail="User is not the company owner")

            owner = await users_collection.find_one(
                {"_id": target_user_id, "company_id": target_company_id},
                {"_id": 1},
                session=session,
            )
            if not owner:
                raise HTTPException(status_code=404, detail="Company owner not found")

            users = await users_collection.find(
                {"company_id": target_company_id},
                {"_id": 1},
                session=session,
            ).to_list(None)
            user_ids = [user["_id"] for user in users]
            company_logo_public_id = company.get("company_logo_public_id")

            await _delete_company_scoped_documents(target_company_id, session)
            if user_ids:
                await refresh_tokens_collection.delete_many({"user_id": {"$in": user_ids}}, session=session)
            await users_collection.delete_many({"company_id": target_company_id}, session=session)
            await companies_collection.delete_one({"_id": target_company_id}, session=session)

            await session.commit_transaction()
            await _safe_delete_logo(company_logo_public_id)

            await manager.send_to_company(my_company_id, {
                "type": "company_deleted",
                "data": {"_id": str(target_company_id)},
            })

            return {"message": "Company and users deleted successfully"}

    except HTTPException:
        raise
    except PyMongoError as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")


@router.patch("/change_company_status/{company_id}")
async def change_user_status(company_id: str, company_status: bool = Body(None),
                             data: dict = Depends(security.get_current_user)):
    try:
        await _ensure_company_management_access(data)
        if company_status is None:
            raise HTTPException(status_code=400, detail="Company status is required")
        target_company_id = _object_id(company_id, "company ID")
        my_company_id = data.get("company_id")
        result = await companies_collection.update_one(
            {"_id": target_company_id}, {"$set": {"status": company_status, "updatedAt": security.now_utc()}},
        )
        if result.matched_count == 0:
            raise HTTPException(status_code=404, detail="Company not found")
        await manager.send_to_company(my_company_id, {
            "type": "company_status_updated",
            "data": {"status": company_status, "_id": company_id}
        })
        return {"message": "Company status updated successfully"}
    except HTTPException:
        raise
    except Exception as error:
        raise HTTPException(status_code=500, detail=str(error))


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
                        }, {
                            '$project': {
                                '_id': 1,
                                'is_admin': 1,
                                'branch_details': 1,
                                'user_name': 1
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
                    'current_user_name': {
                        '$ifNull': [
                            '$current_user_details.user_name', None
                        ]
                    },
                    'current_user_id': {
                        '$ifNull': [
                            '$current_user_details._id', None
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
                                'currency_name': 1,
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
                        'country_id': '$country_details._id',
                        'company_id': '$_id'
                    },
                    'pipeline': [
                        {
                            '$match': {
                                '$expr': {
                                    '$and': [
                                        {
                                            '$eq': [
                                                '$country_id', '$$country_id'
                                            ]
                                        }, {
                                            '$eq': [
                                                '$company_id', '$$company_id'
                                            ]
                                        }
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
                    'currency_name': {
                        '$ifNull': [
                            '$country_details.currency_name', None
                        ]
                    },
                    'subunit_name': {
                        '$ifNull': [
                            '$country_details.subunit_name', None
                        ]
                    },
                    'is_admin': {
                        '$ifNull': [
                            '$current_user_details.is_admin', False
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

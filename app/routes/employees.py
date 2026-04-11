import copy
from typing import Optional, List, Any
from bson import ObjectId
from fastapi import APIRouter, HTTPException, Depends, Form, UploadFile, File, Body
from pydantic import BaseModel
from app.core import security
from app.database import get_collection
from datetime import datetime, timedelta

from app.routes.car_trading import PyObjectId
from app.routes.counters import create_custom_counter
from app.websocket_config import manager
from app.widgets import upload_images

router = APIRouter()
employees_collection = get_collection("employees")
employees_address_collection = get_collection("employees_address")
employees_nationality_collection = get_collection("employees_nationality")
employees_phone_collection = get_collection("employees_phone")
employees_email_collection = get_collection("employees_email")
employees_bank_accounts_collection = get_collection("employees_bank_accounts")
employees_leaves_collection = get_collection("employees_leaves")
employees_contacts_and_relatives_collection = get_collection("employees_contacts_and_relatives")
employees_payrolls_collection = get_collection("employees_payrolls")
attachment_collection = get_collection("attachment")
legislations_collection = get_collection("legislations")
public_holidays_collection = get_collection("public_holidays")


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


class EmployeesModel(BaseModel):
    name: Optional[str] = None
    gender: Optional[str] = None
    nationality: Optional[str] = None
    date_of_birth: Optional[datetime] = None
    martial_status: Optional[str] = None
    national_id_or_passport_number: Optional[str] = None
    email: Optional[str] = None
    phone: Optional[str] = None
    address: Optional[str] = None
    emergency_contact_name: Optional[str] = None
    emergency_contact_number: Optional[str] = None
    job_title: Optional[str] = None
    hire_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    job_description: Optional[str] = None
    status: Optional[str] = None
    department: Optional[List[str]] = None


class EmployeeAddressModel(BaseModel):
    line: Optional[str] = None
    country: Optional[str] = None
    city: Optional[str] = None


class EmployeeNationalityModel(BaseModel):
    nationality: Optional[str] = None
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None


class EmployeePhoneModel(BaseModel):
    type: Optional[str] = None
    phone: Optional[str] = None


class EmployeeEmailModel(BaseModel):
    type: Optional[str] = None
    email: Optional[str] = None


class EmployeesSearch(BaseModel):
    name: Optional[str] = None
    employer: Optional[PyObjectId] = None
    department: Optional[PyObjectId] = None
    job_title: Optional[PyObjectId] = None
    location: Optional[PyObjectId] = None
    status: Optional[str] = None
    type: Optional[str] = None
    from_date: Optional[datetime] = None
    to_date: Optional[datetime] = None


main_screen_pipeline: list[dict[str, Any]] = [
    {
        '$project': {
            'full_name': 1,
            'person_type': 1,
            'status': 1,
            'employer': 1,
            'department': 1,
            'job_title': 1,
            'location': 1,
            'all_ids': [
                '$employer', '$department', '$job_title', '$location'
            ]
        }
    }, {
        '$lookup': {
            'from': 'all_lists_values',
            'let': {
                'ids': '$all_ids'
            },
            'pipeline': [
                {
                    '$match': {
                        '$expr': {
                            '$in': [
                                '$_id', '$$ids'
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
            'as': 'lookup_data'
        }
    }, {
        '$addFields': {
            'employer_name': {
                '$let': {
                    'vars': {
                        'match': {
                            '$first': {
                                '$filter': {
                                    'input': '$lookup_data',
                                    'cond': {
                                        '$eq': [
                                            '$$this._id', '$employer'
                                        ]
                                    }
                                }
                            }
                        }
                    },
                    'in': '$$match.name'
                }
            },
            'department_name': {
                '$let': {
                    'vars': {
                        'match': {
                            '$first': {
                                '$filter': {
                                    'input': '$lookup_data',
                                    'cond': {
                                        '$eq': [
                                            '$$this._id', '$department'
                                        ]
                                    }
                                }
                            }
                        }
                    },
                    'in': '$$match.name'
                }
            },
            'job_title_name': {
                '$let': {
                    'vars': {
                        'match': {
                            '$first': {
                                '$filter': {
                                    'input': '$lookup_data',
                                    'cond': {
                                        '$eq': [
                                            '$$this._id', '$job_title'
                                        ]
                                    }
                                }
                            }
                        }
                    },
                    'in': '$$match.name'
                }
            },
            'location_name': {
                '$let': {
                    'vars': {
                        'match': {
                            '$first': {
                                '$filter': {
                                    'input': '$lookup_data',
                                    'cond': {
                                        '$eq': [
                                            '$$this._id', '$location'
                                        ]
                                    }
                                }
                            }
                        }
                    },
                    'in': '$$match.name'
                }
            },
            '_id': {
                '$toString': '$_id'
            },
            'employer': {
                '$toString': '$employer'
            },
            'department': {
                '$toString': '$department'
            },
            'job_title': {
                '$toString': '$job_title'
            },
            'location': {
                '$toString': '$location'
            }
        }
    }, {
        '$project': {
            'lookup_data': 0,
            'all_ids': 0
        }
    }
]

details_pipeline = [
    {
        '$addFields': {
            'all_ids': [
                '$employer', '$department', '$job_title', '$location', '$gender', '$martial_status'
            ]
        }
    }, {
        '$lookup': {
            'from': 'all_lists_values',
            'let': {
                'ids': '$all_ids'
            },
            'pipeline': [
                {
                    '$match': {
                        '$expr': {
                            '$in': [
                                '$_id', '$$ids'
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
            'as': 'lookup_data'
        }
    }, {
        '$lookup': {
            'from': 'all_countries',
            'localField': 'country_of_birth',
            'foreignField': '_id',
            'as': 'country_details'
        }
    }, {
        '$lookup': {
            'from': 'legislations',
            'localField': 'legislation',
            'foreignField': '_id',
            'as': 'legislation_details'
        }
    }, {
        '$lookup': {
            'from': 'employees',
            'localField': 'reporting_manager',
            'foreignField': '_id',
            'as': 'reporting_manager_details'
        }
    }, {
        '$lookup': {
            'from': 'employees_payrolls',
            'let': {
                'employee_id': '$_id'
            },
            'pipeline': [
                {
                    '$match': {
                        '$expr': {
                            '$eq': [
                                '$employee_id', '$$employee_id'
                            ]
                        }
                    }
                }, {
                    '$lookup': {
                        'from': 'payroll_elements',
                        'localField': 'name',
                        'foreignField': '_id',
                        'as': 'name_details'
                    }
                }, {
                    '$addFields': {
                        'name_value': {
                            '$ifNull': [
                                {
                                    '$first': '$name_details.name'
                                }, None
                            ]
                        },
                        '_id': {
                            '$toString': '$_id'
                        },
                        'name': {
                            '$toString': '$name'
                        }
                    }
                }, {
                    '$project': {
                        'name_details': 0,
                        'company_id': 0,
                        'employee_id': 0
                    }
                }
            ],
            'as': 'payrolls_details'
        }
    }, {
        '$lookup': {
            'from': 'employees_bank_accounts',
            'let': {
                'employee_id': '$_id'
            },
            'pipeline': [
                {
                    '$match': {
                        '$expr': {
                            '$eq': [
                                '$employee_id', '$$employee_id'
                            ]
                        }
                    }
                }, {
                    '$lookup': {
                        'from': 'all_lists_values',
                        'localField': 'bank_name',
                        'foreignField': '_id',
                        'as': 'bank_name_details'
                    }
                }, {
                    '$addFields': {
                        'bank_name_value': {
                            '$ifNull': [
                                {
                                    '$first': '$bank_name_details.name'
                                }, None
                            ]
                        },
                        '_id': {
                            '$toString': '$_id'
                        },
                        'bank_name': {
                            '$toString': '$bank_name'
                        }
                    }
                }, {
                    '$project': {
                        'bank_name_details': 0,
                        'company_id': 0,
                        'employee_id': 0
                    }
                }
            ],
            'as': 'bank_accounts_list'
        }
    }, {
        '$lookup': {
            'from': 'employees_address',
            'let': {
                'employee_id': '$_id'
            },
            'pipeline': [
                {
                    '$match': {
                        '$expr': {
                            '$eq': [
                                '$employee_id', '$$employee_id'
                            ]
                        }
                    }
                }, {
                    '$lookup': {
                        'from': 'all_countries',
                        'localField': 'country',
                        'foreignField': '_id',
                        'as': 'country_details'
                    }
                }, {
                    '$lookup': {
                        'from': 'all_countries_cities',
                        'localField': 'city',
                        'foreignField': '_id',
                        'as': 'city_details'
                    }
                }, {
                    '$addFields': {
                        'country_name': {
                            '$ifNull': [
                                {
                                    '$first': '$country_details.name'
                                }, None
                            ]
                        },
                        'city_name': {
                            '$ifNull': [
                                {
                                    '$first': '$city_details.name'
                                }, None
                            ]
                        },
                        'country': {
                            '$toString': '$country'
                        },
                        'city': {
                            '$toString': '$city'
                        },
                        '_id': {
                            '$toString': '$_id'
                        }
                    }
                }, {
                    '$project': {
                        'line': 1,
                        'country': 1,
                        'city': 1,
                        'country_name': 1,
                        'city_name': 1
                    }
                }
            ],
            'as': 'addresses_list'
        }
    }, {
        '$lookup': {
            'from': 'employees_nationality',
            'let': {
                'employee_id': '$_id'
            },
            'pipeline': [
                {
                    '$match': {
                        '$expr': {
                            '$eq': [
                                '$employee_id', '$$employee_id'
                            ]
                        }
                    }
                }, {
                    '$lookup': {
                        'from': 'all_lists_values',
                        'localField': 'nationality',
                        'foreignField': '_id',
                        'as': 'nationality_details'
                    }
                }, {
                    '$addFields': {
                        'nationality_name': {
                            '$ifNull': [
                                {
                                    '$first': '$nationality_details.name'
                                }, None
                            ]
                        },
                        'nationality': {
                            '$toString': '$nationality'
                        },
                        '_id': {
                            '$toString': '$_id'
                        }
                    }
                }, {
                    '$project': {
                        'createdAt': 0,
                        'updatedAt': 0,
                        'company_id': 0,
                        'nationality_details': 0,
                        'employee_id': 0
                    }
                }
            ],
            'as': 'nationalities_list'
        }
    }, {
        '$lookup': {
            'from': 'employees_phone',
            'let': {
                'employee_id': '$_id'
            },
            'pipeline': [
                {
                    '$match': {
                        '$expr': {
                            '$eq': [
                                '$employee_id', '$$employee_id'
                            ]
                        }
                    }
                }, {
                    '$lookup': {
                        'from': 'all_lists_values',
                        'localField': 'type',
                        'foreignField': '_id',
                        'as': 'type_details'
                    }
                }, {
                    '$addFields': {
                        'type_name': {
                            '$ifNull': [
                                {
                                    '$first': '$type_details.name'
                                }, None
                            ]
                        },
                        'type': {
                            '$toString': '$type'
                        },
                        '_id': {
                            '$toString': '$_id'
                        }
                    }
                }, {
                    '$project': {
                        'createdAt': 0,
                        'updatedAt': 0,
                        'company_id': 0,
                        'type_details': 0,
                        'employee_id': 0
                    }
                }
            ],
            'as': 'phone_list'
        }
    }, {
        '$lookup': {
            'from': 'employees_email',
            'let': {
                'employee_id': '$_id'
            },
            'pipeline': [
                {
                    '$match': {
                        '$expr': {
                            '$eq': [
                                '$employee_id', '$$employee_id'
                            ]
                        }
                    }
                }, {
                    '$lookup': {
                        'from': 'all_lists_values',
                        'localField': 'type',
                        'foreignField': '_id',
                        'as': 'type_details'
                    }
                }, {
                    '$addFields': {
                        'type_name': {
                            '$ifNull': [
                                {
                                    '$first': '$type_details.name'
                                }, None
                            ]
                        },
                        'type': {
                            '$toString': '$type'
                        },
                        '_id': {
                            '$toString': '$_id'
                        }
                    }
                }, {
                    '$project': {
                        'createdAt': 0,
                        'updatedAt': 0,
                        'company_id': 0,
                        'type_details': 0,
                        'employee_id': 0
                    }
                }
            ],
            'as': 'email_list'
        }
    }, {
        '$addFields': {
            'gender_name': {
                '$let': {
                    'vars': {
                        'match': {
                            '$first': {
                                '$filter': {
                                    'input': '$lookup_data',
                                    'cond': {
                                        '$eq': [
                                            '$$this._id', '$gender'
                                        ]
                                    }
                                }
                            }
                        }
                    },
                    'in': '$$match.name'
                }
            },
            'employer_name': {
                '$let': {
                    'vars': {
                        'match': {
                            '$first': {
                                '$filter': {
                                    'input': '$lookup_data',
                                    'cond': {
                                        '$eq': [
                                            '$$this._id', '$employer'
                                        ]
                                    }
                                }
                            }
                        }
                    },
                    'in': '$$match.name'
                }
            },
            'department_name': {
                '$let': {
                    'vars': {
                        'match': {
                            '$first': {
                                '$filter': {
                                    'input': '$lookup_data',
                                    'cond': {
                                        '$eq': [
                                            '$$this._id', '$department'
                                        ]
                                    }
                                }
                            }
                        }
                    },
                    'in': '$$match.name'
                }
            },
            'job_title_name': {
                '$let': {
                    'vars': {
                        'match': {
                            '$first': {
                                '$filter': {
                                    'input': '$lookup_data',
                                    'cond': {
                                        '$eq': [
                                            '$$this._id', '$job_title'
                                        ]
                                    }
                                }
                            }
                        }
                    },
                    'in': '$$match.name'
                }
            },
            'location_name': {
                '$let': {
                    'vars': {
                        'match': {
                            '$first': {
                                '$filter': {
                                    'input': '$lookup_data',
                                    'cond': {
                                        '$eq': [
                                            '$$this._id', '$location'
                                        ]
                                    }
                                }
                            }
                        }
                    },
                    'in': '$$match.name'
                }
            },
            'martial_status_name': {
                '$let': {
                    'vars': {
                        'match': {
                            '$first': {
                                '$filter': {
                                    'input': '$lookup_data',
                                    'cond': {
                                        '$eq': [
                                            '$$this._id', '$martial_status'
                                        ]
                                    }
                                }
                            }
                        }
                    },
                    'in': '$$match.name'
                }
            },
            'country_of_birth_name': {
                '$ifNull': [
                    {
                        '$first': '$country_details.name'
                    }, None
                ]
            },
            'legislation_name': {
                '$ifNull': [
                    {
                        '$first': '$legislation_details.name'
                    }, None
                ]
            },
            '_id': {
                '$toString': '$_id'
            },
            'legislation': {
                '$toString': '$legislation'
            },
            'employer': {
                '$toString': '$employer'
            },
            'department': {
                '$toString': '$department'
            },
            'job_title': {
                '$toString': '$job_title'
            },
            'location': {
                '$toString': '$location'
            },
            'gender': {
                '$toString': '$gender'
            },
            'country_of_birth': {
                '$toString': '$country_of_birth'
            },
            'martial_status': {
                '$toString': '$martial_status'
            },
            'company_id': {
                '$toString': '$company_id'
            },
            'reporting_manager': {
                '$toString': '$reporting_manager'
            },
            'country_name': {
                '$ifNull': [
                    {
                        '$first': '$country_details.name'
                    }, None
                ]
            },
            'reporting_manager_name': {
                '$ifNull': [
                    {
                        '$first': '$reporting_manager_details.full_name'
                    }, None
                ]
            }
        }
    }, {
        '$project': {
            'lookup_data': 0,
            'all_ids': 0,
            'country_details': 0,
            'reporting_manager_details': 0,
            'legislation_details': 0
        }
    }
]


@router.post("/get_all_reporting_managers")
async def get_all_reporting_managers(
        employer_id: str = Body(None),
        current_employee_id: str = Body(None),
        data: dict = Depends(security.get_current_user)
):
    try:
        company_id = ObjectId(data.get('company_id'))

        pipeline: Any = [
            {"$match": {"company_id": company_id, "person_type": "Employee"}}
        ]

        # Add employer filter if employer_id exists
        if employer_id:
            pipeline.append({"$match": {"employer": ObjectId(employer_id)}})

        # Exclude current employee if current_employee_id exists
        if current_employee_id:
            pipeline.append({"$match": {"_id": {"$ne": ObjectId(current_employee_id)}}})

        # Only return _id and full_name
        pipeline.append({"$project": {"_id": 1, "full_name": 1}})

        employees_cursor = await employees_collection.aggregate(pipeline)
        employees = []
        for emp in await employees_cursor.to_list(None):
            emp["_id"] = str(emp["_id"])
            employees.append(emp)

        return {"status": "success", "result": employees}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


async def get_employee_details(employee_id: ObjectId):
    new_pipeline = copy.deepcopy(details_pipeline)
    new_pipeline.insert(0, {
        "$match": {
            "_id": employee_id
        }
    })
    cursor = await employees_collection.aggregate(new_pipeline)
    result = await cursor.to_list(1)
    return result[0] if result else None


@router.get("/get_employee_details_dor_editing/{employee_id}")
async def get_employee_details_dor_editing(employee_id: str, _: dict = Depends(security.get_current_user)):
    try:
        result = await get_employee_details(ObjectId(employee_id))
        return {"details": result}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/get_all_employees")
async def get_all_employees(data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        all_employees_pipeline = copy.deepcopy(main_screen_pipeline)
        all_employees_pipeline.insert(0, {
            "$match": {
                "company_id": company_id
            }
        })
        cursor = await employees_collection.aggregate(all_employees_pipeline)
        results = await cursor.to_list(None)
        return {"employees": results}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/create_employee")
async def create_employee(full_name: str = Form(None), country_of_birth: str = Form(None),
                          place_of_birth: str = Form(None), date_of_birth: datetime = Form(None),
                          gender: str = Form(None), martial_status: str = Form(None), person_type: str = Form(None),
                          status: str = Form(None), employer: str = Form(None), department: str = Form(None),
                          job_title: str = Form(None), location: str = Form(None), hire_date: datetime = Form(None),
                          end_date: datetime = Form(None), reporting_manager: str = Form(None),
                          legislation: str = Form(None),
                          person_image: UploadFile = File(None), data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        person_image_url = ""
        person_image_public_id = ""
        if person_image:
            result = await upload_images.upload_image(person_image, 'People')
            person_image_url = result["url"]
            person_image_public_id = result["public_id"]
        new_people_counter = await create_custom_counter("PEN", "PE", description='People Counter', data=data)
        employee_dict = {
            "company_id": company_id,
            "full_name": full_name,
            "country_of_birth": ObjectId(country_of_birth) if country_of_birth else None,
            "legislation": ObjectId(legislation) if legislation else None,
            "place_of_birth": place_of_birth,
            "date_of_birth": date_of_birth,
            "gender": ObjectId(gender) if gender else None,
            "martial_status": ObjectId(martial_status) if martial_status else None,
            "person_type": person_type,
            "status": status,
            "employer": ObjectId(employer) if employer else None,
            "department": ObjectId(department) if department else None,
            "job_title": ObjectId(job_title) if job_title else None,
            "location": ObjectId(location) if location else None,
            "hire_date": hire_date,
            "end_date": end_date,
            "reporting_manager": ObjectId(reporting_manager) if reporting_manager else None,
            "createdAt": security.now_utc(),
            "updatedAt": security.now_utc(),
            "people_counter": new_people_counter['final_counter'] if new_people_counter[
                'success'] else None,
            "person_image_url": person_image_url,
            "person_image_public_id": person_image_public_id,
        }

        result = await employees_collection.insert_one(employee_dict)
        new_employee = await get_employee_details(result.inserted_id)
        serialized = serializer(new_employee)
        await manager.send_to_company(str(company_id), {
            "type": "employee_added",
            "data": serialized
        })
        return {"employee_id": str(result.inserted_id)}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.patch("/update_employee/{employee_id}")
async def update_employee(employee_id: str, full_name: str = Form(None), country_of_birth: str = Form(None),
                          place_of_birth: str = Form(None), date_of_birth: datetime = Form(None),
                          gender: str = Form(None), martial_status: str = Form(None), person_type: str = Form(None),
                          status: str = Form(None), employer: str = Form(None), department: str = Form(None),
                          job_title: str = Form(None), location: str = Form(None), hire_date: datetime = Form(None),
                          end_date: datetime = Form(None), reporting_manager: str = Form(None),
                          legislation: str = Form(None),
                          person_image: UploadFile = File(None), data: dict = Depends(security.get_current_user)):
    try:
        company_id = data.get("company_id")
        employee_dict = {
            "full_name": full_name,
            "country_of_birth": ObjectId(country_of_birth) if country_of_birth else None,
            "place_of_birth": place_of_birth,
            "date_of_birth": date_of_birth,
            "legislation": ObjectId(legislation) if legislation else None,
            "gender": ObjectId(gender) if gender else None,
            "martial_status": ObjectId(martial_status) if martial_status else None,
            "person_type": person_type,
            "status": status,
            "employer": ObjectId(employer) if employer else None,
            "department": ObjectId(department) if department else None,
            "job_title": ObjectId(job_title) if job_title else None,
            "location": ObjectId(location) if location else None,
            "hire_date": hire_date,
            "end_date": end_date,
            "reporting_manager": ObjectId(reporting_manager) if reporting_manager else None,
            "updatedAt": security.now_utc(),
        }
        if person_image:
            current_employee = await employees_collection.find_one({"_id": ObjectId(employee_id)})
            if current_employee:
                person_image_public_id = current_employee.get("person_image_public_id")
                if person_image_public_id:
                    await upload_images.delete_image_from_server(person_image_public_id)
                result = await upload_images.upload_image(person_image, 'People')
                employee_dict["person_image_url"] = result["url"]
                employee_dict["person_image_public_id"] = result["public_id"]

        result = await employees_collection.update_one({"_id": ObjectId(employee_id)}, {"$set": employee_dict})
        if result.modified_count > 0:
            updated_employee = await get_employee_details(ObjectId(employee_id))
            serialized = serializer(updated_employee)
            await manager.send_to_company(company_id, {
                "type": "employee_updated",
                "data": serialized
            })

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/delete_employee/{employee_id}")
async def delete_employee(employee_id: str, data: dict = Depends(security.get_current_user)):
    try:
        company_id = data.get("company_id")
        result = await employees_collection.find_one({"_id": ObjectId(employee_id)})
        person_image_public_id = result.get("person_image_public_id")
        if person_image_public_id:
            await upload_images.delete_image_from_server(person_image_public_id)
        employee_contacts_and_relatives = await employees_contacts_and_relatives_collection.find(
            {"employee_id": ObjectId(employee_id)}).to_list(None)
        for contact in employee_contacts_and_relatives:
            contact_id = contact.get("_id")

        # await employees_address_collection.delete_many({"employee_id": ObjectId(employee_id)})
        # await employees_email_collection.delete_many({"employee_id": ObjectId(employee_id)})
        # await employees_nationality_collection.delete_many({"employee_id": ObjectId(employee_id)})
        # await employees_phone_collection.delete_many({"employee_id": ObjectId(employee_id)})
        # await employees_contacts_and_relatives_collection.delete_many({"employee_id": ObjectId(employee_id)})
        if result.deleted_count == 1:
            await manager.send_to_company(company_id, {
                "type": "employee_deleted",
                "data": {"_id": employee_id}
            })
            return {"message": "Branch removed successfully!"}
        else:
            raise HTTPException(status_code=404, detail="Branch not found")

    except Exception as error:
        return {"message": str(error)}


@router.get("/get_employees_by_department")
async def get_employees_by_department(department: str, data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        get_employees_by_department_pipeline = [
            {
                '$match': {
                    'company_id': company_id,
                    'department': {
                        '$in': [
                            department
                        ]
                    }
                }
            }, {
                '$lookup': {
                    'from': 'all_lists_values',
                    'localField': 'status',
                    'foreignField': '_id',
                    'as': 'status_details'
                }
            }, {
                '$match': {
                    '$expr': {
                        '$ne': [
                            {
                                '$arrayElemAt': [
                                    '$status_details.name', 0
                                ]
                            }, 'Inactive'
                        ]
                    }
                }
            }, {
                '$project': {
                    '_id': 1,
                    'name': 1,
                    'job_title': 1
                }
            }
        ]
        cursor = await employees_collection.aggregate(get_employees_by_department_pipeline)
        results = await cursor.to_list(None)
        return {"employees": [serializer(e) for e in results]}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# =========================== PAYROLL SECTION ============================
employee_payroll_pipeline = [
    {
        '$lookup': {
            'from': 'payroll_elements',
            'localField': 'name',
            'foreignField': '_id',
            'as': 'name_details'
        }
    }, {
        '$addFields': {
            '_id': {
                '$toString': '$_id'
            },
            'name': {
                '$toString': '$name'
            },
            'company_id': {
                '$toString': '$company_id'
            },
            'employee_id': {
                '$toString': '$employee_id'
            },
            'name_value': {
                '$ifNull': [
                    {
                        '$first': '$name_details.name'
                    }, None
                ]
            }
        }
    }, {
        '$project': {
            'name_details': 0
        }
    }
]


async def get_payroll_details(payroll_id: ObjectId):
    try:
        new_pipeline: Any = copy.deepcopy(employee_payroll_pipeline)
        new_pipeline.insert(0, {
            "$match": {
                "_id": payroll_id
            }
        })
        cursor = await employees_payrolls_collection.aggregate(new_pipeline)
        result = await cursor.to_list(1)
        return result[0] if result else None

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


class EmployeePayrollModel(BaseModel):
    name: Optional[str] = None
    value: Optional[int] = None
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    notes: Optional[str] = None


@router.post("/add_new_employee_payroll/{employee_id}")
async def add_new_employee_payroll(employee_id: str, payroll: EmployeePayrollModel,
                                   data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        payroll = payroll.model_dump(exclude_unset=True)
        payroll['company_id'] = company_id
        payroll['name'] = ObjectId(payroll['name']) if payroll['name'] else None
        payroll['employee_id'] = ObjectId(employee_id) if employee_id else None
        payroll['value'] = payroll['value']
        payroll['start_date'] = payroll['start_date']
        payroll['end_date'] = payroll['end_date']
        payroll['notes'] = payroll['notes']
        payroll['createdAt'] = security.now_utc()
        payroll['updatedAt'] = security.now_utc()

        new_payroll = await employees_payrolls_collection.insert_one(payroll)

        if not new_payroll.inserted_id:
            raise HTTPException(status_code=500, detail="Failed to create new payroll document")
        added_payroll = await get_payroll_details(new_payroll.inserted_id)
        return {"new_payroll": added_payroll}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.patch("/update_employee_payroll/{payroll_id}")
async def update_employee_payroll(payroll_id: str, payroll: EmployeePayrollModel,
                                  _: dict = Depends(security.get_current_user)):
    try:
        payroll = payroll.model_dump(exclude_unset=True)
        payroll['name'] = ObjectId(payroll['name']) if payroll['name'] else None
        payroll['value'] = payroll['value']
        payroll['start_date'] = payroll['start_date']
        payroll['end_date'] = payroll['end_date']
        payroll['notes'] = payroll['notes']
        payroll['updatedAt'] = security.now_utc()

        updated_payroll = await employees_payrolls_collection.update_one({"_id": ObjectId(payroll_id)},
                                                                         {"$set": payroll})

        if updated_payroll.matched_count == 0:
            raise HTTPException(status_code=500, detail="Failed to update payroll document")
        updated_payroll = await get_payroll_details(ObjectId(payroll_id))
        return {"updated_payroll": updated_payroll}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/delete_employee_payroll/{payroll_id}")
async def delete_employee_payroll(payroll_id: str, _: dict = Depends(security.get_current_user)):
    try:
        if not payroll_id:
            raise HTTPException(status_code=404, detail="payroll id not found")
        await employees_payrolls_collection.delete_one({"_id": ObjectId(payroll_id)})
        return {"deleted_payroll_id": payroll_id}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ==================== CONTACTS AND RELATIVES SECTION ====================
contacts_pipeline = [
    {
        '$addFields': {
            'all_ids': [
                '$relationship', '$gender', '$nationality'
            ]
        }
    }, {
        '$lookup': {
            'from': 'all_lists_values',
            'let': {
                'ids': '$all_ids'
            },
            'pipeline': [
                {
                    '$match': {
                        '$expr': {
                            '$in': [
                                '$_id', '$$ids'
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
            'as': 'lookup_data'
        }
    }, {
        '$addFields': {
            '_id': {
                '$toString': '$_id'
            },
            'relationship': {
                '$toString': '$relationship'
            },
            'gender': {
                '$toString': '$gender'
            },
            'nationality': {
                '$toString': '$nationality'
            },
            'company_id': {
                '$toString': '$company_id'
            },
            'employee_id': {
                '$toString': '$employee_id'
            },
            'relationship_name': {
                '$let': {
                    'vars': {
                        'match': {
                            '$first': {
                                '$filter': {
                                    'input': '$lookup_data',
                                    'cond': {
                                        '$eq': [
                                            '$$this._id', '$relationship'
                                        ]
                                    }
                                }
                            }
                        }
                    },
                    'in': '$$match.name'
                }
            },
            'gender_name': {
                '$let': {
                    'vars': {
                        'match': {
                            '$first': {
                                '$filter': {
                                    'input': '$lookup_data',
                                    'cond': {
                                        '$eq': [
                                            '$$this._id', '$gender'
                                        ]
                                    }
                                }
                            }
                        }
                    },
                    'in': '$$match.name'
                }
            },
            'nationality_name': {
                '$let': {
                    'vars': {
                        'match': {
                            '$first': {
                                '$filter': {
                                    'input': '$lookup_data',
                                    'cond': {
                                        '$eq': [
                                            '$$this._id', '$nationality'
                                        ]
                                    }
                                }
                            }
                        }
                    },
                    'in': '$$match.name'
                }
            }
        }
    }, {
        '$project': {
            'all_ids': 0,
            'lookup_data': 0
        }
    }
]


async def get_contacts_details(contact_id: ObjectId):
    try:
        new_pipeline: Any = copy.deepcopy(contacts_pipeline)
        new_pipeline.insert(0, {
            "$match": {
                "_id": contact_id
            }
        })
        cursor = await employees_contacts_and_relatives_collection.aggregate(new_pipeline)
        result = await cursor.to_list(1)
        return result[0] if result else None

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


class EmployeeContactsAndRelatives(BaseModel):
    full_name: Optional[str] = None
    relationship: Optional[str] = None
    phone_number: Optional[str] = None
    gender: Optional[str] = None
    date_of_birth: Optional[datetime] = None
    nationality: Optional[str] = None
    email_address: Optional[str] = None
    note: Optional[str] = None
    is_emergency: Optional[bool] = None


class EmployeePayrollModel(BaseModel):
    full_name: Optional[str] = None
    relationship: Optional[str] = None
    phone_number: Optional[str] = None
    gender: Optional[str] = None
    date_of_birth: Optional[datetime] = None
    nationality: Optional[str] = None
    email_address: Optional[str] = None
    note: Optional[str] = None
    is_emergency: Optional[bool] = None


@router.get("/get_employee_contact_and_relative/{employee_id}")
async def get_employee_contact_and_relative(employee_id: str, _: dict = Depends(security.get_current_user)):
    try:
        employee_id = ObjectId(employee_id)
        new_pipeline: Any = copy.deepcopy(contacts_pipeline)
        new_pipeline.insert(0, {
            "$match": {
                "employee_id": employee_id
            }
        })

        cursor = await employees_contacts_and_relatives_collection.aggregate(new_pipeline)
        result = await cursor.to_list(None)
        return {"contact": result if result else None}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/add_new_employee_contact_and_relative/{employee_id}")
async def add_new_employee_contact_and_relative(employee_id: str, contact: EmployeeContactsAndRelatives,
                                                data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        contact = contact.model_dump(exclude_unset=True)
        contact['company_id'] = company_id
        contact['relationship'] = ObjectId(contact['relationship']) if contact['relationship'] else None
        contact['gender'] = ObjectId(contact['gender']) if contact['gender'] else None
        contact['nationality'] = ObjectId(contact['nationality']) if contact['nationality'] else None
        contact['employee_id'] = ObjectId(employee_id) if employee_id else None
        contact['createdAt'] = security.now_utc()
        contact['updatedAt'] = security.now_utc()

        new_contact = await employees_contacts_and_relatives_collection.insert_one(contact)

        if not new_contact.inserted_id:
            raise HTTPException(status_code=500, detail="Failed to create new contact")
        added_contact = await get_contacts_details(new_contact.inserted_id)
        return {"new_contact": added_contact}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.patch("/update_employee_contact_and_relative/{contact_id}")
async def update_employee_contact_and_relative(contact_id: str, contact: EmployeeContactsAndRelatives,
                                               _: dict = Depends(security.get_current_user)):
    try:
        contact = contact.model_dump(exclude_unset=True)
        contact['relationship'] = ObjectId(contact['relationship']) if contact['relationship'] else None
        contact['gender'] = ObjectId(contact['gender']) if contact['gender'] else None
        contact['nationality'] = ObjectId(contact['nationality']) if contact['nationality'] else None
        contact['updatedAt'] = security.now_utc()

        new_contact = await employees_contacts_and_relatives_collection.update_one({"_id": ObjectId(contact_id)},
                                                                                   {"$set": contact})

        if new_contact.matched_count == 0:
            raise HTTPException(status_code=500, detail="Failed to update contact information")
        updated_contact = await get_contacts_details(ObjectId(contact_id))
        return {"updated_contact": updated_contact}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/delete_employee_contact_and_relative/{contact_id}")
async def delete_employee_contact_and_relative(contact_id: str, _: dict = Depends(security.get_current_user)):
    try:
        if not contact_id:
            raise HTTPException(status_code=404, detail="contact id not found")
        await employees_contacts_and_relatives_collection.delete_one({"_id": ObjectId(contact_id)})
        return {"deleted_contact_id": contact_id}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ==================== ADDRESS SECTION ====================
address_details_pipeline = [
    {
        '$lookup': {
            'from': 'all_countries',
            'localField': 'country',
            'foreignField': '_id',
            'as': 'country_details'
        }
    }, {
        '$lookup': {
            'from': 'all_countries_cities',
            'localField': 'city',
            'foreignField': '_id',
            'as': 'city_details'
        }
    }, {
        '$addFields': {
            'country_name': {
                '$ifNull': [
                    {
                        '$first': '$country_details.name'
                    }, None
                ]
            },
            'city_name': {
                '$ifNull': [
                    {
                        '$first': '$city_details.name'
                    }, None
                ]
            },
            'country': {
                '$toString': '$country'
            },
            'city': {
                '$toString': '$city'
            },
            '_id': {
                '$toString': '$_id'
            }
        }
    }, {
        '$project': {
            'line': 1,
            'country': 1,
            'city': 1,
            'country_name': 1,
            'city_name': 1
        }
    }
]


async def get_employee_address_details(address_id: ObjectId):
    try:
        new_pipeline: Any = copy.deepcopy(address_details_pipeline)
        new_pipeline.insert(0, {
            "$match": {
                "_id": address_id
            }
        })
        cursor = await employees_address_collection.aggregate(new_pipeline)
        result = await cursor.to_list(1)
        return result[0] if result else None

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/add_employee_address/{employee_id}")
async def add_employee_address(employee_id: str, address: EmployeeAddressModel,
                               data: dict = Depends(security.get_current_user)):
    try:
        if not employee_id:
            raise HTTPException(status_code=404, detail="Employee ID not found")
        company_id = ObjectId(data.get("company_id"))
        address = address.model_dump(exclude_unset=True)
        if 'country' in address and address.get('country'):
            address['country'] = ObjectId(address['country']) if address['country'] else None
        if 'city' in address and address.get('city'):
            address['city'] = ObjectId(address['city']) if address['city'] else None
        address['company_id'] = company_id
        address['employee_id'] = ObjectId(employee_id)
        address['createdAt'] = security.now_utc()
        address['updatedAt'] = security.now_utc()
        added_address = await employees_address_collection.insert_one(address)
        if not added_address.inserted_id:
            raise HTTPException(status_code=500, detail="Failed to create address")

        new_address_details = await get_employee_address_details(added_address.inserted_id)

        return {"new_address": new_address_details}


    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.patch("/update_employee_address/{address_id}")
async def update_employee_address(address_id: str, address: EmployeeAddressModel,
                                  _: dict = Depends(security.get_current_user)):
    try:
        if not address_id:
            raise HTTPException(status_code=404, detail="Address ID not found")
        address = address.model_dump(exclude_unset=True)
        if 'country' in address and address.get('country'):
            address['country'] = ObjectId(address['country']) if address['country'] else None
        if 'city' in address and address.get('city'):
            address['city'] = ObjectId(address['city']) if address['city'] else None
        address['updatedAt'] = security.now_utc()
        await employees_address_collection.update_one({"_id": ObjectId(address_id)}, {"$set": address})
        update_address_details = await get_employee_address_details(ObjectId(address_id))
        return {"update_address": update_address_details}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/delete_employee_address/{address_id}")
async def delete_employee_address(address_id: str, _: dict = Depends(security.get_current_user)):
    try:
        if not address_id:
            raise HTTPException(status_code=404, detail="Address ID not found")
        await employees_address_collection.delete_one({"_id": ObjectId(address_id)})
        return {"deleted_address_id": address_id}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ==================== NATIONALITY SECTION ====================

employee_nationality_pipeline = [
    {
        '$lookup': {
            'from': 'all_lists_values',
            'localField': 'nationality',
            'foreignField': '_id',
            'as': 'nationality_details'
        }
    }, {
        '$addFields': {
            'nationality_name': {
                '$ifNull': [
                    {
                        '$first': '$nationality_details.name'
                    }, None
                ]
            },
            '_id': {
                '$toString': '$_id'
            },
            'nationality': {
                '$toString': '$nationality'
            },
            'company_id': {
                '$toString': '$company_id'
            },
            'employee_id': {
                '$toString': '$employee_id'
            }
        }
    }, {
        '$project': {
            'nationality_details': 0
        }
    }
]


async def get_employee_nationality_details(nationality_id: ObjectId):
    try:
        new_pipeline: Any = copy.deepcopy(employee_nationality_pipeline)
        new_pipeline.insert(0, {
            "$match": {
                "_id": nationality_id
            }
        })
        cursor = await employees_nationality_collection.aggregate(new_pipeline)
        result = await cursor.to_list(1)
        return result[0] if result else None

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/add_employee_nationality/{employee_id}")
async def add_employee_nationality(employee_id: str, nationality: EmployeeNationalityModel,
                                   data: dict = Depends(security.get_current_user)):
    try:
        if not employee_id:
            raise HTTPException(status_code=404, detail="Employee ID not found")
        company_id = ObjectId(data.get("company_id"))
        nationality = nationality.model_dump(exclude_unset=True)
        if 'nationality' in nationality and nationality.get('nationality'):
            nationality['nationality'] = ObjectId(nationality['nationality']) if nationality['nationality'] else None

        nationality['company_id'] = company_id
        nationality['employee_id'] = ObjectId(employee_id)
        nationality['createdAt'] = security.now_utc()
        nationality['updatedAt'] = security.now_utc()
        added_nationality = await employees_nationality_collection.insert_one(nationality)
        if not added_nationality.inserted_id:
            raise HTTPException(status_code=500, detail="Failed to create nationality")

        new_nationality_details = await get_employee_nationality_details(added_nationality.inserted_id)

        return {"new_nationality": new_nationality_details}


    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.patch("/edit_employee_nationality/{nationality_id}")
async def edit_employee_nationality(nationality_id: str, nationality: EmployeeNationalityModel,
                                    _: dict = Depends(security.get_current_user)):
    try:
        if not nationality_id:
            raise HTTPException(status_code=404, detail="nationality_id not found")
        nationality = nationality.model_dump(exclude_unset=True)
        if 'nationality' in nationality and nationality.get('nationality'):
            nationality['nationality'] = ObjectId(nationality['nationality']) if nationality['nationality'] else None

        nationality['updatedAt'] = security.now_utc()
        updated_nationality = await employees_nationality_collection.update_one({"_id": ObjectId(nationality_id)},
                                                                                {"$set": nationality})
        if updated_nationality.modified_count == 0:
            raise HTTPException(status_code=500, detail="Failed to update nationality")

        new_nationality_details = await get_employee_nationality_details(updated_nationality.inserted_id)

        return {"updated_nationality": new_nationality_details}


    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/delete_employee_nationality/{nationality_id}")
async def delete_employee_nationality(nationality_id: str, _: dict = Depends(security.get_current_user)):
    try:
        if not nationality_id:
            raise HTTPException(status_code=404, detail="nationality_id not found")
        await employees_nationality_collection.delete_one({"_id": ObjectId(nationality_id)})
        return {"deleted_nationality_id": nationality_id}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ==================== PHONE SECTION ====================
employee_phones_pipeline = [
    {
        '$lookup': {
            'from': 'all_lists_values',
            'localField': 'type',
            'foreignField': '_id',
            'as': 'type_details'
        }
    }, {
        '$addFields': {
            '_id': {
                '$toString': '$_id'
            },
            'type': {
                '$toString': '$type'
            },
            'company_id': {
                '$toString': '$company_id'
            },
            'employee_id': {
                '$toString': '$employee_id'
            },
            'type_name': {
                '$ifNull': [
                    {
                        '$first': [
                            '$type_details.name'
                        ]
                    }, None
                ]
            }
        }
    }, {
        '$project': {
            'type_details': 0
        }
    }
]


async def get_employee_phone_details(phone_id: ObjectId):
    try:
        new_pipeline: Any = copy.deepcopy(employee_phones_pipeline)
        new_pipeline.insert(0, {
            "$match": {
                "_id": phone_id
            }
        })
        cursor = await employees_phone_collection.aggregate(new_pipeline)
        result = await cursor.to_list(1)
        return result[0] if result else None

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/add_employee_phone/{employee_id}")
async def add_employee_phone(employee_id: str, phone: EmployeePhoneModel,
                             data: dict = Depends(security.get_current_user)):
    try:
        if not employee_id:
            raise HTTPException(status_code=404, detail="Employee ID not found")
        company_id = ObjectId(data.get("company_id"))
        phone = phone.model_dump(exclude_unset=True)
        if 'type' in phone and phone.get('type'):
            phone['type'] = ObjectId(phone['type']) if phone['type'] else None

        phone['company_id'] = company_id
        phone['employee_id'] = ObjectId(employee_id)
        phone['createdAt'] = security.now_utc()
        phone['updatedAt'] = security.now_utc()
        added_phone = await employees_phone_collection.insert_one(phone)
        if not added_phone.inserted_id:
            raise HTTPException(status_code=500, detail="Failed to create phone")

        new_phone_details = await get_employee_phone_details(added_phone.inserted_id)

        return {"new_phone": new_phone_details}


    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.patch("/edit_employee_phone/{phone_id}")
async def edit_employee_phone(phone_id: str, phone: EmployeePhoneModel,
                              _: dict = Depends(security.get_current_user)):
    try:
        if not phone_id:
            raise HTTPException(status_code=404, detail="phone_id not found")
        phone = phone.model_dump(exclude_unset=True)
        if 'type' in phone and phone.get('type'):
            phone['type'] = ObjectId(phone['type']) if phone['type'] else None

        phone['updatedAt'] = security.now_utc()
        added_phone = await employees_phone_collection.update_one({"_id": ObjectId(phone_id)}, {"$set": phone})
        if added_phone.modified_count == 0:
            raise HTTPException(status_code=500, detail="Failed to update phone")

        updated_phone_details = await get_employee_phone_details(added_phone.inserted_id)

        return {"updated_phone": updated_phone_details}


    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/delete_employee_phone/{phone_id}")
async def delete_employee_phone(phone_id: str, _: dict = Depends(security.get_current_user)):
    try:
        if not phone_id:
            raise HTTPException(status_code=404, detail="phone_id not found")
        await employees_phone_collection.delete_one({"_id": ObjectId(phone_id)})
        return {"deleted_phone_id": phone_id}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ==================== EMAIL SECTION ====================
employee_email_pipeline = [
    {
        '$lookup': {
            'from': 'all_lists_values',
            'localField': 'type',
            'foreignField': '_id',
            'as': 'type_details'
        }
    }, {
        '$addFields': {
            '_id': {
                '$toString': '$_id'
            },
            'type': {
                '$toString': '$type'
            },
            'company_id': {
                '$toString': '$company_id'
            },
            'employee_id': {
                '$toString': '$employee_id'
            },
            'type_name': {
                '$ifNull': [
                    {
                        '$first': [
                            '$type_details.name'
                        ]
                    }, None
                ]
            }
        }
    }, {
        '$project': {
            'type_details': 0
        }
    }
]


async def get_employee_email_details(email_id: ObjectId):
    try:
        new_pipeline: Any = copy.deepcopy(employee_email_pipeline)
        new_pipeline.insert(0, {
            "$match": {
                "_id": email_id
            }
        })
        cursor = await employees_email_collection.aggregate(new_pipeline)
        result = await cursor.to_list(1)
        return result[0] if result else None

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/add_employee_email/{employee_id}")
async def add_employee_email(employee_id: str, email: EmployeeEmailModel,
                             data: dict = Depends(security.get_current_user)):
    try:
        if not employee_id:
            raise HTTPException(status_code=404, detail="Employee ID not found")
        company_id = ObjectId(data.get("company_id"))
        email = email.model_dump(exclude_unset=True)
        if 'type' in email and email.get('type'):
            email['type'] = ObjectId(email['type']) if email['type'] else None

        email['company_id'] = company_id
        email['employee_id'] = ObjectId(employee_id)
        email['createdAt'] = security.now_utc()
        email['updatedAt'] = security.now_utc()
        added_email = await employees_email_collection.insert_one(email)
        if not added_email.inserted_id:
            raise HTTPException(status_code=500, detail="Failed to create email")

        new_email_details = await get_employee_email_details(added_email.inserted_id)
        return {"new_email": new_email_details}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.patch("/edit_employee_email/{email_id}")
async def edit_employee_email(email_id: str, email: EmployeeEmailModel,
                              _: dict = Depends(security.get_current_user)):
    try:
        if not email_id:
            raise HTTPException(status_code=404, detail="email_id not found")
        email = email.model_dump(exclude_unset=True)
        if 'type' in email and email.get('type'):
            email['type'] = ObjectId(email['type']) if email['type'] else None

        email['updatedAt'] = security.now_utc()
        added_email = await employees_email_collection.update_one({"_id": ObjectId(email_id)}, {"$set": email})
        if not added_email.matched_count == 0:
            raise HTTPException(status_code=500, detail="Failed to update email")

        updated_email_details = await get_employee_email_details(ObjectId(email_id))
        return {"updated_email": updated_email_details}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/delete_employee_email/{email_id}")
async def delete_employee_email(email_id: str, _: dict = Depends(security.get_current_user)):
    try:
        if not email_id:
            raise HTTPException(status_code=404, detail="email_id not found")
        await employees_email_collection.delete_one({"_id": ObjectId(email_id)})
        return {"deleted_email_id": email_id}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/search_engine_for_employees")
async def search_engine_for_employees(
        filter_employees: EmployeesSearch,
        data: dict = Depends(security.get_current_user)
):
    try:
        match_stage: Any = {}
        company_id = ObjectId(data.get("company_id"))
        if company_id:
            match_stage = {"company_id": company_id}
        if filter_employees.from_date or filter_employees.to_date:
            match_stage['hire_date'] = {}
            if filter_employees.from_date:
                match_stage['hire_date']["$gte"] = filter_employees.from_date
            if filter_employees.to_date:
                match_stage['hire_date']["$lte"] = filter_employees.to_date

        if filter_employees.name:
            match_stage["full_name"] = {"$regex": filter_employees.name, "$options": "i"}
        if filter_employees.employer:
            match_stage["employer"] = filter_employees.employer
        if filter_employees.department:
            match_stage["department"] = filter_employees.department
        if filter_employees.job_title:
            match_stage["job_title"] = filter_employees.job_title
        if filter_employees.location:
            match_stage["location"] = filter_employees.location
        if filter_employees.status:
            match_stage["status"] = filter_employees.status
        if filter_employees.type:
            match_stage["person_type"] = filter_employees.type.capitalize()

        new_search_pipeline = copy.deepcopy(main_screen_pipeline)
        new_search_pipeline.insert(0, {"$match": match_stage})
        cursor = await employees_collection.aggregate(new_search_pipeline)
        employees = await cursor.to_list(None)
        return {"employees": employees}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ==================== BANK ACCOUNTS SECTION ====================

employee_bank_accounts_pipeline = [
    {
        '$lookup': {
            'from': 'all_lists_values',
            'localField': 'bank_name',
            'foreignField': '_id',
            'as': 'bank_name_details'
        }
    }, {
        '$addFields': {
            '_id': {
                '$toString': '$_id'
            },
            'bank_name': {
                '$toString': '$bank_name'
            },
            'company_id': {
                '$toString': '$company_id'
            },
            'employee_id': {
                '$toString': '$employee_id'
            },
            'bank_name_value': {
                '$ifNull': [
                    {
                        '$first': '$bank_name_details.name'
                    }, None
                ]
            }
        }
    }, {
        '$project': {
            'bank_name_details': 0
        }
    }
]


class EmployeeBankAccount(BaseModel):
    bank_name: Optional[str] = None
    account_number: Optional[str] = None
    iban: Optional[str] = None
    swift_code: Optional[str] = None


async def get_employee_bank_account_details(account_id: ObjectId):
    try:
        new_pipeline: Any = copy.deepcopy(employee_bank_accounts_pipeline)
        new_pipeline.insert(0, {
            "$match": {
                "_id": account_id
            }
        })
        cursor = await employees_bank_accounts_collection.aggregate(new_pipeline)
        result = await cursor.to_list(1)
        return result[0] if result else None


    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/add_employee_bank_account/{employee_id}")
async def add_employee_bank_account(employee_id: str, bank: EmployeeBankAccount,
                                    data: dict = Depends(security.get_current_user)):
    try:
        if not employee_id:
            raise HTTPException(status_code=404, detail="Employee ID not found")
        company_id = ObjectId(data.get("company_id"))
        bank = bank.model_dump(exclude_unset=True)
        if 'bank_name' in bank and bank.get('bank_name'):
            bank['bank_name'] = ObjectId(bank['bank_name']) if bank['bank_name'] else None

        bank['company_id'] = company_id
        bank['employee_id'] = ObjectId(employee_id)
        bank['createdAt'] = security.now_utc()
        bank['updatedAt'] = security.now_utc()
        added_bank_account = await employees_bank_accounts_collection.insert_one(bank)
        if not added_bank_account.inserted_id:
            raise HTTPException(status_code=500, detail="Failed to create bank account")

        new_bank_account_details = await get_employee_bank_account_details(added_bank_account.inserted_id)
        return {"new_bank_account": new_bank_account_details}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.patch("/edit_employee_bank_account/{account_id}")
async def edit_employee_bank_account(account_id: str, bank: EmployeeBankAccount,
                                     _: dict = Depends(security.get_current_user)):
    try:
        if not account_id:
            raise HTTPException(status_code=404, detail="Account ID not found")
        bank = bank.model_dump(exclude_unset=True)
        if 'bank_name' in bank and bank.get('bank_name'):
            bank['bank_name'] = ObjectId(bank['bank_name']) if bank['bank_name'] else None

        bank['updatedAt'] = security.now_utc()
        updated_bank_account = await employees_bank_accounts_collection.update_one({"_id": ObjectId(account_id)},
                                                                                   {"$set": bank})
        if updated_bank_account.matched_count == 0:
            raise HTTPException(status_code=500, detail="Failed to update bank account")

        updated_bank_account_details = await get_employee_bank_account_details(ObjectId(account_id))
        return {"updated_bank_account": updated_bank_account_details}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/delete_employee_bank_account/{account_id}")
async def delete_employee_bank_account(account_id: str, _: dict = Depends(security.get_current_user)):
    try:
        if not account_id:
            raise HTTPException(status_code=404, detail="account_id not found")
        await employees_bank_accounts_collection.delete_one({"_id": ObjectId(account_id)})
        return {"deleted_account_id": account_id}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ==================== LEAVES  SECTION ====================
class NumberOfDaysForWorkingDaysModel(BaseModel):
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None


class EmployeeLeavesModel(BaseModel):
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    status: Optional[str] = None
    leave_type: Optional[str] = None
    number_of_days: Optional[int] = None
    note: Optional[str] = None


employee_leave_details_pipeline = [
    {
        '$lookup': {
            'from': 'leave_types',
            'localField': 'leave_type',
            'foreignField': '_id',
            'as': 'leave_type_details'
        }
    }, {
        '$addFields': {
            '_id': {
                '$toString': '$_id'
            },
            'leave_type': {
                '$toString': '$leave_type'
            },
            'company_id': {
                '$toString': '$company_id'
            },
            'employee_id': {
                '$toString': '$employee_id'
            },
            'leave_type_name': {
                '$ifNull': [
                    {
                        '$first': '$leave_type_details.name'
                    }, None
                ]
            }
        }
    }, {
        '$project': {
            'leave_type_details': 0
        }
    }
]


async def get_employee_leave_details(leave_id: ObjectId):
    try:
        new_pipeline: Any = copy.deepcopy(employee_leave_details_pipeline)
        new_pipeline.insert(0, {
            "$match": {
                "_id": leave_id
            }
        })

        cursor = await employees_leaves_collection.aggregate(new_pipeline)
        result = await cursor.to_list(None)
        return result[0] if result else None

    except Exception:
        raise


@router.post("/get_number_of_days_for_working_days/{employee_id}")
async def get_number_of_days_for_working_days(
        employee_id: str,
        data: NumberOfDaysForWorkingDaysModel,
        _: dict = Depends(security.get_current_user)
):
    try:
        employee_id = ObjectId(employee_id)

        employee_doc = await employees_collection.find_one({"_id": employee_id})
        if not employee_doc:
            raise HTTPException(status_code=404, detail="Employee not found")
        # Default empty weekend
        legislations_weekend = []
        if "legislation" in employee_doc:
            legislation = employee_doc["legislation"]
            legislation_doc = await legislations_collection.find_one({"_id": legislation})
            if not legislation_doc:
                raise HTTPException(status_code=404, detail="Legislation not found")

            legislations_weekend = legislation_doc.get("weekend", [])
        start_date = data.start_date
        end_date = data.end_date
        if start_date > end_date:
            raise HTTPException(status_code=400, detail="Invalid date range")
        # Map weekday index to name
        weekday_map = {
            0: "Monday",
            1: "Tuesday",
            2: "Wednesday",
            3: "Thursday",
            4: "Friday",
            5: "Saturday",
            6: "Sunday"
        }
        company_id = employee_doc.get("company_id")
        # Fetch holidays in range
        holidays = await public_holidays_collection.find(
            {
                "company_id": company_id,
                "date": {
                    "$gte": start_date,
                    "$lte": end_date
                }
            },
            {"date": 1}
        ).to_list(None)
        holiday_dates = set(h["date"].date() for h in holidays)
        # Calculate working days
        total_days = 0
        current_date = start_date

        while current_date <= end_date:
            day_name = weekday_map[current_date.weekday()]
            # Skip weekends
            if day_name not in legislations_weekend:
                # Skip holidays
                if current_date.date() not in holiday_dates:
                    total_days += 1
            current_date += timedelta(days=1)
        return {
            "working_days": total_days,
            "total_days_including_weekends": (end_date - start_date).days + 1,
            "holidays_count": len(holiday_dates),
            "weekends": legislations_weekend
        }

    except Exception:
        raise


@router.get("/get_all_employee_leaves/{employee_id}")
async def get_all_employee_leaves(data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        new_pipeline: Any = copy.deepcopy(employee_leave_details_pipeline)
        new_pipeline.insert(0, {
            "$match": {
                "company_id": company_id
            }
        })
        new_pipeline.append({"$sort": {"start_date": 1}})
        cursor = await employees_leaves_collection.aggregate(new_pipeline)
        results = await cursor.to_list(None)
        return {"all_leaves": results if results else []}

    except Exception:
        raise


@router.post("/add_new_employee_leave/{employee_id}")
async def add_new_employee_leave(employee_id: str, leave_data: EmployeeLeavesModel,
                                 data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        if not employee_id:
            raise HTTPException(status_code=404, detail="Employee ID not found")
        leave_data = leave_data.model_dump(exclude_unset=True)
        if 'leave_type' in leave_data and leave_data.get('leave_type'):
            leave_data['leave_type'] = ObjectId(leave_data['leave_type']) if leave_data['leave_type'] else None

        leave_data['company_id'] = company_id
        leave_data['status'] = "New"
        leave_data['employee_id'] = ObjectId(employee_id)
        leave_data['createdAt'] = security.now_utc()
        leave_data['updatedAt'] = security.now_utc()
        added_leave_details = await employees_leaves_collection.insert_one(leave_data)
        if not added_leave_details.inserted_id:
            raise HTTPException(status_code=500, detail="Failed to add employee leave")

        added_leave = await get_employee_leave_details(added_leave_details.inserted_id)
        print(added_leave)
        return {"new_leave": added_leave}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.patch("/update_employee_leave/{leave_id}")
async def update_employee_leave(leave_id: str, leave_data: EmployeeLeavesModel,
                                _: dict = Depends(security.get_current_user)):
    try:
        if not leave_id:
            raise HTTPException(status_code=404, detail="Leave ID not found")
        leave_data = leave_data.model_dump(exclude_unset=True)
        if 'leave_type' in leave_data and leave_data.get('leave_type'):
            leave_data['leave_type'] = ObjectId(leave_data['leave_type']) if leave_data['leave_type'] else None

        leave_data['updatedAt'] = security.now_utc()
        updated_leave_details = await employees_leaves_collection.update_one({"_id": ObjectId(leave_id)},
                                                                             {"$set": leave_data})
        if updated_leave_details.matched_count == 0:
            raise HTTPException(status_code=500, detail="Failed to update employee leave")

        updated_leave = await get_employee_leave_details(ObjectId(leave_id))
        print(updated_leave)
        return {"update_leave": updated_leave}

    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/delete_employee_leave/{leave_id}")
async def delete_employee_leave(leave_id: str, _: dict = Depends(security.get_current_user)):
    try:
        if not leave_id:
            raise HTTPException(status_code=404, detail="leave_id not found")
        await employees_leaves_collection.delete_one({"_id": ObjectId(leave_id)})
        return {"deleted_leave_id": leave_id}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/get_employee_leave_status/{leave_id}")
async def get_employee_leave_status(leave_id: str, _: dict = Depends(security.get_current_user)):
    try:
        leave_id = ObjectId(leave_id)
        leave_doc = await employees_leaves_collection.find_one(({"_id": ObjectId(leave_id)}))
        if not leave_doc:
            raise HTTPException(status_code=404, detail="Leave not found")
        status = leave_doc.get("status", "")
        return {"status": status}

    except Exception as e:
        raise

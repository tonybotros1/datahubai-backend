import copy
from typing import Optional, List, Any
from bson import ObjectId
from fastapi import APIRouter, HTTPException, Depends, Form, UploadFile, File
from pydantic import BaseModel
from app.core import security
from app.database import get_collection
from datetime import datetime

from app.routes.counters import create_custom_counter
from app.websocket_config import manager
from app.widgets import upload_images

router = APIRouter()
employees_collection = get_collection("employees")
employees_address_collection = get_collection("employees_address")
employees_contacts_and_relatives_collection = get_collection("employees_contacts_and_relatives")


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
                '$status', '$employer', '$department', '$job_title', '$location'
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
            'status_name': {
                '$let': {
                    'vars': {
                        'match': {
                            '$first': {
                                '$filter': {
                                    'input': '$lookup_data',
                                    'cond': {
                                        '$eq': [
                                            '$$this._id', '$status'
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
            '_id': {
                '$toString': '$_id'
            },
            'status': {
                '$toString': '$status'
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
                '$status', '$employer', '$department', '$job_title', '$location', '$gender', '$martial_status'
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
            'from': 'employees',
            'localField': 'reporting_manager',
            'foreignField': '_id',
            'as': 'reporting_manager_details'
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
        '$addFields': {
            'status_name': {
                '$let': {
                    'vars': {
                        'match': {
                            '$first': {
                                '$filter': {
                                    'input': '$lookup_data',
                                    'cond': {
                                        '$eq': [
                                            '$$this._id', '$status'
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
            '_id': {
                '$toString': '$_id'
            },
            'status': {
                '$toString': '$status'
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
                        '$first': '$reporting_manager_details.name'
                    }, None
                ]
            }
        }
    }, {
        '$project': {
            'lookup_data': 0,
            'all_ids': 0,
            'country_details': 0,
            'reporting_manager_details': 0
        }
    }
]


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
            "place_of_birth": place_of_birth,
            "date_of_birth": date_of_birth,
            "gender": ObjectId(gender) if gender else None,
            "martial_status": ObjectId(martial_status) if martial_status else None,
            "person_type": person_type,
            "status": ObjectId(status) if status else None,
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
                          person_image: UploadFile = File(None), data: dict = Depends(security.get_current_user)):
    try:
        company_id = data.get("company_id")
        employee_dict = {
            "full_name": full_name,
            "country_of_birth": ObjectId(country_of_birth) if country_of_birth else None,
            "place_of_birth": place_of_birth,
            "date_of_birth": date_of_birth,
            "gender": ObjectId(gender) if gender else None,
            "martial_status": ObjectId(martial_status) if martial_status else None,
            "person_type": person_type,
            "status": ObjectId(status) if status else None,
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
        result = await employees_collection.delete_one({"_id": ObjectId(employee_id)})
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
    date_of_birth: Optional[str] = None
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
        return {"new_contact": result if result else None}

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
        print(added_contact)
        return {"new_contact": added_contact}

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
        print(e)
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
        print(e)
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/delete_employee_address/{address_id}")
async def delete_employee_address(address_id: str, _: dict = Depends(security.get_current_user)):
    try:
        if not address_id:
            raise HTTPException(status_code=404, detail="Address ID not found")
        await employees_address_collection.delete_one({"_id": ObjectId(address_id)})
        return {"deleted_address_id": address_id}

    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=str(e))

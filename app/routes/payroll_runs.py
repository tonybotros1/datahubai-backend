import copy
from datetime import datetime
from typing import Optional, Any, List
from bson import ObjectId
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from app import database
from app.core import security
from app.database import get_collection
from app.routes.car_trading import PyObjectId
from app.routes.counters import create_custom_counter

router = APIRouter()
payroll_runs_collection = get_collection("payroll_runs")
payroll_runs_employees_collection = get_collection("payroll_runs_employees")
payroll_runs_employees_elements_collection = get_collection("payroll_runs_employees_elements")

payroll_collection = get_collection("payroll")
payroll_period_details_collection = get_collection("payroll_period_details")

employees_collection = get_collection("employees")
employees_payrolls_collection = get_collection("employees_payrolls")
payroll_elements_collection = get_collection("payroll_elements")


class PayrollRunModel(BaseModel):
    payroll_id: Optional[PyObjectId] = None
    period_id: Optional[PyObjectId] = None
    employee_id: Optional[PyObjectId] = None
    element_id: Optional[PyObjectId] = None


# is_element_processed ? (element_id,period_id)
async def is_element_processed(element_id: ObjectId, period_id: ObjectId) -> bool:
    try:
        result = await payroll_runs_employees_elements_collection.find_one({
            "period_id": period_id,
            "element_id": element_id
        })
        if result:
            return True
        else:
            return False

    except Exception:
        raise


async def save_payroll_run(payroll_id: ObjectId, period_id: ObjectId, description: str,
                           employee_ids: List[ObjectId], elements_values_maps: List[dict],
                           data: dict = Depends(security.get_current_user)):
    async with database.client.start_session() as session:
        try:
            await session.start_transaction()
            print(payroll_id)
            print(period_id)
            print(employee_ids)
            print(elements_values_maps)
            company_id = ObjectId(data.get("company_id"))
            new_run_counter = await create_custom_counter("PRN", "R", description='Payroll Run Number', data=data,
                                                          session=session)
            run_counter = new_run_counter["final_counter"] if new_run_counter["success"] else None

            run_dict = {
                "company_id": company_id,
                "run_number": run_counter,
                "payroll_id": payroll_id,
                "period_id": period_id,
                "description": description,
                "payment_number": "",
                "createdAt": security.now_utc(),
                "updatedAt": security.now_utc(),
            }
            run_result = await payroll_runs_collection.insert_one(run_dict, session=session)
            run_id = run_result.inserted_id

            for employee_id in employee_ids:
                run_employee_dict = {
                    "company_id": company_id,
                    "run_id": run_id,
                    "period_id": period_id,
                    "payroll_id": payroll_id,
                    "employee_id": employee_id,
                    "createdAt": security.now_utc(),
                    "updatedAt": security.now_utc(),
                }

                run_employee_result = await payroll_runs_employees_collection.insert_one(run_employee_dict,
                                                                                         session=session)
                run_employee_id = run_employee_result.inserted_id

                for elements_values_map in elements_values_maps:
                    element_id, value = next(iter(elements_values_map.items()))
                    run_employee_element_dict = {
                        "company_id": company_id,
                        "run_employee_id": run_employee_id,
                        "element_id": element_id,
                        "value": value,
                        "run_id": run_id,
                        "period_id": period_id,
                        "payroll_id": payroll_id,
                        "createdAt": security.now_utc(),
                        "updatedAt": security.now_utc(),
                    }
                    if run_employee_element_dict:
                        run_employee_result = await payroll_runs_employees_elements_collection.insert_one(
                            run_employee_element_dict,
                            session=session)
                        run_employee_element_id = run_employee_result.inserted_id

            await session.commit_transaction()

        except Exception:
            await session.abort_transaction()
            raise


@router.post("/payroll_run")
async def payroll_run(run: PayrollRunModel, data: dict = Depends(security.get_current_user)):
    try:
        payroll_id = run.payroll_id
        period_id = run.period_id
        employee_id = run.employee_id
        element_id = run.element_id
        all_employees = []
        # === period section ===:
        period_document = await payroll_period_details_collection.find_one({"_id": period_id})
        period_start_date = period_document.get("start_date")
        period_end_date = period_document.get("end_date")

        if not period_start_date or not period_end_date:
            raise HTTPException(status_code=400, detail="period dates are required")

        # === employees section ===:
        employee_filter = {
            "status": "Active",
            "person_type": "Employee",
            "payroll": payroll_id,
            "hire_date": {"$lte": period_end_date},
            "$or": [
                {"end_date": {"$gte": period_start_date}},
                {"end_date": None}
            ]
        }

        if employee_id:
            employee_filter["_id"] = employee_id
            employee_document = await employees_collection.find_one(employee_filter)
            if employee_document:
                all_employees.append(employee_document)
        else:
            employees = await employees_collection.find(employee_filter).to_list(None)
            all_employees.extend(employees)

        # === loop employees ===:
        description = ""
        elements_values_maps = []
        for employee in all_employees:
            current_employee_id = employee.get("_id")
            employee_hire_date = employee.get("hire_date") or datetime.min
            employee_end_date = employee.get("end_date") or datetime.max
            employee_name = employee.get("full_name") or None
            description = employee_name if len(all_employees) == 1 else "All Employees"

            # === payroll elements ===:
            element_filter = {
                "employee_id": current_employee_id,
                "start_date": {"$lte": period_end_date},
                "$or": [
                    {"end_date": {"$gte": period_start_date}},
                    {"end_date": None},
                ]
            }
            if element_id:
                element_filter["name"] = element_id
                element = await employees_payrolls_collection.find_one(element_filter)
                payroll_elements = [element] if element else []
            else:
                payroll_elements = await employees_payrolls_collection.find(element_filter).to_list(None)

            for employee_payroll in payroll_elements:
                if not employee_payroll:
                    continue
                if not await is_element_processed(employee_payroll["_id"], period_id):
                    element_start = employee_payroll.get("start_date") or datetime.min
                    element_end = employee_payroll.get("end_date") or datetime.max
                    element_value = employee_payroll.get("value", 0)
                    element_function = await payroll_elements_collection.find_one(
                        {"_id": employee_payroll.get("name")}, {"function": 1, "_id": 0})
                    element_function = element_function['function'] if element_function else None
                    if element_function:
                        if element_function.upper() == "PY_INPUT_VALUE_FF":
                            value = await py_input_value_ff(employee_hire_date, employee_end_date, element_start,
                                                            element_value,
                                                            element_end, period_start_date, period_end_date)
                            elements_values_maps.append({
                                employee_payroll.get("_id"): value
                            })
            print(elements_values_maps)
        await save_payroll_run(payroll_id, period_id, description, [item["_id"] for item in all_employees],
                               elements_values_maps, data)

    except Exception:
        raise


# ==== PY_INPUT_VALUE_FF ====
async def py_input_value_ff(employee_hire_date: datetime, employee_end_date: datetime, element_start: datetime,
                            element_value: float,
                            element_end: datetime, period_start_date: datetime, period_end_date: datetime):
    try:
        date1 = max(employee_hire_date, element_start, period_start_date)
        date2 = min(employee_end_date, element_end, period_end_date)

        working_days = max((date2 - date1).days + 1, 0)
        period_days = max((period_end_date - period_start_date).days + 1, 0)

        if period_days == 0:
            final_value = 0
        else:
            final_value = element_value * (working_days / period_days)

        return round(final_value, 2)

    except Exception as e:
        raise e


## ====================================================================================================================================================================================================
## ====================================================================================================================================================================================================


all_payroll_runs_pipeline = [
    {
        '$lookup': {
            'from': 'payroll',
            'localField': 'payroll_id',
            'foreignField': '_id',
            'as': 'payroll_details'
        }
    }, {
        '$lookup': {
            'from': 'payroll_period_details',
            'localField': 'period_id',
            'foreignField': '_id',
            'as': 'period_details'
        }
    }, {
        '$addFields': {
            'payroll_name': {
                '$ifNull': [
                    {
                        '$first': '$payroll_details.name'
                    }, None
                ]
            },
            'period_name': {
                '$ifNull': [
                    {
                        '$first': '$period_details.period_name'
                    }, None
                ]
            }
        }
    }, {
        '$project': {
            '_id': {
                '$toString': '$_id'
            },
            'run_number': 1,
            'description': 1,
            'payment_number': 1,
            'payroll_name': 1,
            'period_name': 1
        }
    }
]

payroll_runs_details_pipeline = [
    {
        '$lookup': {
            'from': 'payroll',
            'localField': 'payroll_id',
            'foreignField': '_id',
            'pipeline': [
                {
                    '$project': {
                        '_id': 0,
                        'name': 1
                    }
                }
            ],
            'as': 'payroll_details'
        }
    }, {
        '$lookup': {
            'from': 'payroll_period_details',
            'localField': 'period_id',
            'foreignField': '_id',
            'pipeline': [
                {
                    '$project': {
                        '_id': 0,
                        'period_name': 1
                    }
                }
            ],
            'as': 'period_details'
        }
    }, {
        '$lookup': {
            'from': 'payroll_runs_employees',
            'localField': '_id',
            'foreignField': 'run_id',
            'pipeline': [
                {
                    '$lookup': {
                        'from': 'employees',
                        'localField': 'employee_id',
                        'foreignField': '_id',
                        'pipeline': [
                            {
                                '$project': {
                                    '_id': 0,
                                    'full_name': 1
                                }
                            }
                        ],
                        'as': 'employee_details'
                    }
                }, {
                    '$lookup': {
                        'from': 'payroll_runs_employees_elements',
                        'localField': '_id',
                        'foreignField': 'run_employee_id',
                        'pipeline': [
                            {
                                '$lookup': {
                                    'from': 'employees_payrolls',
                                    'localField': 'element_id',
                                    'foreignField': '_id',
                                    'pipeline': [
                                        {
                                            '$project': {
                                                '_id': 0,
                                                'payroll_element_id': '$name'
                                            }
                                        }
                                    ],
                                    'as': 'employee_payroll_details'
                                }
                            }, {
                                '$set': {
                                    'payroll_element_id': {
                                        '$first': '$employee_payroll_details.payroll_element_id'
                                    }
                                }
                            }, {
                                '$lookup': {
                                    'from': 'payroll_elements',
                                    'localField': 'payroll_element_id',
                                    'foreignField': '_id',
                                    'pipeline': [
                                        {
                                            '$project': {
                                                '_id': 0,
                                                'name': 1,
                                                'type': 1
                                            }
                                        }
                                    ],
                                    'as': 'element_details'
                                }
                            }, {
                                '$set': {
                                    'element_name': {
                                        '$first': '$element_details.name'
                                    },
                                    'element_type': {
                                        '$first': '$element_details.type'
                                    }
                                }
                            }, {
                                '$project': {
                                    '_id': {
                                        '$toString': '$_id'
                                    },
                                    'value': 1,
                                    'element_name': 1,
                                    'element_type': 1,
                                    'payment': {
                                        '$cond': [
                                            {
                                                '$eq': [
                                                    '$element_type', 'Earning'
                                                ]
                                            }, '$value', 0
                                        ]
                                    },
                                    'deduction': {
                                        '$cond': [
                                            {
                                                '$eq': [
                                                    '$element_type', 'Deduction'
                                                ]
                                            }, '$value', 0
                                        ]
                                    }
                                }
                            }
                        ],
                        'as': 'run_employee_details'
                    }
                }, {
                    '$set': {
                        'employee_name': {
                            '$first': '$employee_details.full_name'
                        },
                        'total_payments': {
                            '$sum': '$run_employee_details.payment'
                        },
                        'total_deductions': {
                            '$sum': '$run_employee_details.deduction'
                        }
                    }
                }, {
                    '$set': {
                        'net_salary': {
                            '$subtract': [
                                '$total_payments', '$total_deductions'
                            ]
                        }
                    }
                }, {
                    '$project': {
                        '_id': {
                            '$toString': '$_id'
                        },
                        'employee_name': 1,
                        'total_payments': 1,
                        'total_deductions': 1,
                        'net_salary': 1,
                        'run_employee_details': 1
                    }
                }
            ],
            'as': 'employees_details'
        }
    }, {
        '$project': {
            '_id': {
                '$toString': '$_id'
            },
            'run_number': 1,
            'description': 1,
            'payment_number': 1,
            'payroll_name': {
                '$first': '$payroll_details.name'
            },
            'period_name': {
                '$first': '$period_details.period_name'
            },
            'employees_details': 1
        }
    }
]


@router.get("/get_all_payroll_runs")
async def get_all_payroll_runs(data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        new_pipeline: Any = copy.deepcopy(all_payroll_runs_pipeline)
        new_pipeline.insert(0, {"$match": {"company_id": company_id}})
        cursor = await payroll_runs_collection.aggregate(new_pipeline)
        results = await cursor.to_list(None)
        return {"payroll_runs": results}

    except Exception:
        raise


@router.get("/get_payroll_runs_details/{run_id}")
async def get_payroll_runs_details(run_id: str, data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        run_id = ObjectId(run_id)
        new_pipeline: Any = copy.deepcopy(payroll_runs_details_pipeline)
        new_pipeline.insert(0, {"$match": {"company_id": company_id, "_id": run_id}})
        cursor = await payroll_runs_collection.aggregate(new_pipeline)
        results = await cursor.to_list(None)
        print(results)
        return {"payroll_runs_details": results[0] if len(results) > 0 else None}

    except Exception:
        raise


##### ============= FUNCTIONS TO GET LOVs ============= #####
@router.get("/get_payroll_for_lov")
async def get_payroll_for_lov(data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        cursor = await payroll_collection.aggregate([
            {"$match": {"company_id": company_id}},
            {"$set": {"_id": {"$toString": "$_id"}}},
            {"$project": {
                "_id": 1,
                "name": 1
            }}
        ])
        results = await cursor.to_list(None)
        return {"all_payrolls": results}

    except Exception:
        raise


@router.get("/get_payroll_periods_for_lov/{payroll_id}")
async def get_payroll_periods_for_lov(payroll_id: str, _: dict = Depends(security.get_current_user)):
    try:
        payroll_id = ObjectId(payroll_id)
        cursor = await payroll_period_details_collection.aggregate([
            {"$match": {"payroll_id": payroll_id, "status": "Active"}},
            {"$set": {"_id": {"$toString": "$_id"}}},
            {"$project": {
                "_id": 1,
                "period_name": 1
            }},
            {"$sort": {"period_name": -1}}
        ])
        results = await cursor.to_list(None)
        return {"all_periods": results}

    except Exception:
        raise


@router.get("/get_all_employees_for_payroll_runs_lov/{payroll_id}")
async def get_all_employees_for_payroll_runs_lov(payroll_id: str, _: dict = Depends(security.get_current_user)):
    try:
        payroll_id = ObjectId(payroll_id)
        cursor = await employees_collection.aggregate([
            {"$match": {"payroll": payroll_id, "status": "Active", "person_type": "Employee"}},
            {"$set": {"_id": {"$toString": "$_id"}}},
            {"$project": {
                "_id": 1,
                "full_name": 1
            }},
        ])
        results = await cursor.to_list(None)
        return {"all_employees": results}

    except Exception:
        raise

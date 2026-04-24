import copy
from datetime import datetime
from typing import Optional, Any
from bson import ObjectId
from bson.errors import InvalidId
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from app import database
from app.core import security
from app.database import get_collection
from app.routes.car_trading import PyObjectId
from app.routes.counters import create_custom_counter
from app.routes.payroll_runs_widgets.helpers_functions import get_employee_element_value, \
    get_period_days, get_used_sick_days, get_current_leave_used_days

router = APIRouter()
payroll_runs_collection = get_collection("payroll_runs")
payroll_runs_employees_collection = get_collection("payroll_runs_employees")
payroll_runs_employees_elements_collection = get_collection("payroll_runs_employees_elements")

payroll_collection = get_collection("payroll")
payroll_period_details_collection = get_collection("payroll_period_details")
leave_types_collection = get_collection("leave_types")

employees_collection = get_collection("employees")
employees_payrolls_collection = get_collection("employees_payrolls")
payroll_elements_collection = get_collection("payroll_elements")
employees_leaves_collection = get_collection("employees_leaves")
legislations_collection = get_collection("legislations")


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


async def save_payroll_run(payroll_id: ObjectId, period_id: ObjectId, description: str, employee_ids: list[ObjectId],
                           elements_values_maps: dict, data: dict = Depends(security.get_current_user),
                           ) -> str:
    async with database.client.start_session() as session:
        try:
            await session.start_transaction()

            company_id = ObjectId(data.get("company_id"))

            # === create run counter ===
            new_run_counter = await create_custom_counter("PRN", "R", description="Payroll Run Number", data=data,
                                                          session=session)

            run_counter = new_run_counter["final_counter"] if new_run_counter["success"] else None

            # === create payroll run ===
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

            # === loop employees ===
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

                run_employee_result = await payroll_runs_employees_collection.insert_one(
                    run_employee_dict,
                    session=session,
                )

                run_employee_id = run_employee_result.inserted_id

                # === get this employee elements ONLY ===
                employee_elements = elements_values_maps.get(employee_id, [])

                for element in employee_elements:
                    element_id = element["element_id"]
                    value = element["value"]
                    payroll_element_id = element["payroll_element_id"]
                    number = element["number"]

                    # 🔥 CHECK IF EXISTS (duplicate run rule)
                    existing = await payroll_runs_employees_elements_collection.find_one(
                        {
                            "employee_id": employee_id,
                            "element_id": element_id,
                            "period_id": period_id,
                            "payroll_id": payroll_id,
                        }
                    )

                    if existing:
                        value = 0  # 👈 your business rule

                    run_employee_element_dict = {
                        "company_id": company_id,
                        "run_employee_id": run_employee_id,
                        "employee_id": employee_id,
                        "element_id": element_id,
                        "value": value,
                        "payroll_element_id": payroll_element_id,
                        "run_id": run_id,
                        "number": number,
                        "period_id": period_id,
                        "payroll_id": payroll_id,
                        "createdAt": security.now_utc(),
                        "updatedAt": security.now_utc(),
                    }

                    await payroll_runs_employees_elements_collection.insert_one(
                        run_employee_element_dict,
                        session=session,
                    )

            await session.commit_transaction()

            return str(run_id)


        except Exception as e:
            await session.abort_transaction()
            raise e


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
        elements_values_maps = {}
        for employee in all_employees:
            current_employee_id = employee.get("_id")
            elements_values_maps[current_employee_id] = []
            employee_hire_date = employee.get("hire_date") or datetime.min
            employee_end_date = employee.get("end_date") or datetime.max
            employee_name = employee.get("full_name") or None
            description = employee_name if len(all_employees) == 1 else "All Employees"
            legislation = employee.get("legislation") or None

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
                    # element_value = await get_employee_element_value(employee_payroll.get("_id"), employee_id)
                    element_value = employee_payroll.get("value")
                    element_function = await payroll_elements_collection.find_one(
                        {"_id": employee_payroll.get("name")}, {"function": 1, "_id": 0})
                    element_function = element_function['function'] if element_function else None
                    if element_function:
                        if element_function.upper() == "PY_INPUT_VALUE_FF":
                            value = await py_input_value_ff(employee_hire_date, employee_end_date, element_start,
                                                            element_value,
                                                            element_end, period_start_date, period_end_date)
                            elements_values_maps[current_employee_id].append({
                                "element_id": employee_payroll.get("_id"),
                                "value": value,
                                "payroll_element_id": employee_payroll.get("name"),
                                "number": None
                            })

            # === employee leaves ===:
            employee_leaves = await employees_leaves_collection.find(
                {"employee_id": current_employee_id, "status": "Posted", "start_date": {"$lte": period_end_date},
                 "end_date": {"$gte": period_start_date}}).to_list(None)
            for leave in employee_leaves:
                leave_type = leave.get("leave_type")
                if not leave_type:
                    continue

                leave_type_doc = await leave_types_collection.find_one({"_id": leave_type})
                if not leave_type_doc:
                    continue

                based_element_id = leave_type_doc.get("based_element")
                number_of_days = leave.get("number_of_days")
                if not based_element_id:
                    continue

                if not number_of_days:
                    leave_type_name = leave_type_doc.get("name", "Selected leave type")
                    raise HTTPException(
                        status_code=400,
                        detail=f"{leave_type_name} is missing number_of_days for annual leave calculation",
                    )

                if not await is_element_processed(leave["_id"], period_id):
                    payroll_element_doc = await payroll_elements_collection.find_one({"_id": based_element_id})
                    function = payroll_element_doc.get("function") if payroll_element_doc else None
                    if function.upper() == "PY_ANNUAL_LEAVE_FF":
                        final_value = await py_annual_leave_ff(current_employee_id, period_start_date,
                                                               period_end_date, based_element_id,
                                                               number_of_days)

                        elements_values_maps[current_employee_id].append({
                            "element_id": leave["_id"],
                            "value": final_value,
                            "payroll_element_id": based_element_id,
                            "number": None
                        })
                    elif function.upper() == "PY_UNPAID_LEAVE_FF":
                        final_value = await py_unpaid_leave_ff(current_employee_id, period_start_date,
                                                               period_end_date, based_element_id,
                                                               leave.get("start_date"), leave.get("end_date"))

                        elements_values_maps[current_employee_id].append({
                            "element_id": leave["_id"],
                            "value": final_value,
                            "payroll_element_id": based_element_id,
                            "number": None
                        })
                    elif function.upper() == "PY_SICK_LEAVE_FF":
                        final_value = await py_sick_leave_ff(current_employee_id, period_start_date,
                                                             period_end_date, based_element_id,
                                                             legislation,
                                                             leave.get("start_date"), leave.get("end_date"))

                        elements_values_maps[current_employee_id].append({
                            "element_id": leave["_id"],
                            "value": final_value,
                            "payroll_element_id": based_element_id,
                            "number": None
                        })
                    elif function.upper() == "PY_MATERNITY_LEAVE_FF":
                        final_value = await py_maternity_leave_ff(current_employee_id, period_start_date,
                                                             period_end_date, based_element_id,
                                                             legislation,
                                                             leave.get("start_date"), leave.get("end_date"))

                        elements_values_maps[current_employee_id].append({
                            "element_id": leave["_id"],
                            "value": final_value,
                            "payroll_element_id": based_element_id,
                            "number": None
                        })

        run_id: str = await save_payroll_run(payroll_id, period_id, description,
                                             [item["_id"] for item in all_employees],
                                             elements_values_maps, data)

        details = await get_payroll_runs_details(run_id, data)
        return {"added_run": details["payroll_runs_details"]}


    except Exception:
        raise


# ==== PY_INPUT_VALUE_FF ====
async def py_input_value_ff(employee_hire_date: datetime, employee_end_date: datetime, element_start: datetime,
                            element_value: float, element_end: datetime, period_start_date: datetime,
                            period_end_date: datetime):
    try:
        date1 = max(employee_hire_date, element_start, period_start_date)
        date2 = min(employee_end_date, element_end, period_end_date)
        # number_of_leave_days_dict = await get_leave_days(employee_id, date1, date2, company_id)

        # number_of_leave_days = number_of_leave_days_dict['number_of_leave_days']
        # working_days = max((date2 - date1).days + 1, 0) - number_of_leave_days
        working_days = max((date2 - date1).days + 1, 0)
        period_days = get_period_days(period_start_date, period_end_date)

        if period_days == 0:
            final_value = 0
        else:
            final_value = element_value * (working_days / period_days)

        return round(final_value, 2)

    except Exception as e:
        raise e


# ==== PY_ANNUAL_LEAVE_FF ====
async def py_annual_leave_ff(employee_id: ObjectId, period_start_date: datetime, period_end_date: datetime,
                             based_element_id: ObjectId, number_of_days: int):
    try:
        value = await get_employee_element_value(based_element_id, employee_id)
        period_days = get_period_days(period_start_date, period_end_date)
        final_value = round(((value or 0) * (number_of_days / period_days)), 2)
        return final_value

    except Exception:
        raise


# ==== PY_UNPAID_LEAVE_FF ====
async def py_unpaid_leave_ff(employee_id: ObjectId, period_start_date: datetime, period_end_date: datetime,
                             based_element_id: ObjectId, leave_start_date: datetime, leave_end_date: datetime):
    try:
        value = await get_employee_element_value(based_element_id, employee_id)
        period_days = get_period_days(period_start_date, period_end_date)

        date1 = max(period_start_date, leave_start_date)
        date2 = min(period_end_date, leave_end_date)
        l_days = (date2 - date1).days + 1

        final_value = round(((value or 0) * (l_days / period_days)), 2)

        return final_value

    except Exception:
        raise


# ==== PY_SICK_LEAVE_FF ====
async def py_sick_leave_ff(employee_id: ObjectId, period_start_date: datetime, period_end_date: datetime,
                           based_element_id: ObjectId, legislation: ObjectId, leave_start_date: datetime,
                           leave_end_date: datetime):
    try:
        value = await get_employee_element_value(based_element_id, employee_id)
        period_days = get_period_days(period_start_date, period_end_date)

        legislation_doc = await legislations_collection.find_one({"_id": legislation})
        if not legislation_doc:
            raise HTTPException(status_code=404, detail="Legislation not found")

        full_limit = legislation_doc.get("number_of_paid_days_for_sick_leave", 0)
        half_limit = legislation_doc.get("number_of_half_paid_days_for_sick_leave", 0)

        #  get previously used days
        used_days_before = await get_used_sick_days(employee_id, leave_start_date, leave_end_date, period_start_date,
                                                    period_end_date)

        date1 = max(period_start_date, leave_start_date)
        date2 = min(period_end_date, leave_end_date)
        l_days = (date2 - date1).days + 1

        remaining_days = l_days
        total_value = 0

        # === FULL PAID ===
        remaining_full = max(0, full_limit - used_days_before)
        full_paid_days = min(remaining_days, remaining_full)

        total_value += value * (full_paid_days / period_days) * 0
        remaining_days -= full_paid_days

        # === HALF PAID ===
        used_after_full = max(0, used_days_before - full_limit)
        remaining_half = max(0, half_limit - used_after_full)

        half_paid_days = min(remaining_days, remaining_half)

        total_value += value * (half_paid_days / period_days) * 0.5
        remaining_days -= half_paid_days

        # === UNPAID ===
        if remaining_days > 0:
            total_value += value * (remaining_days / period_days) * 1.0

        return round(total_value, 2)

    except Exception:
        raise


# ==== PY_MATERNITY_LEAVE_FF ====
async def py_maternity_leave_ff(employee_id: ObjectId, period_start_date: datetime, period_end_date: datetime,
                                based_element_id: ObjectId, legislation: ObjectId, leave_start_date: datetime,
                                leave_end_date: datetime):
    try:
        value = await get_employee_element_value(based_element_id, employee_id)
        period_days = get_period_days(period_start_date, period_end_date)

        legislation_doc = await legislations_collection.find_one({"_id": legislation})
        if not legislation_doc:
            raise HTTPException(status_code=404, detail="Legislation not found")

        full_limit = legislation_doc.get("number_of_paid_days_for_maternity_leave", 0)

        #  get previously used days
        used_days_before = get_current_leave_used_days(period_start_date, leave_start_date)

        date1 = max(period_start_date, leave_start_date)
        date2 = min(period_end_date, leave_end_date)
        l_days = (date2 - date1).days + 1

        remaining_days = l_days
        total_value = 0

        # === FULL PAID ===
        remaining_full = max(0, full_limit - used_days_before)
        full_paid_days = min(remaining_days, remaining_full)

        total_value += value * (full_paid_days / period_days) * 0
        remaining_days -= full_paid_days

        # === UNPAID ===
        remaining_unpaid = 0

        unpaid_days = min(remaining_days, remaining_unpaid)

        total_value += value * (unpaid_days / period_days) * 1.0
        remaining_days -= unpaid_days

        return round(total_value, 2)

    except Exception:
        raise


# === ROLLBACK ===
@router.delete("/rollback_payroll_run/{run_id}")
async def rollback_payroll_run(run_id: str, _: dict = Depends(security.get_current_user)):
    try:
        run_id = ObjectId(run_id)
    except InvalidId:
        raise HTTPException(status_code=400, detail="Invalid run_id")

    async with database.client.start_session() as session:
        try:
            await session.start_transaction()

            # Delete children first
            await payroll_runs_employees_elements_collection.delete_many(
                {"run_id": run_id}, session=session
            )
            await payroll_runs_employees_collection.delete_many(
                {"run_id": run_id}, session=session
            )

            # Delete main run
            result = await payroll_runs_collection.delete_one(
                {"_id": run_id}, session=session
            )

            if result.deleted_count == 0:
                await session.abort_transaction()
                raise HTTPException(status_code=404, detail="Payroll run not found")

            await session.commit_transaction()

            return {"message": "Payroll run rolled back successfully"}

        except Exception as e:
            await session.abort_transaction()
            raise HTTPException(status_code=500, detail=str(e))


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
                                    'priority': 1,
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
                            }, {
                                '$match': {
                                    'element_type': {
                                        '$ne': 'Information'
                                    }
                                }
                            }, {
                                '$sort': {
                                    'priority': 1
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
                }, {
                    '$sort': {
                        'employee_name': 1
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

        # # =================================================
        # start = datetime(2026, 4, 1)
        # end = datetime(2026, 4, 30)
        # await get_leave_days(ObjectId("69cfa8718f07622eb9ce9b68"), start, end)
        # # =================================================

        # # =================================================
        # await get_payroll_element_value(ObjectId("69d64ad68fc5df07583ec9a8"),ObjectId("69cfa8718f07622eb9ce9b68"))
        # # =================================================

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

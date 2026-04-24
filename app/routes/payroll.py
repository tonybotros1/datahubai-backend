import copy
from datetime import datetime, timedelta
from typing import Optional, Any
from bson import ObjectId
from dateutil.relativedelta import relativedelta
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from app import database
from app.core import security
from app.database import get_collection

router = APIRouter()
payroll_collection = get_collection("payroll")
payroll_period_details_collection = get_collection("payroll_period_details")


class PayrollModel(BaseModel):
    name: Optional[str] = None
    notes: Optional[str] = None
    payment_type: Optional[str] = None


class PeriodPayrollModel(BaseModel):
    period_name: Optional[str] = None
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    status: Optional[str] = None


class MonthlyPeriodsModel(BaseModel):
    year_start_date: Optional[datetime] = None


payroll_details_pipeline = [
    {
        '$lookup': {
            'from': 'payroll_period_details',
            'localField': '_id',
            'foreignField': 'payroll_id',
            'pipeline': [
                {
                    '$set': {
                        '_id': {
                            '$toString': '$_id'
                        },
                        'payroll_id': {
                            '$toString': '$payroll_id'
                        },
                        'company_id': {
                            '$toString': '$company_id'
                        }
                    }
                },
                {
                    '$sort': {
                        'period_name': -1
                    }
                }
            ],
            'as': 'details'
        }
    }, {
        '$lookup': {
            'from': 'ap_payment_types',
            'localField': 'payment_type',
            'foreignField': '_id',
            'as': 'payment_type_details'
        }
    }, {
        '$set': {
            '_id': {
                '$toString': '$_id'
            },
            'company_id': {
                '$toString': '$company_id'
            },
            'payment_type': {
                '$toString': '$payment_type'
            },
            'payment_type_name': {
                '$ifNull': [
                    {
                        '$first': '$payment_type_details.type'
                    }, None
                ]
            }
        }
    },
    {
        '$project': {
            'payment_type_details': 0
        }
    }
]


async def get_payroll_details(payroll_id: ObjectId):
    try:
        new_pipeline: Any = copy.deepcopy(payroll_details_pipeline)
        new_pipeline.insert(0, {
            "$match": {
                "_id": payroll_id
            }
        })

        cursor = await payroll_collection.aggregate(new_pipeline)
        result = await cursor.to_list(None)
        return result[0] if result else None
    except Exception:
        raise


@router.get("/get_current_payroll_details/{payroll_id}")
async def get_current_payroll_details(payroll_id: str, _: dict = Depends(security.get_current_user)):
    try:
        payroll_id = ObjectId(payroll_id)
        new_pipeline: Any = copy.deepcopy(payroll_details_pipeline)
        new_pipeline.insert(0, {
            "$match": {
                "_id": payroll_id
            }
        })
        cursor = await payroll_collection.aggregate(new_pipeline)
        result = await cursor.to_list(None)
        return {"payroll_details": result[0] if result else None}

    except Exception:
        raise


@router.get("/get_all_payrolls")
async def get_all_payrolls(data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))

        cursor = await payroll_collection.aggregate([
            {
                "$match": {"company_id": company_id}
            },
            {
                '$lookup': {
                    'from': 'ap_payment_types',
                    'localField': 'payment_type',
                    'foreignField': '_id',
                    'as': 'payment_type_details'
                }
            },
            {
                '$set': {

                    'payment_type_name': {
                        '$ifNull': [
                            {
                                '$first': '$payment_type_details.type'
                            }, None
                        ]
                    }
                }
            },

            {
                "$project": {
                    "_id": {"$toString": "$_id"},
                    "name": 1,
                    "notes": 1,
                    "payment_type_name": 1
                }
            }
        ])
        results = await cursor.to_list(None)

        return {"all_payrolls": results if results else []}

    except HTTPException:
        raise


@router.post("/create_new_payroll")
async def create_new_payroll(payroll: PayrollModel, data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        payroll = payroll.model_dump(exclude_unset=True)
        name = payroll.get("name")
        now = security.now_utc()
        payroll_dict = {
            "company_id": company_id,
            "name": name,
            "notes": payroll.get("notes"),
            "payment_type": ObjectId(payroll.get("payment_type")) if payroll.get("payment_type") else None,
            "createdAt": now,
            "updatedAt": now,
        }
        result = await payroll_collection.insert_one(payroll_dict)
        if not result.inserted_id:
            raise HTTPException(status_code=500, detail="Failed to create Payroll")
        added_details = await get_payroll_details(result.inserted_id)
        return {"added_details": added_details}

    except Exception:
        raise


@router.patch("/update_payroll/{payroll_id}")
async def update_payroll(payroll_id: str, payroll: PayrollModel, _: dict = Depends(security.get_current_user)):
    try:
        payroll_id = ObjectId(payroll_id)
        payroll = payroll.model_dump(exclude_unset=True)
        name = payroll.get("name")
        now = security.now_utc()
        payroll_dict = {
            "name": name,
            "notes": payroll.get("notes"),
            "payment_type": ObjectId(payroll.get("payment_type")) if payroll.get("payment_type") else None,
            "updatedAt": now,
        }
        result = await payroll_collection.update_one({"_id": payroll_id}, {"$set": payroll_dict})
        if result.matched_count == 0:
            raise HTTPException(status_code=500, detail="Failed to update Payroll")
        updated_details = await get_payroll_details(ObjectId(payroll_id))

        return {"updated_data": updated_details}

    except Exception:
        raise


@router.delete("/delete_payroll/{payroll_id}")
async def delete_payroll(payroll_id: str, _: dict = Depends(security.get_current_user)):
    async with database.client.start_session() as session:
        try:
            await session.start_transaction()
            payroll_id = ObjectId(payroll_id)
            result = await payroll_collection.delete_one({"_id": payroll_id}, session=session)
            if result.deleted_count == 0:
                raise HTTPException(status_code=500, detail="Failed to delete Payroll")

            await payroll_period_details_collection.delete_many({"payroll_id": payroll_id}, session=session)

            await session.commit_transaction()

        except Exception:
            await session.abort_transaction()
            raise


@router.post("/add_new_period/{payroll_id}")
async def add_new_period(payroll_id: str, period: PeriodPayrollModel, data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        payroll_id = ObjectId(payroll_id)
        period = period.model_dump(exclude_unset=True)
        period_dict = {
            "company_id": company_id,
            "payroll_id": payroll_id,
            "period_name": period.get("period_name"),
            "start_date": period.get("start_date"),
            "end_date": period.get("end_date"),
            "status": period.get("status"),
            "createdAt": security.now_utc(),
            "updatedAt": security.now_utc(),
        }
        result = await payroll_period_details_collection.insert_one(period_dict)
        if not result.inserted_id:
            raise HTTPException(status_code=500, detail="Failed to create Period")
        return {"added_period": {
            "_id": str(result.inserted_id),
            "period_name": period_dict['period_name'],
            "start_date": period_dict['start_date'],
            "end_date": period_dict['end_date'],
            "status": period_dict['status'],
        }}

    except Exception:
        raise


@router.patch("/update_period/{period_id}")
async def update_period(period_id: str, period: PeriodPayrollModel, _: dict = Depends(security.get_current_user)):
    try:
        period_id = ObjectId(period_id)
        period = period.model_dump(exclude_unset=True)
        period_dict = {
            "period_name": period.get("period_name"),
            "start_date": period.get("start_date"),
            "end_date": period.get("end_date"),
            "status": period.get("status"),
            "updatedAt": security.now_utc(),
        }
        result = await payroll_period_details_collection.update_one({"_id": period_id}, {"$set": period_dict})
        if result.matched_count == 0:
            raise HTTPException(status_code=500, detail="Failed to update Period")
        return {"updated_period": {
            "_id": str(period_id),
            "period_name": period_dict['period_name'],
            "start_date": period_dict['start_date'],
            "end_date": period_dict['end_date'],
            "status": period_dict['status'],
        }}

    except Exception:
        raise


@router.delete("/delete_period/{period_id}")
async def delete_period(period_id: str, _: dict = Depends(security.get_current_user)):
    try:
        period_id = ObjectId(period_id)
        result = await payroll_period_details_collection.delete_one({"_id": period_id})
        if result.deleted_count == 0:
            raise HTTPException(status_code=500, detail="Failed to delete period")

    except Exception:
        raise


@router.post("/generate_monthly_periods/{payroll_id}")
async def generate_monthly_periods(
        payroll_id: str,
        gen_data: MonthlyPeriodsModel,
        data: dict = Depends(security.get_current_user),
):
    try:
        company_id = ObjectId(data.get("company_id"))
        payroll_id = ObjectId(payroll_id)

        if not gen_data.year_start_date:
            raise HTTPException(status_code=400, detail="Year start date must be provided")

        payroll_doc = await payroll_collection.find_one({"_id": payroll_id})
        if not payroll_doc:
            raise HTTPException(status_code=404, detail="Payroll not found")

        payroll_name = payroll_doc.get("name", "Payroll")
        payroll_start_date = gen_data.year_start_date
        cycle_end_date = payroll_start_date + relativedelta(months=12)

        # Check existing periods in the selected payroll year/cycle.
        existing_periods = await payroll_period_details_collection.find(
            {
                "payroll_id": payroll_id,
                "company_id": company_id,
                "start_date": {
                    "$gte": payroll_start_date,
                    "$lt": cycle_end_date,
                },
            },
            {
                "start_date": 1,
            },
        ).to_list(length=None)

        existing_months = {
            (period["start_date"].year, period["start_date"].month)
            for period in existing_periods
            if period.get("start_date")
        }

        periods = []
        current_start = payroll_start_date

        for _ in range(12):
            period_start = current_start
            period_end = period_start + relativedelta(months=1) - timedelta(days=1)
            month_key = (period_start.year, period_start.month)

            if month_key not in existing_months:
                periods.append(
                    {
                        "payroll_id": payroll_id,
                        "company_id": company_id,
                        "period_name": f"{period_start.strftime('%Y')}-{period_start.strftime('%m')}-{payroll_name}",
                        "status": "Active",
                        "start_date": period_start,
                        "end_date": period_end,
                        "createdAt": security.now_utc(),
                        "updatedAt": security.now_utc(),
                    }
                )

            current_start = period_end + timedelta(days=1)

        if periods:
            final_result = await payroll_period_details_collection.insert_many(periods)
            if not final_result.inserted_ids:
                raise HTTPException(status_code=500, detail="Failed to create Monthly Periods")

        cursor = await payroll_period_details_collection.aggregate(
            [
                {
                    "$match": {
                        "payroll_id": payroll_id,
                        "company_id": company_id,
                        "status": "Active",
                        "start_date": {
                            "$gte": payroll_start_date,
                            "$lt": cycle_end_date,
                        },
                    }
                },
                {
                    "$sort": {
                        "start_date": 1
                    }
                },
                {
                    "$set": {
                        "_id": {
                            "$toString": "$_id"
                        }
                    }
                },
                {
                    "$project": {
                        "company_id": 0,
                        "payroll_id": 0,
                        "createdAt": 0,
                        "updatedAt": 0
                    }
                }
            ]
        )

        added_periods = await cursor.to_list(length=None)
        return {"periods": added_periods}

    except Exception as e:
        print(e)
        raise

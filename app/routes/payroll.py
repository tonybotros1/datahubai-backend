import copy
from datetime import datetime
from typing import Optional
from bson import ObjectId
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
    period_type: Optional[str] = None
    first_period_start_date: Optional[datetime] = None
    number_of_years: Optional[int] = None
    notes: Optional[str] = None


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
                }
            ],
            'as': 'details'
        }
    }, {
        '$set': {
            '_id': {
                '$toString': '$_id'
            },
            'company_id': {
                '$toString': '$company_id'
            }
        }
    }
]


async def get_payroll_details(payroll_id: ObjectId):
    try:
        new_pipeline = copy.deepcopy(payroll_details_pipeline)
        cursor = await payroll_collection.aggregate(new_pipeline)
        result = await cursor.to_list(None)
        return result[0] if result else None


    except Exception:
        raise


@router.post("/create_new_payroll")
async def create_new_payroll(payroll_details: PayrollModel, data: dict = Depends(security.get_current_user)):
    async with database.client.start_session() as session:
        try:
            await session.start_transaction()
            company_id = ObjectId(data.get("company_id"))
            payroll_details = payroll_details.model_dump(exclude_unset=True)

            name = payroll_details.get("name")
            years = payroll_details.get("number_of_years")
            start_date = payroll_details.get("first_period_start_date")
            period_type = payroll_details.get("period_type")

            now = security.now_utc()  # ✅ call once

            payroll_dict = {
                "company_id": company_id,
                "name": name,
                "period_type": period_type,
                "first_period_start_date": start_date,
                "number_of_years": years,
                "notes": payroll_details.get("notes"),
                "createdAt": now,
                "updatedAt": now,
            }
            result = await payroll_collection.insert_one(payroll_dict, session=session)
            if not result.inserted_id:
                raise HTTPException(status_code=500, detail="Failed to create Payroll")

            if not years:
                return {"message": "No periods created", "periods_created": []}

            # ✅ Define behavior once
            config = {
                "Yearly": {
                    "count": years,
                    "delta": lambda d: d + relativedelta(years=1) - timedelta(days=1),
                    "label": lambda d: f"{d.year} - {name}",
                },
                "Monthly": {
                    "count": years * 12,
                    "delta": lambda d: d + relativedelta(months=1) - timedelta(days=1),
                    "label": lambda d: f"({d.month}) {d.strftime('%B')} {d.strftime('%Y')} - {name}",
                },
                "Weekly": {
                    "count": years * 52,
                    "delta": lambda d: d + timedelta(weeks=1) - timedelta(days=1),
                    "label": lambda d: f"Week {d.isocalendar()[1]} {d.year} - {name}",
                },
            }

            if period_type not in config:
                raise HTTPException(status_code=400, detail="Invalid period type")

            cfg = config[period_type]

            periods = []
            current_start = start_date

            for _ in range(cfg["count"]):
                period_start = current_start
                period_end = cfg["delta"](period_start)

                periods.append({
                    "payroll_id": result.inserted_id,
                    "company_id": company_id,
                    "period": cfg["label"](period_start),
                    "start_date": period_start,
                    "end_date": period_end,
                    "status": True,
                    "createdAt": now,
                    "updatedAt": now,
                })

                current_start = period_end + timedelta(days=1)
            await payroll_period_details_collection.insert_many(periods, session=session)
            await session.commit_transaction()
            # return {
            #     "message": "Payroll created successfully",
            #     "periods_created": periods
            # }

        except Exception as e:
            await session.abort_transaction()
            print(e)
            raise HTTPException(status_code=500, detail=str(e))

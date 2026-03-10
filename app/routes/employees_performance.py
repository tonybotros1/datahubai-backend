from typing import Optional
from bson import ObjectId
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from app.core import security
from app.database import get_collection
from datetime import datetime, timezone

router = APIRouter()
all_technicians_collection = get_collection("all_technicians")
companies_collection = get_collection("companies")
time_sheets_collection = get_collection("time_sheets")
job_cards_collection = get_collection("job_cards")


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


class TimeSheetsSearch(BaseModel):
    year: Optional[int] = None
    month: Optional[int] = None
    all: Optional[bool] = False
    this_month: Optional[bool] = False
    this_year: Optional[bool] = False


#
# @router.post("/search_engine")
# async def search_engine(filter_time_sheets: TimeSheetsSearch, data: dict = Depends(security.get_current_user)):
#     try:
#         company_id = ObjectId(data.get("company_id"))
#         now = datetime.now(timezone.utc)
#
#         # Determine date range dynamically (for all other cases)
#         if filter_time_sheets.this_year:
#             start_date = datetime(now.year, 1, 1, tzinfo=timezone.utc)
#             end_date = datetime(now.year + 1, 1, 1, tzinfo=timezone.utc)
#         elif filter_time_sheets.this_month:
#             start_date = datetime(now.year, now.month, 1, tzinfo=timezone.utc)
#             end_date = datetime(now.year + (now.month // 12), (now.month % 12) + 1, 1, tzinfo=timezone.utc)
#         elif filter_time_sheets.year and filter_time_sheets.month:
#             start_date = datetime(filter_time_sheets.year, filter_time_sheets.month, 1, tzinfo=timezone.utc)
#             if filter_time_sheets.month == 12:
#                 end_date = datetime(filter_time_sheets.year + 1, 1, 1, tzinfo=timezone.utc)
#             else:
#                 end_date = datetime(filter_time_sheets.year, filter_time_sheets.month + 1, 1, tzinfo=timezone.utc)
#         elif filter_time_sheets.year:
#             start_date = datetime(filter_time_sheets.year, 1, 1, tzinfo=timezone.utc)
#             end_date = datetime(filter_time_sheets.year + 1, 1, 1, tzinfo=timezone.utc)
#         else:
#             # Default: this month
#             start_date = datetime(now.year, now.month, 1, tzinfo=timezone.utc)
#             end_date = datetime(now.year + (now.month // 12), (now.month % 12) + 1, 1, tzinfo=timezone.utc)
#
#         pipeline = [
#             {
#                 '$match': {
#                     'company_id': company_id
#                 }
#             },
#             {
#                 '$lookup': {
#                     'from': 'companies',
#                     'let': {
#                         'company_id': company_id
#                     },
#                     'pipeline': [
#                         {
#                             '$match': {
#                                 '$expr': {
#                                     '$eq': [
#                                         '$_id', '$$company_id'
#                                     ]
#                                 }
#                             }
#                         }, {
#                             '$project': {
#                                 'incentive_percentage': 1
#                             }
#                         }
#                     ],
#                     'as': 'company_details'
#                 }
#             }, {
#                 '$unwind': {
#                     'path': '$company_details',
#                     'preserveNullAndEmptyArrays': True
#                 }
#             },
#             {
#                 '$lookup': {
#                     'from': 'time_sheets',
#                     'let': {
#                         'from_date': start_date,
#                         'to_date': end_date,
#                         'employee_id': '$_id',
#                         'company_id': company_id
#                     },
#                     'pipeline': [
#                         {
#                             '$match': {
#                                 '$expr': {
#                                     '$and': [
#                                         {
#                                             '$eq': [
#                                                 '$company_id', '$$company_id'
#                                             ]
#                                         }, {
#                                             '$eq': [
#                                                 '$employee_id', '$$employee_id'
#                                             ]
#                                         }, {
#                                             '$gte': [
#                                                 '$end_date', '$$from_date'
#                                             ]
#                                         }, {
#                                             '$lt': [
#                                                 '$end_date', '$$to_date'
#                                             ]
#                                         }, {
#                                             '$ne': [
#                                                 '$end_date', None
#                                             ]
#                                         }
#                                     ]
#                                 }
#                             }
#                         }, {
#                             '$lookup': {
#                                 'from': 'job_cards',
#                                 'let': {
#                                     'job_id': '$job_id'
#                                 },
#                                 'pipeline': [
#                                     {
#                                         '$match': {
#                                             '$expr': {
#                                                 '$eq': [
#                                                     '$_id', '$$job_id'
#                                                 ]
#                                             }
#                                         }
#                                     }, {
#                                         '$project': {
#                                             'car_brand': 1,
#                                             'car_model': 1
#                                         }
#                                     }, {
#                                         '$lookup': {
#                                             'from': 'all_brands',
#                                             'let': {
#                                                 'brand_id': '$car_brand'
#                                             },
#                                             'pipeline': [
#                                                 {
#                                                     '$match': {
#                                                         '$expr': {
#                                                             '$eq': [
#                                                                 '$_id', '$$brand_id'
#                                                             ]
#                                                         }
#                                                     }
#                                                 }, {
#                                                     '$project': {
#                                                         'name': 1
#                                                     }
#                                                 }
#                                             ],
#                                             'as': 'brand_details'
#                                         }
#                                     }, {
#                                         '$unwind': {
#                                             'path': '$brand_details',
#                                             'preserveNullAndEmptyArrays': True
#                                         }
#                                     }, {
#                                         '$lookup': {
#                                             'from': 'all_brand_models',
#                                             'let': {
#                                                 'model_id': '$car_model'
#                                             },
#                                             'pipeline': [
#                                                 {
#                                                     '$match': {
#                                                         '$expr': {
#                                                             '$eq': [
#                                                                 '$_id', '$$model_id'
#                                                             ]
#                                                         }
#                                                     }
#                                                 }, {
#                                                     '$project': {
#                                                         'name': 1
#                                                     }
#                                                 }
#                                             ],
#                                             'as': 'model_details'
#                                         }
#                                     }, {
#                                         '$unwind': {
#                                             'path': '$model_details',
#                                             'preserveNullAndEmptyArrays': True
#                                         }
#                                     }
#                                 ],
#                                 'as': 'job_card'
#                             }
#                         }, {
#                             '$unwind': {
#                                 'path': '$job_card',
#                                 'preserveNullAndEmptyArrays': True
#                             }
#                         }
#                     ],
#                     'as': 'completed_sheets'
#                 }
#             }, {
#                 '$lookup': {
#                     'from': 'all_job_tasks',
#                     'let': {
#                         'task_ids': '$completed_sheets.task_id'
#                     },
#                     'pipeline': [
#                         {
#                             '$match': {
#                                 '$expr': {
#                                     '$in': [
#                                         '$_id', {
#                                             '$ifNull': [
#                                                 '$$task_ids', []
#                                             ]
#                                         }
#                                     ]
#                                 }
#                             }
#                         }, {
#                             '$project': {
#                                 '_id': 1,
#                                 'name_en': 1,
#                                 'name_ar': 1,
#                                 'points': {
#                                     '$toInt': {
#                                         '$ifNull': [
#                                             '$points', 0
#                                         ]
#                                     }
#                                 }
#                             }
#                         }
#                     ],
#                     'as': 'task_points'
#                 }
#             }, {
#                 '$lookup': {
#                     'from': 'job_cards',
#                     'let': {
#                         'start_date': start_date,
#                         'end_date': end_date,
#                         'company_id': company_id
#                     },
#                     'pipeline': [
#                         {
#                             '$match': {
#                                 '$expr': {
#                                     '$and': [
#                                         {
#                                             '$eq': [
#                                                 '$job_status_1', 'Posted'
#                                             ]
#                                         }, {
#                                             '$eq': [
#                                                 '$company_id', '$$company_id'
#                                             ]
#                                         }, {
#                                             '$gte': [
#                                                 '$job_date', '$$start_date'
#                                             ]
#                                         }, {
#                                             '$lt': [
#                                                 '$job_date', '$$end_date'
#                                             ]
#                                         }
#                                     ]
#                                 }
#                             }
#                         }, {
#                             '$project': {
#                                 '_id': 1
#                             }
#                         }
#                     ],
#                     'as': 'posted_jobs'
#                 }
#             }, {
#                 '$lookup': {
#                     'from': 'job_cards_invoice_items',
#                     'let': {
#                         'job_ids': '$posted_jobs._id'
#                     },
#                     'pipeline': [
#                         {
#                             '$match': {
#                                 '$expr': {
#                                     '$in': [
#                                         '$job_card_id', {
#                                             '$ifNull': [
#                                                 '$$job_ids', []
#                                             ]
#                                         }
#                                     ]
#                                 }
#                             }
#                         }, {
#                             '$project': {
#                                 'total': 1
#                             }
#                         }
#                     ],
#                     'as': 'invoice_items_details'
#                 }
#             }, {
#                 '$addFields': {
#                     'total_amount': {
#                         '$sum': {
#                             '$map': {
#                                 'input': '$invoice_items_details',
#                                 'as': 'item',
#                                 'in': {
#                                     '$ifNull': [
#                                         '$$item.total', 0
#                                     ]
#                                 }
#                             }
#                         }
#                     }
#                 }
#             }, {
#                 '$addFields': {
#                     'total_worked_millis': {
#                         '$sum': {
#                             '$map': {
#                                 'input': {
#                                     '$ifNull': [
#                                         '$completed_sheets', []
#                                     ]
#                                 },
#                                 'as': 'sheet',
#                                 'in': {
#                                     '$sum': {
#                                         '$map': {
#                                             'input': {
#                                                 '$ifNull': [
#                                                     '$$sheet.active_periods', []
#                                                 ]
#                                             },
#                                             'as': 'period',
#                                             'in': {
#                                                 '$subtract': [
#                                                     {
#                                                         '$toLong': '$$period.to'
#                                                     }, {
#                                                         '$toLong': '$$period.from'
#                                                     }
#                                                 ]
#                                             }
#                                         }
#                                     }
#                                 }
#                             }
#                         }
#                     },
#                     'total_tasks': {
#                         '$size': {
#                             '$ifNull': [
#                                 '$completed_sheets', []
#                             ]
#                         }
#                     },
#                     'points': {
#                         '$sum': '$task_points.points'
#                     }
#                 }
#             }, {
#                 '$addFields': {
#                     'completed_sheets_infos': {
#                         '$map': {
#                             'input': {
#                                 '$ifNull': [
#                                     '$completed_sheets', []
#                                 ]
#                             },
#                             'as': 'sheet',
#                             'in': {
#                                 '_id': '$$sheet._id',
#                                 'brand_name': '$$sheet.job_card.brand_details.name',
#                                 'model_name': '$$sheet.job_card.model_details.name',
#                                 'start_date': '$$sheet.start_date',
#                                 'end_date': '$$sheet.end_date',
#                                 'name_en': {
#                                     '$let': {
#                                         'vars': {
#                                             'matched_task': {
#                                                 '$first': {
#                                                     '$filter': {
#                                                         'input': '$task_points',
#                                                         'as': 'tp',
#                                                         'cond': {
#                                                             '$eq': [
#                                                                 '$$tp._id', '$$sheet.task_id'
#                                                             ]
#                                                         }
#                                                     }
#                                                 }
#                                             }
#                                         },
#                                         'in': '$$matched_task.name_en'
#                                     }
#                                 },
#                                 'name_ar': {
#                                     '$let': {
#                                         'vars': {
#                                             'matched_task': {
#                                                 '$first': {
#                                                     '$filter': {
#                                                         'input': '$task_points',
#                                                         'as': 'tp',
#                                                         'cond': {
#                                                             '$eq': [
#                                                                 '$$tp._id', '$$sheet.task_id'
#                                                             ]
#                                                         }
#                                                     }
#                                                 }
#                                             }
#                                         },
#                                         'in': '$$matched_task.name_ar'
#                                     }
#                                 },
#                                 'points': {
#                                     '$let': {
#                                         'vars': {
#                                             'matched_task': {
#                                                 '$first': {
#                                                     '$filter': {
#                                                         'input': '$task_points',
#                                                         'as': 'tp',
#                                                         'cond': {
#                                                             '$eq': [
#                                                                 '$$tp._id', '$$sheet.task_id'
#                                                             ]
#                                                         }
#                                                     }
#                                                 }
#                                             }
#                                         },
#                                         'in': '$$matched_task.points'
#                                     }
#                                 },
#                                 'minutes': {
#                                     '$floor': {
#                                         '$divide': [
#                                             {
#                                                 '$sum': {
#                                                     '$map': {
#                                                         'input': '$$sheet.active_periods',
#                                                         'as': 'period',
#                                                         'in': {
#                                                             '$divide': [
#                                                                 {
#                                                                     '$subtract': [
#                                                                         '$$period.to', '$$period.from'
#                                                                     ]
#                                                                 }, 1000 * 60
#                                                             ]
#                                                         }
#                                                     }
#                                                 }
#                                             }, 1
#                                         ]
#                                     }
#                                 },
#                                 'seconds': {
#                                     '$mod': [
#                                         {
#                                             '$floor': {
#                                                 '$divide': [
#                                                     {
#                                                         '$sum': {
#                                                             '$map': {
#                                                                 'input': '$$sheet.active_periods',
#                                                                 'as': 'period',
#                                                                 'in': {
#                                                                     '$divide': [
#                                                                         {
#                                                                             '$subtract': [
#                                                                                 '$$period.to', '$$period.from'
#                                                                             ]
#                                                                         }, 1000
#                                                                     ]
#                                                                 }
#                                                             }
#                                                         }
#                                                     }, 1
#                                                 ]
#                                             }
#                                         }, 60
#                                     ]
#                                 }
#                             }
#                         }
#                     }
#                 }
#             }, {
#                 '$lookup': {
#                     'from': 'time_sheets',
#                     'let': {
#                         'from_date': start_date,
#                         'to_date': end_date,
#                         'company_id': company_id
#                     },
#                     'pipeline': [
#                         {
#                             '$match': {
#                                 '$expr': {
#                                     '$and': [
#                                         {
#                                             '$eq': [
#                                                 '$company_id', '$$company_id'
#                                             ]
#                                         }, {
#                                             '$gte': [
#                                                 '$end_date', '$$from_date'
#                                             ]
#                                         }, {
#                                             '$lt': [
#                                                 '$end_date', '$$to_date'
#                                             ]
#                                         }, {
#                                             '$ne': [
#                                                 '$end_date', None
#                                             ]
#                                         }
#                                     ]
#                                 }
#                             }
#                         }, {
#                             '$lookup': {
#                                 'from': 'all_job_tasks',
#                                 'localField': 'task_id',
#                                 'foreignField': '_id',
#                                 'as': 'task'
#                             }
#                         }, {
#                             '$unwind': '$task'
#                         }, {
#                             '$group': {
#                                 '_id': None,
#                                 'total_points_all': {
#                                     '$sum': {
#                                         '$toInt': {
#                                             '$ifNull': [
#                                                 '$task.points', 0
#                                             ]
#                                         }
#                                     }
#                                 }
#                             }
#                         }
#                     ],
#                     'as': 'all_points_summary'
#                 }
#             }, {
#                 '$addFields': {
#                     'total_points_all': {
#                         '$ifNull': [
#                             {
#                                 '$arrayElemAt': [
#                                     '$all_points_summary.total_points_all', 0
#                                 ]
#                             }, 0
#                         ]
#                     }
#                 }
#             }, {
#                 '$addFields': {
#                     'AMT': {
#                         '$cond': [
#                             {
#                                 '$gt': [
#                                     '$total_points_all', 0
#                                 ]
#                             }, {
#                                 '$divide': [
#                                     {
#                                         '$multiply': [
#                                             '$points', "$company_details.incentive_percentage", '$total_amount'
#                                         ]
#                                     }, '$total_points_all'
#                                 ]
#                             }, 0
#                         ]
#                     }
#                 }
#             }, {
#                 '$project': {
#                     'completed_sheets_infos': 1,
#                     'name': 1,
#                     'total_tasks': 1,
#                     'points': 1,
#                     'total_amount': 1,
#                     'AMT': 1,
#                     'total_worked_hours': {
#                         '$floor': {
#                             '$divide': [
#                                 '$total_worked_millis', 1000 * 60 * 60
#                             ]
#                         }
#                     },
#                     'total_worked_minutes': {
#                         '$floor': {
#                             '$mod': [
#                                 {
#                                     '$divide': [
#                                         '$total_worked_millis', 1000 * 60
#                                     ]
#                                 }, 60
#                             ]
#                         }
#                     },
#                     'total_worked_seconds': {
#                         '$floor': {
#                             '$mod': [
#                                 {
#                                     '$divide': [
#                                         '$total_worked_millis', 1000
#                                     ]
#                                 }, 60
#                             ]
#                         }
#                     },
#                     'time_string': {
#                         '$concat': [
#                             {
#                                 '$toString': {
#                                     '$floor': {
#                                         '$divide': [
#                                             '$total_worked_millis', 1000 * 60 * 60
#                                         ]
#                                     }
#                                 }
#                             }, 'H : ', {
#                                 '$toString': {
#                                     '$floor': {
#                                         '$mod': [
#                                             {
#                                                 '$divide': [
#                                                     '$total_worked_millis', 1000 * 60
#                                                 ]
#                                             }, 60
#                                         ]
#                                     }
#                                 }
#                             }, 'M : ', {
#                                 '$toString': {
#                                     '$floor': {
#                                         '$mod': [
#                                             {
#                                                 '$divide': [
#                                                     '$total_worked_millis', 1000
#                                                 ]
#                                             }, 60
#                                         ]
#                                     }
#                                 }
#                             }, 'S'
#                         ]
#                     }
#                 }
#             }
#         ]
#
#         cursor = await all_technicians_collection.aggregate(pipeline)
#         results = await cursor.to_list(None)
#         return {"filtered_time_sheets": [serializer(r) for r in results]}
#
#
#     except HTTPException:
#         raise
#     except Exception as error:
#         raise HTTPException(status_code=500, detail=str(error))


#
# @router.post("/search_engine")
# async def search_engine(filter_time_sheets: TimeSheetsSearch, data: dict = Depends(security.get_current_user)):
#     try:
#         company_id = ObjectId(data.get("company_id"))
#         now = datetime.now(timezone.utc)
#
#         # Determine date range dynamically (for all other cases)
#         if filter_time_sheets.this_year:
#             start_date = datetime(now.year, 1, 1, tzinfo=timezone.utc)
#             end_date = datetime(now.year + 1, 1, 1, tzinfo=timezone.utc)
#         elif filter_time_sheets.this_month:
#             start_date = datetime(now.year, now.month, 1, tzinfo=timezone.utc)
#             end_date = datetime(now.year + (now.month // 12), (now.month % 12) + 1, 1, tzinfo=timezone.utc)
#         elif filter_time_sheets.year and filter_time_sheets.month:
#             start_date = datetime(filter_time_sheets.year, filter_time_sheets.month, 1, tzinfo=timezone.utc)
#             if filter_time_sheets.month == 12:
#                 end_date = datetime(filter_time_sheets.year + 1, 1, 1, tzinfo=timezone.utc)
#             else:
#                 end_date = datetime(filter_time_sheets.year, filter_time_sheets.month + 1, 1, tzinfo=timezone.utc)
#         elif filter_time_sheets.year:
#             start_date = datetime(filter_time_sheets.year, 1, 1, tzinfo=timezone.utc)
#             end_date = datetime(filter_time_sheets.year + 1, 1, 1, tzinfo=timezone.utc)
#         else:
#             # Default: this month
#             start_date = datetime(now.year, now.month, 1, tzinfo=timezone.utc)
#             end_date = datetime(now.year + (now.month // 12), (now.month % 12) + 1, 1, tzinfo=timezone.utc)
#
#         pipeline = [
#             {
#                 '$match': {
#                     'company_id': company_id
#                 }
#             },
#             {
#                 '$lookup': {
#                     'from': 'companies',
#                     'let': {
#                         'company_id': company_id
#                     },
#                     'pipeline': [
#                         {
#                             '$match': {
#                                 '$expr': {
#                                     '$eq': [
#                                         '$_id', '$$company_id'
#                                     ]
#                                 }
#                             }
#                         }, {
#                             '$project': {
#                                 'incentive_percentage': 1
#                             }
#                         }
#                     ],
#                     'as': 'company_details'
#                 }
#             }, {
#                 '$unwind': {
#                     'path': '$company_details',
#                     'preserveNullAndEmptyArrays': True
#                 }
#             },
#             {
#                 '$lookup': {
#                     'from': 'time_sheets',
#                     'let': {
#                         'from_date': start_date,
#                         'to_date': end_date,
#                         'employee_id': '$_id',
#                         'company_id': company_id
#                     },
#                     'pipeline': [
#                         {
#                             '$match': {
#                                 '$expr': {
#                                     '$and': [
#                                         {
#                                             '$eq': [
#                                                 '$company_id', '$$company_id'
#                                             ]
#                                         }, {
#                                             '$eq': [
#                                                 '$employee_id', '$$employee_id'
#                                             ]
#                                         }, {
#                                             '$gte': [
#                                                 '$end_date', '$$from_date'
#                                             ]
#                                         }, {
#                                             '$lt': [
#                                                 '$end_date', '$$to_date'
#                                             ]
#                                         }, {
#                                             '$ne': [
#                                                 '$end_date', None
#                                             ]
#                                         }
#                                     ]
#                                 }
#                             }
#                         }, {
#                             '$lookup': {
#                                 'from': 'job_cards',
#                                 'let': {
#                                     'job_id': '$job_id'
#                                 },
#                                 'pipeline': [
#                                     {
#                                         '$match': {
#                                             '$expr': {
#                                                 '$eq': [
#                                                     '$_id', '$$job_id'
#                                                 ]
#                                             }
#                                         }
#                                     }, {
#                                         '$project': {
#                                             'car_brand': 1,
#                                             'car_model': 1
#                                         }
#                                     }, {
#                                         '$lookup': {
#                                             'from': 'all_brands',
#                                             'let': {
#                                                 'brand_id': '$car_brand'
#                                             },
#                                             'pipeline': [
#                                                 {
#                                                     '$match': {
#                                                         '$expr': {
#                                                             '$eq': [
#                                                                 '$_id', '$$brand_id'
#                                                             ]
#                                                         }
#                                                     }
#                                                 }, {
#                                                     '$project': {
#                                                         'name': 1
#                                                     }
#                                                 }
#                                             ],
#                                             'as': 'brand_details'
#                                         }
#                                     }, {
#                                         '$unwind': {
#                                             'path': '$brand_details',
#                                             'preserveNullAndEmptyArrays': True
#                                         }
#                                     }, {
#                                         '$lookup': {
#                                             'from': 'all_brand_models',
#                                             'let': {
#                                                 'model_id': '$car_model'
#                                             },
#                                             'pipeline': [
#                                                 {
#                                                     '$match': {
#                                                         '$expr': {
#                                                             '$eq': [
#                                                                 '$_id', '$$model_id'
#                                                             ]
#                                                         }
#                                                     }
#                                                 }, {
#                                                     '$project': {
#                                                         'name': 1
#                                                     }
#                                                 }
#                                             ],
#                                             'as': 'model_details'
#                                         }
#                                     }, {
#                                         '$unwind': {
#                                             'path': '$model_details',
#                                             'preserveNullAndEmptyArrays': True
#                                         }
#                                     }
#                                 ],
#                                 'as': 'job_card'
#                             }
#                         }, {
#                             '$unwind': {
#                                 'path': '$job_card',
#                                 'preserveNullAndEmptyArrays': True
#                             }
#                         }
#                     ],
#                     'as': 'completed_sheets'
#                 }
#             }, {
#                 '$lookup': {
#                     'from': 'all_job_tasks',
#                     'let': {
#                         'task_ids': '$completed_sheets.task_id'
#                     },
#                     'pipeline': [
#                         {
#                             '$match': {
#                                 '$expr': {
#                                     '$in': [
#                                         '$_id', {
#                                             '$ifNull': [
#                                                 '$$task_ids', []
#                                             ]
#                                         }
#                                     ]
#                                 }
#                             }
#                         }, {
#                             '$project': {
#                                 '_id': 1,
#                                 'name_en': 1,
#                                 'name_ar': 1,
#                                 'points': {
#                                     '$toInt': {
#                                         '$ifNull': [
#                                             '$points', 0
#                                         ]
#                                     }
#                                 }
#                             }
#                         }
#                     ],
#                     'as': 'task_points'
#                 }
#             }, {
#                 '$lookup': {
#                     'from': 'job_cards',
#                     'let': {
#                         'start_date': start_date,
#                         'end_date': end_date,
#                         'company_id': company_id
#                     },
#                     'pipeline': [
#                         {
#                             '$match': {
#                                 '$expr': {
#                                     '$and': [
#                                         {
#                                             '$eq': [
#                                                 '$job_status_1', 'Posted'
#                                             ]
#                                         }, {
#                                             '$eq': [
#                                                 '$company_id', '$$company_id'
#                                             ]
#                                         }, {
#                                             '$gte': [
#                                                 '$job_date', '$$start_date'
#                                             ]
#                                         }, {
#                                             '$lt': [
#                                                 '$job_date', '$$end_date'
#                                             ]
#                                         }
#                                     ]
#                                 }
#                             }
#                         }, {
#                             '$project': {
#                                 '_id': 1
#                             }
#                         }
#                     ],
#                     'as': 'posted_jobs'
#                 }
#             }, {
#                 '$lookup': {
#                     'from': 'job_cards_invoice_items',
#                     'let': {
#                         'job_ids': '$posted_jobs._id'
#                     },
#                     'pipeline': [
#                         {
#                             '$match': {
#                                 '$expr': {
#                                     '$in': [
#                                         '$job_card_id', {
#                                             '$ifNull': [
#                                                 '$$job_ids', []
#                                             ]
#                                         }
#                                     ]
#                                 }
#                             }
#                         }, {
#                             '$project': {
#                                 'total': 1
#                             }
#                         }
#                     ],
#                     'as': 'invoice_items_details'
#                 }
#             }, {
#                 '$addFields': {
#                     'total_amount': {
#                         '$sum': {
#                             '$map': {
#                                 'input': '$invoice_items_details',
#                                 'as': 'item',
#                                 'in': {
#                                     '$ifNull': [
#                                         '$$item.total', 0
#                                     ]
#                                 }
#                             }
#                         }
#                     }
#                 }
#             }, {
#                 '$addFields': {
#                     'total_worked_millis': {
#                         '$sum': {
#                             '$map': {
#                                 'input': {
#                                     '$ifNull': [
#                                         '$completed_sheets', []
#                                     ]
#                                 },
#                                 'as': 'sheet',
#                                 'in': {
#                                     '$sum': {
#                                         '$map': {
#                                             'input': {
#                                                 '$ifNull': [
#                                                     '$$sheet.active_periods', []
#                                                 ]
#                                             },
#                                             'as': 'period',
#                                             'in': {
#                                                 '$subtract': [
#                                                     {
#                                                         '$toLong': '$$period.to'
#                                                     }, {
#                                                         '$toLong': '$$period.from'
#                                                     }
#                                                 ]
#                                             }
#                                         }
#                                     }
#                                 }
#                             }
#                         }
#                     },
#                     'total_tasks': {
#                         '$size': {
#                             '$ifNull': [
#                                 '$completed_sheets', []
#                             ]
#                         }
#                     },
#                     'points': {
#                         '$sum': '$task_points.points'
#                     }
#                 }
#             }, {
#                 '$addFields': {
#                     'completed_sheets_infos': {
#                         '$map': {
#                             'input': {
#                                 '$ifNull': [
#                                     '$completed_sheets', []
#                                 ]
#                             },
#                             'as': 'sheet',
#                             'in': {
#                                 '_id': '$$sheet._id',
#                                 'brand_name': '$$sheet.job_card.brand_details.name',
#                                 'model_name': '$$sheet.job_card.model_details.name',
#                                 'start_date': '$$sheet.start_date',
#                                 'end_date': '$$sheet.end_date',
#                                 'name_en': {
#                                     '$let': {
#                                         'vars': {
#                                             'matched_task': {
#                                                 '$first': {
#                                                     '$filter': {
#                                                         'input': '$task_points',
#                                                         'as': 'tp',
#                                                         'cond': {
#                                                             '$eq': [
#                                                                 '$$tp._id', '$$sheet.task_id'
#                                                             ]
#                                                         }
#                                                     }
#                                                 }
#                                             }
#                                         },
#                                         'in': '$$matched_task.name_en'
#                                     }
#                                 },
#                                 'name_ar': {
#                                     '$let': {
#                                         'vars': {
#                                             'matched_task': {
#                                                 '$first': {
#                                                     '$filter': {
#                                                         'input': '$task_points',
#                                                         'as': 'tp',
#                                                         'cond': {
#                                                             '$eq': [
#                                                                 '$$tp._id', '$$sheet.task_id'
#                                                             ]
#                                                         }
#                                                     }
#                                                 }
#                                             }
#                                         },
#                                         'in': '$$matched_task.name_ar'
#                                     }
#                                 },
#                                 'points': {
#                                     '$let': {
#                                         'vars': {
#                                             'matched_task': {
#                                                 '$first': {
#                                                     '$filter': {
#                                                         'input': '$task_points',
#                                                         'as': 'tp',
#                                                         'cond': {
#                                                             '$eq': [
#                                                                 '$$tp._id', '$$sheet.task_id'
#                                                             ]
#                                                         }
#                                                     }
#                                                 }
#                                             }
#                                         },
#                                         'in': '$$matched_task.points'
#                                     }
#                                 },
#                                 'minutes': {
#                                     '$floor': {
#                                         '$divide': [
#                                             {
#                                                 '$sum': {
#                                                     '$map': {
#                                                         'input': '$$sheet.active_periods',
#                                                         'as': 'period',
#                                                         'in': {
#                                                             '$divide': [
#                                                                 {
#                                                                     '$subtract': [
#                                                                         '$$period.to', '$$period.from'
#                                                                     ]
#                                                                 }, 1000 * 60
#                                                             ]
#                                                         }
#                                                     }
#                                                 }
#                                             }, 1
#                                         ]
#                                     }
#                                 },
#                                 'seconds': {
#                                     '$mod': [
#                                         {
#                                             '$floor': {
#                                                 '$divide': [
#                                                     {
#                                                         '$sum': {
#                                                             '$map': {
#                                                                 'input': '$$sheet.active_periods',
#                                                                 'as': 'period',
#                                                                 'in': {
#                                                                     '$divide': [
#                                                                         {
#                                                                             '$subtract': [
#                                                                                 '$$period.to', '$$period.from'
#                                                                             ]
#                                                                         }, 1000
#                                                                     ]
#                                                                 }
#                                                             }
#                                                         }
#                                                     }, 1
#                                                 ]
#                                             }
#                                         }, 60
#                                     ]
#                                 }
#                             }
#                         }
#                     }
#                 }
#             }, {
#                 '$lookup': {
#                     'from': 'time_sheets',
#                     'let': {
#                         'from_date': start_date,
#                         'to_date': end_date,
#                         'company_id': company_id
#                     },
#                     'pipeline': [
#                         {
#                             '$match': {
#                                 '$expr': {
#                                     '$and': [
#                                         {
#                                             '$eq': [
#                                                 '$company_id', '$$company_id'
#                                             ]
#                                         }, {
#                                             '$gte': [
#                                                 '$end_date', '$$from_date'
#                                             ]
#                                         }, {
#                                             '$lt': [
#                                                 '$end_date', '$$to_date'
#                                             ]
#                                         }, {
#                                             '$ne': [
#                                                 '$end_date', None
#                                             ]
#                                         }
#                                     ]
#                                 }
#                             }
#                         }, {
#                             '$lookup': {
#                                 'from': 'all_job_tasks',
#                                 'localField': 'task_id',
#                                 'foreignField': '_id',
#                                 'as': 'task'
#                             }
#                         }, {
#                             '$unwind': '$task'
#                         }, {
#                             '$group': {
#                                 '_id': None,
#                                 'total_points_all': {
#                                     '$sum': {
#                                         '$toInt': {
#                                             '$ifNull': [
#                                                 '$task.points', 0
#                                             ]
#                                         }
#                                     }
#                                 }
#                             }
#                         }
#                     ],
#                     'as': 'all_points_summary'
#                 }
#             }, {
#                 '$addFields': {
#                     'total_points_all': {
#                         '$ifNull': [
#                             {
#                                 '$arrayElemAt': [
#                                     '$all_points_summary.total_points_all', 0
#                                 ]
#                             }, 0
#                         ]
#                     }
#                 }
#             }, {
#                 '$addFields': {
#                     'AMT': {
#                         '$cond': [
#                             {
#                                 '$gt': [
#                                     '$total_points_all', 0
#                                 ]
#                             }, {
#                                 '$divide': [
#                                     {
#                                         '$multiply': [
#                                             '$points', "$company_details.incentive_percentage", '$total_amount'
#                                         ]
#                                     }, '$total_points_all'
#                                 ]
#                             }, 0
#                         ]
#                     }
#                 }
#             }, {
#                 '$project': {
#                     'completed_sheets_infos': 1,
#                     'name': 1,
#                     'total_tasks': 1,
#                     'points': 1,
#                     'total_amount': 1,
#                     'AMT': 1,
#                     'total_worked_hours': {
#                         '$floor': {
#                             '$divide': [
#                                 '$total_worked_millis', 1000 * 60 * 60
#                             ]
#                         }
#                     },
#                     'total_worked_minutes': {
#                         '$floor': {
#                             '$mod': [
#                                 {
#                                     '$divide': [
#                                         '$total_worked_millis', 1000 * 60
#                                     ]
#                                 }, 60
#                             ]
#                         }
#                     },
#                     'total_worked_seconds': {
#                         '$floor': {
#                             '$mod': [
#                                 {
#                                     '$divide': [
#                                         '$total_worked_millis', 1000
#                                     ]
#                                 }, 60
#                             ]
#                         }
#                     },
#                     'time_string': {
#                         '$concat': [
#                             {
#                                 '$toString': {
#                                     '$floor': {
#                                         '$divide': [
#                                             '$total_worked_millis', 1000 * 60 * 60
#                                         ]
#                                     }
#                                 }
#                             }, 'H : ', {
#                                 '$toString': {
#                                     '$floor': {
#                                         '$mod': [
#                                             {
#                                                 '$divide': [
#                                                     '$total_worked_millis', 1000 * 60
#                                                 ]
#                                             }, 60
#                                         ]
#                                     }
#                                 }
#                             }, 'M : ', {
#                                 '$toString': {
#                                     '$floor': {
#                                         '$mod': [
#                                             {
#                                                 '$divide': [
#                                                     '$total_worked_millis', 1000
#                                                 ]
#                                             }, 60
#                                         ]
#                                     }
#                                 }
#                             }, 'S'
#                         ]
#                     }
#                 }
#             }
#         ]
#
#         cursor = await all_technicians_collection.aggregate(pipeline)
#         results = await cursor.to_list(None)
#         return {"filtered_time_sheets": [serializer(r) for r in results]}
#
#
#     except HTTPException:
#         raise
#     except Exception as error:
#         raise HTTPException(status_code=500, detail=str(error))


import asyncio
from datetime import datetime, timezone
from bson import ObjectId
from fastapi import Depends, HTTPException


@router.post("/search_engine")
async def search_engine(
        filter_time_sheets: TimeSheetsSearch,
        data: dict = Depends(security.get_current_user)
):
    try:
        company_id_raw = data.get("company_id")
        company_id = company_id_raw if isinstance(company_id_raw, ObjectId) else ObjectId(company_id_raw)
        now = datetime.now(timezone.utc)

        # Optional: if your request model has this field, it will enforce employee list membership.
        technicians_list_id = None
        if hasattr(filter_time_sheets, "technicians_list_id") and filter_time_sheets.technicians_list_id:
            technicians_list_id = (
                filter_time_sheets.technicians_list_id
                if isinstance(filter_time_sheets.technicians_list_id, ObjectId)
                else ObjectId(filter_time_sheets.technicians_list_id)
            )

        # Date range
        if filter_time_sheets.this_year:
            start_date = datetime(now.year, 1, 1, tzinfo=timezone.utc)
            end_date = datetime(now.year + 1, 1, 1, tzinfo=timezone.utc)
        elif filter_time_sheets.this_month:
            start_date = datetime(now.year, now.month, 1, tzinfo=timezone.utc)
            end_date = datetime(now.year + (now.month // 12), (now.month % 12) + 1, 1, tzinfo=timezone.utc)
        elif filter_time_sheets.year and filter_time_sheets.month:
            start_date = datetime(filter_time_sheets.year, filter_time_sheets.month, 1, tzinfo=timezone.utc)
            if filter_time_sheets.month == 12:
                end_date = datetime(filter_time_sheets.year + 1, 1, 1, tzinfo=timezone.utc)
            else:
                end_date = datetime(filter_time_sheets.year, filter_time_sheets.month + 1, 1, tzinfo=timezone.utc)
        elif filter_time_sheets.year:
            start_date = datetime(filter_time_sheets.year, 1, 1, tzinfo=timezone.utc)
            end_date = datetime(filter_time_sheets.year + 1, 1, 1, tzinfo=timezone.utc)
        else:
            start_date = datetime(now.year, now.month, 1, tzinfo=timezone.utc)
            end_date = datetime(now.year + (now.month // 12), (now.month % 12) + 1, 1, tzinfo=timezone.utc)

        # 1) Company incentive %
        company_doc = await companies_collection.find_one(
            {"_id": company_id},
            {"incentive_percentage": 1}
        )
        incentive_percentage = float((company_doc or {}).get("incentive_percentage", 0) or 0)

        base_ts_match = {
            "company_id": company_id,
            "end_date": {"$gte": start_date, "$lt": end_date, "$ne": None}
        }

        # 2) Total points for all matched time sheets (global denominator)
        total_points_pipeline = [
            {"$match": base_ts_match},
            {
                "$lookup": {
                    "from": "all_job_tasks",
                    "localField": "task_id",
                    "foreignField": "_id",
                    "as": "task"
                }
            },
            {"$unwind": {"path": "$task", "preserveNullAndEmptyArrays": True}},
            {
                "$group": {
                    "_id": None,
                    "total_points_all": {
                        "$sum": {
                            "$convert": {
                                "input": "$task.points",
                                "to": "double",
                                "onError": 0,
                                "onNull": 0
                            }
                        }
                    }
                }
            }
        ]

        # 3) Total posted jobs amount for date range (global numerator part)
        total_amount_pipeline = [
            {
                "$match": {
                    "company_id": company_id,
                    "job_status_1": "Posted",
                    "job_date": {"$gte": start_date, "$lt": end_date}
                }
            },
            {
                "$lookup": {
                    "from": "job_cards_invoice_items",
                    "localField": "_id",
                    "foreignField": "job_card_id",
                    "as": "invoice_items"
                }
            },
            {
                "$project": {
                    "job_total": {
                        "$sum": {
                            "$map": {
                                "input": {"$ifNull": ["$invoice_items", []]},
                                "as": "it",
                                "in": {
                                    "$convert": {
                                        "input": "$$it.total",
                                        "to": "double",
                                        "onError": 0,
                                        "onNull": 0
                                    }
                                }
                            }
                        }
                    }
                }
            },
            {
                "$group": {
                    "_id": None,
                    "total_amount": {"$sum": "$job_total"}
                }
            }
        ]

        # employee lookup match (supports ObjectId/string mismatch)
        employee_match_and = [
            {"$eq": ["$company_id", "$$cid"]},
            {
                "$or": [
                    {"$eq": ["$_id", "$$eid_raw"]},
                    {"$eq": [{"$toString": "$_id"}, {"$toString": "$$eid_raw"}]}
                ]
            }
        ]
        if technicians_list_id is not None:
            employee_match_and.append({"$eq": ["$list_id", "$$lid"]})

        # 4) Employee aggregation
        employees_pipeline = [
            {"$match": {**base_ts_match, "employee_id": {"$ne": None}}},
            {
                "$lookup": {
                    "from": "all_job_tasks",
                    "localField": "task_id",
                    "foreignField": "_id",
                    "as": "task"
                }
            },
            {"$unwind": {"path": "$task", "preserveNullAndEmptyArrays": True}},
            {
                "$lookup": {
                    "from": "job_cards",
                    "localField": "job_id",
                    "foreignField": "_id",
                    "as": "job_card"
                }
            },
            {"$unwind": {"path": "$job_card", "preserveNullAndEmptyArrays": True}},
            {
                "$lookup": {
                    "from": "all_brands",
                    "localField": "job_card.car_brand",
                    "foreignField": "_id",
                    "as": "brand_details"
                }
            },
            {"$unwind": {"path": "$brand_details", "preserveNullAndEmptyArrays": True}},
            {
                "$lookup": {
                    "from": "all_brand_models",
                    "localField": "job_card.car_model",
                    "foreignField": "_id",
                    "as": "model_details"
                }
            },
            {"$unwind": {"path": "$model_details", "preserveNullAndEmptyArrays": True}},
            {
                "$addFields": {
                    "task_points": {
                        "$convert": {
                            "input": "$task.points",
                            "to": "double",
                            "onError": 0,
                            "onNull": 0
                        }
                    },
                    "sheet_millis": {
                        "$sum": {
                            "$map": {
                                "input": {"$ifNull": ["$active_periods", []]},
                                "as": "period",
                                "in": {
                                    "$let": {
                                        "vars": {
                                            "from_ms": {
                                                "$convert": {
                                                    "input": "$$period.from",
                                                    "to": "long",
                                                    "onError": 0,
                                                    "onNull": 0
                                                }
                                            },
                                            "to_ms": {
                                                "$convert": {
                                                    "input": "$$period.to",
                                                    "to": "long",
                                                    "onError": 0,
                                                    "onNull": 0
                                                }
                                            }
                                        },
                                        "in": {"$max": [0, {"$subtract": ["$$to_ms", "$$from_ms"]}]}
                                    }
                                }
                            }
                        }
                    }
                }
            },
            {
                "$group": {
                    "_id": "$employee_id",
                    "points": {"$sum": "$task_points"},
                    "total_tasks": {"$sum": 1},
                    "total_worked_millis": {"$sum": "$sheet_millis"},
                    "employee_name_from_sheet": {"$first": "$employee_name"},
                    "completed_sheets_infos": {
                        "$push": {
                            "_id": "$_id",
                            "brand_name": "$brand_details.name",
                            "model_name": "$model_details.name",
                            "start_date": "$start_date",
                            "end_date": "$end_date",
                            "name_en": "$task.name_en",
                            "name_ar": "$task.name_ar",
                            "points": "$task_points",
                            "minutes": {"$floor": {"$divide": ["$sheet_millis", 1000 * 60]}},
                            "seconds": {
                                "$mod": [
                                    {"$floor": {"$divide": ["$sheet_millis", 1000]}},
                                    60
                                ]
                            }
                        }
                    }
                }
            },
            {
                "$lookup": {
                    "from": "list_values",
                    "let": {
                        "eid_raw": "$_id",
                        "cid": company_id,
                        "lid": technicians_list_id
                    },
                    "pipeline": [
                        {
                            "$match": {
                                "$expr": {
                                    "$and": employee_match_and
                                }
                            }
                        },
                        {
                            "$project": {
                                "name": 1,
                                "name_en": 1,
                                "name_ar": 1,
                                "value": 1
                            }
                        }
                    ],
                    "as": "employee"
                }
            },
            {"$unwind": {"path": "$employee", "preserveNullAndEmptyArrays": True}},
            {
                "$addFields": {
                    "name": {
                        "$ifNull": [
                            "$employee.name",
                            "$employee.name_en",
                            "$employee.value.name",
                            "$employee.value.name_en",
                            "$employee_name_from_sheet",
                            "Unknown"
                        ]
                    }
                }
            },
            {
                "$project": {
                    "_id": 1,  # employee_id
                    "name": 1,
                    "completed_sheets_infos": 1,
                    "total_tasks": 1,
                    "points": 1,
                    "total_worked_millis": 1,
                    "total_worked_hours": {
                        "$floor": {
                            "$divide": ["$total_worked_millis", 1000 * 60 * 60]
                        }
                    },
                    "total_worked_minutes": {
                        "$floor": {
                            "$mod": [
                                {"$divide": ["$total_worked_millis", 1000 * 60]},
                                60
                            ]
                        }
                    },
                    "total_worked_seconds": {
                        "$floor": {
                            "$mod": [
                                {"$divide": ["$total_worked_millis", 1000]},
                                60
                            ]
                        }
                    },
                    "time_string": {
                        "$concat": [
                            {
                                "$toString": {
                                    "$floor": {"$divide": ["$total_worked_millis", 1000 * 60 * 60]}
                                }
                            },
                            "H : ",
                            {
                                "$toString": {
                                    "$floor": {
                                        "$mod": [
                                            {"$divide": ["$total_worked_millis", 1000 * 60]},
                                            60
                                        ]
                                    }
                                }
                            },
                            "M : ",
                            {
                                "$toString": {
                                    "$floor": {
                                        "$mod": [
                                            {"$divide": ["$total_worked_millis", 1000]},
                                            60
                                        ]
                                    }
                                }
                            },
                            "S"
                        ]
                    }
                }
            },
            {"$sort": {"points": -1, "name": 1}}
        ]

        # Run independent queries in parallel
        points_cursor = await time_sheets_collection.aggregate(total_points_pipeline)
        amount_cursor = await job_cards_collection.aggregate(total_amount_pipeline)
        employees_cursor = await time_sheets_collection.aggregate(employees_pipeline)

        points_docs, amount_docs, employee_rows = await asyncio.gather(
            points_cursor.to_list(length=1),
            amount_cursor.to_list(length=1),
            employees_cursor.to_list(length=None)
        )

        total_points_all = float(points_docs[0]["total_points_all"]) if points_docs else 0.0
        total_amount = float(amount_docs[0]["total_amount"]) if amount_docs else 0.0

        # Final AMT calc per employee
        for row in employee_rows:
            row_points = float(row.get("points", 0) or 0)
            row["total_amount"] = total_amount
            row["AMT"] = (
                (row_points * incentive_percentage * total_amount) / total_points_all
                if total_points_all > 0
                else 0
            )

        return {"filtered_time_sheets": [serializer(r) for r in employee_rows]}

    except HTTPException:
        raise
    except Exception as error:
        raise HTTPException(status_code=500, detail=str(error))

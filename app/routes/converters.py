import copy
from typing import Optional, Any
from bson import ObjectId
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from app.core import security
from app.database import get_collection
from datetime import datetime, timezone, timedelta
from app.routes.counters import create_custom_counter

router = APIRouter()
converters_collection = get_collection("converters")


class Converter(BaseModel):
    date: Optional[datetime] = None
    name: Optional[str] = None
    description: Optional[str] = None
    status: Optional[str] = None


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


class SearchConvertersModel(BaseModel):
    converter_number: Optional[str] = None
    converter_name: Optional[str] = None
    description: Optional[str] = None
    status: Optional[str] = None
    from_date: Optional[datetime] = None
    to_date: Optional[datetime] = None
    all: Optional[bool] = False
    today: Optional[bool] = False
    this_month: Optional[bool] = False
    this_year: Optional[bool] = False


converter_pipeline: list[dict[str, Any]] = [
    {
        '$lookup': {
            'from': 'issuing',
            'let': {
                'converter_id': '$_id'
            },
            'pipeline': [
                {
                    '$match': {
                        '$expr': {
                            '$and': [
                                {
                                    '$eq': [
                                        '$converter_id', '$$converter_id'
                                    ]
                                }, {
                                    '$eq': [
                                        '$status', 'Posted'
                                    ]
                                }
                            ]
                        }
                    }
                }, {
                    '$lookup': {
                        'from': 'issuing_items_details',
                        'let': {
                            'issue_id': '$_id'
                        },
                        'pipeline': [
                            {
                                '$match': {
                                    '$expr': {
                                        '$eq': [
                                            '$issue_id', '$$issue_id'
                                        ]
                                    }
                                }
                            }, {
                                '$lookup': {
                                    'from': 'inventory_items',
                                    'localField': 'inventory_item_id',
                                    'foreignField': '_id',
                                    'as': 'inventory_details'
                                }
                            }, {
                                '$unwind': {
                                    'path': '$inventory_details',
                                    'preserveNullAndEmptyArrays': True
                                }
                            }, {
                                '$addFields': {
                                    'item_code': {
                                        '$ifNull': [
                                            '$inventory_details.code', None
                                        ]
                                    },
                                    'item_name': {
                                        '$ifNull': [
                                            '$inventory_details.name', None
                                        ]
                                    }
                                }
                            }, {
                                '$project': {
                                    'inventory_details': 0
                                }
                            }
                        ],
                        'as': 'items_details'
                    }
                }, {
                    '$unwind': '$items_details'
                }, {
                    '$addFields': {
                        'item_name': {
                            '$ifNull': [
                                '$items_details.item_name', None
                            ]
                        },
                        'item_code': {
                            '$ifNull': [
                                '$items_details.item_code', None
                            ]
                        },
                        'quantity': {
                            '$ifNull': [
                                '$items_details.quantity', 0
                            ]
                        },
                        'price': {
                            '$ifNull': [
                                '$items_details.price', 0
                            ]
                        }
                    }
                }, {
                    '$addFields': {
                        'total': {
                            '$multiply': [
                                '$quantity', '$price'
                            ]
                        }
                    }
                }, {
                    '$project': {
                        'items_details': 0,
                        'issue_type': 0,
                        'job_card_id': 0,
                        'converter_id': 0,
                        'received_by': 0,
                        'note': 0,
                        'status': 0,
                        'createdAt': 0,
                        'updatedAt': 0
                    }
                }
            ],
            'as': 'issues'
        }
    }
]


async def get_converter_details(converter_id: ObjectId):
    new_pipeline = copy.deepcopy(converter_pipeline)
    new_pipeline.insert(0, {
        "$match": {
            '_id': converter_id
        }
    })
    cursor = await converters_collection.aggregate(new_pipeline)
    result = await cursor.next()
    return result


@router.post("/add_new_converter")
async def add_new_converter(converter: Converter, data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        converter = converter.model_dump(exclude_unset=True)
        new_converter_counter = await create_custom_counter("CN", "C", description='Converter Number', data=data)

        converter['company_id'] = company_id
        converter['status'] = 'New'
        converter['converter_number'] = new_converter_counter['final_counter'] if new_converter_counter[
            'success'] else None
        converter['createdAt'] = security.now_utc()
        converter['updatedAt'] = security.now_utc()

        result = await converters_collection.insert_one(converter)
        if not result.inserted_id:
            raise HTTPException(status_code=500, detail="Failed to insert converter")

        return {"converter_id": str(result.inserted_id), "converter_number": new_converter_counter['final_counter']}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.patch("/update_converter/{converter_id}")
async def update_converter(converter_id: str, converter: Converter, _: dict = Depends(security.get_current_user)):
    try:
        converter_id = ObjectId(converter_id)
        converter = converter.model_dump(exclude_unset=True)
        converter['updatedAt'] = security.now_utc()
        result = await converters_collection.update_one({"_id": converter_id}, {"$set": converter})
        if result.matched_count == 0:
            raise HTTPException(status_code=500, detail="Failed to update converter")

        updated = await get_converter_details(converter_id)
        print(updated)
        return {"updated_converter": serializer(updated)}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/delete_converter/{converter_id}")
async def delete_converter(converter_id: str, _: dict = Depends(security.get_current_user)):
    try:
        try:
            obj_id = ObjectId(converter_id)
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid converter_id format")

        # Attempt deletion only if status == "New"
        result = await converters_collection.delete_one({
            "_id": obj_id,
            "status": "New"
        })
        if result.deleted_count == 0:
            raise HTTPException(status_code=404, detail="Converter not found or cannot be deleted")
        return {"converter_id": str(converter_id)}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/get_converter_status/{converter_id}")
async def get_converter_status(converter_id: str, _: dict = Depends(security.get_current_user)):
    try:
        if not ObjectId.is_valid(converter_id):
            raise HTTPException(status_code=400, detail="Invalid job_id format")

        converter_id = ObjectId(converter_id)

        result = await converters_collection.find_one(
            {"_id": converter_id},
            {"_id": 0, "status": 1}
        )

        if not result:
            raise HTTPException(status_code=404, detail="Converter not found")

        return {"status": "success", "data": result}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")


@router.get("/get_all_converter")
async def get_all_converters(data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        results = await converters_collection.find({"company_id": company_id}).to_list(None)
        return {"converters": [serializer(r) for r in results]}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/search_engine_for_converters")
async def search_engine_for_converters(
        filter_converters: SearchConvertersModel,
        data: dict = Depends(security.get_current_user),
):
    try:
        company_id = data.get("company_id")
        if not company_id:
            raise HTTPException(status_code=400, detail="Company ID missing")

        company_id = ObjectId(company_id)
        search_pipeline = copy.deepcopy(converter_pipeline)

        match_stage = {}
        if company_id:
            match_stage['company_id'] = company_id
        if filter_converters.converter_number:
            match_stage["converter_number"] = {
                "$regex": filter_converters.converter_number, "$options": "i"
            }
        if filter_converters.converter_name:
            match_stage["name"] = {
                "$regex": filter_converters.converter_name, "$options": "i"
            }
        if filter_converters.description:
            match_stage["description"] = {
                "$regex": filter_converters.description, "$options": "i"
            }
        if filter_converters.status:
            match_stage["status"] = filter_converters.status

        # 2️⃣ Handle date filters
        now = datetime.now(timezone.utc)
        date_field = "date"
        date_filter = {}

        if filter_converters.today:
            start = datetime(now.year, now.month, now.day, tzinfo=timezone.utc)
            end = start + timedelta(days=1)
            date_filter[date_field] = {"$gte": start, "$lt": end}

        elif filter_converters.this_month:
            start = datetime(now.year, now.month, 1, tzinfo=timezone.utc)
            if now.month == 12:
                end = datetime(now.year + 1, 1, 1, tzinfo=timezone.utc)
            else:
                end = datetime(now.year, now.month + 1, 1, tzinfo=timezone.utc)
            date_filter[date_field] = {"$gte": start, "$lt": end}

        elif filter_converters.this_year:
            start = datetime(now.year, 1, 1, tzinfo=timezone.utc)
            end = datetime(now.year + 1, 1, 1, tzinfo=timezone.utc)
            date_filter[date_field] = {"$gte": start, "$lt": end}

        elif filter_converters.from_date or filter_converters.to_date:
            date_filter[date_field] = {}
            if filter_converters.from_date:
                date_filter[date_field]["$gte"] = filter_converters.from_date
            if filter_converters.to_date:
                date_filter[date_field]["$lte"] = filter_converters.to_date

        # Merge both filters into one $match
        if date_filter:
            match_stage.update(date_filter)

        search_pipeline.insert(0, {"$match": match_stage})

        # 3️⃣ Add computed field
        search_pipeline.append({
            "$addFields": {
                "totals": {"$sum": "$issues.total"},
            }
        })
        search_pipeline.append({
            "$facet": {
                "converters": [
                    {"$sort": {"converter_number": -1}},
                ],
                "grand_totals": [
                    {
                        "$group": {
                            "_id": None,
                            "grand_total": {"$sum": "$totals"},
                        }
                    },
                    {
                        "$project": {
                            "_id": 0
                        }
                    }
                ]
            }
        })

        cursor = await converters_collection.aggregate(search_pipeline)
        result = await cursor.to_list(None)

        if result and len(result) > 0:
            data = result[0]
            converters = [serializer(r) for r in data.get("converters", [])]
            totals = data.get("grand_totals", [])
            grand_totals = totals[0] if totals else {"grand_total": 0}
        else:
            converters = []
            grand_totals = {"grand_total": 0}

        return {
            "converters": converters,
            "grand_totals": grand_totals
        }


    except HTTPException:
        raise
    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")

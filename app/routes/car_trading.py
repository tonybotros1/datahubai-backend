from typing import Optional

from bson import ObjectId
from fastapi import APIRouter, Body, HTTPException, Depends
from pydantic import BaseModel
from pymongo import ReturnDocument
from app.core import security
from app.database import get_collection
from datetime import datetime, timezone
from app.websocket_config import manager

router = APIRouter()
all_trades_collection = get_collection("all_trades")
all_capitals_collection = get_collection("all_capitals")


class CapitalModel(BaseModel):
    name: Optional[str] = None
    pay: Optional[float] = None
    receive: Optional[float] = None
    comment: Optional[str] = None
    date: Optional[datetime] = None


def serialize(document: dict) -> dict:
    document["_id"] = str(document["_id"])
    document["company_id"] = str(document["company_id"])
    if document.get("name"):
        document["name"] = str(document["name"])
        document["name_id"] = str(document["name_id"])
    for key, value in document.items():
        if isinstance(value, datetime):
            document[key] = value.isoformat()
    return document


# =========================================== Capitals Section ===========================================

@router.get("/get_all_capitals")
async def get_all_capitals(data: dict = Depends(security.get_current_user)):
    company_id = ObjectId(data.get("company_id"))

    pipeline = [
        {"$match": {"company_id": company_id}},
        {
            "$lookup": {
                "from": "all_lists_values",
                "localField": "name",
                "foreignField": "_id",
                "as": "item",
            }
        },
        {
            "$unwind": {
                "path": "$item",
                "preserveNullAndEmptyArrays": True
            }
        },
        {
            "$project": {
                "_id": 1,
                "name": {"$ifNull": ["$item.name", ""]},
                "name_id": {"$ifNull": ["$item._id", ""]},
                "company_id": 1,
                "comment": 1,
                "date": 1,
                "pay": 1,
                "receive": 1,
                "createdAt": 1,
                "updatedAt": 1,
            }
        },
        {
            "$facet": {
                "capitals": [{"$match": {}}],
                "totals": [
                    {
                        "$group": {
                            "_id": None,
                            "total_pay": {"$sum": {"$ifNull": ["$pay", 0]}},
                            "total_receive": {"$sum": {"$ifNull": ["$receive", 0]}},
                        }
                    },
                    {
                        "$addFields": {
                            "total_net": {"$subtract": ["$total_receive", "$total_pay"]}
                        }
                    }
                ]
            }
        }
    ]
    cursor = await all_capitals_collection.aggregate(pipeline)
    results = await cursor.to_list()

    capitals = results[0].get("capitals", [])
    totals = results[0].get("totals", [])
    totals = totals[0] if totals else {"total_pay": 0, "total_receive": 0, "total_net": 0}
    return {
        "capitals": [serialize(c) for c in capitals],
        "totals": totals
    }


async def get_capital_details(capital_id: ObjectId):
    try:
        pipeline = [
            {
                "$match": {"_id": capital_id},
            },
            {
                "$lookup": {
                    "from": "all_lists_values",
                    "localField": "name",
                    "foreignField": "_id",
                    "as": "item",
                }
            },
            {
                "$unwind": {
                    "path": "$item",
                    "preserveNullAndEmptyArrays": True
                }
            },
            {
                "$project": {
                    "_id": 1,
                    "name": {"$ifNull": ["$item.name", ""]},
                    "name_id": {"$ifNull": ["$item._id", ""]},
                    "company_id": 1,
                    "comment": 1,
                    "date": 1,
                    "pay": 1,
                    "receive": 1,
                    "createdAt": 1,
                    "updatedAt": 1,
                }
            },
        ]
        cursor = await all_capitals_collection.aggregate(pipeline)
        result = await cursor.to_list(length=1)
        return result[0]

    except Exception as e:
        raise e


@router.get("/get_capitals_summary")
async def get_capitals_summary(data: dict = Depends(security.get_current_user)):
    company_id = ObjectId(data.get("company_id"))
    pipeline = [
        {'$match': {'company_id': company_id}},
        {
            "$group": {
                "_id": None,
                "total_pay": {"$sum": {"$ifNull": ["$pay", 0]}},
                "total_receive": {"$sum": {"$ifNull": ["$receive", 0]}},
                "count": {"$sum": 1}  # count all documents
            }
        },
        {
            "$addFields": {
                "total_net": {"$subtract": ["$total_receive", "$total_pay"]}
            }
        }
    ]

    cursor = await all_capitals_collection.aggregate(pipeline)
    result = await cursor.to_list(None)

    summary = result[0] if result else {
        "total_pay": 0,
        "total_receive": 0,
        "total_net": 0,
        "count": 0
    }

    return {"summary": summary}


@router.post("/add_new_capital")
async def add_new_capital(capital: CapitalModel, data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        capital_dict = {
            "company_id": company_id,
            "name": ObjectId(capital.name),
            "pay": capital.pay,
            "receive": capital.receive,
            "comment": capital.comment,
            "date": capital.date,
            "createdAt": datetime.now(timezone.utc),
            "updatedAt": datetime.now(timezone.utc),
        }
        result = await all_capitals_collection.insert_one(capital_dict)
        new_capital = await get_capital_details(result.inserted_id)
        serialized = serialize(new_capital)
        await manager.broadcast({
            "type": "capital_created",
            "data": serialized
        })
        return {"message": "Capital created successfully!", "capital": serialized}


    except Exception as error:
        raise error


@router.delete("/delete_capital/{capital_id}")
async def delete_capital(capital_id: str, _: dict = Depends(security.get_current_user)):
    try:
        result = await all_capitals_collection.delete_one({"_id": ObjectId(capital_id)})
        if result.deleted_count == 1:
            await manager.broadcast({
                "type": "capital_deleted",
                "data": {"_id": capital_id}
            })
            return {"message": "Capital deleted successfully!"}



    except Exception as error:
        raise error


@router.patch("/update_capital/{capital_id}")
async def update_capital(capital_id: str, capital: CapitalModel,
                         data: dict = Depends(security.get_current_user)
                         ):
    try:
        update_data = capital.model_dump(exclude_unset=True)
        company_id = ObjectId(data.get("company_id"))

        # Convert 'name' to ObjectId if present
        if "name" in update_data and update_data["name"] is not None and update_data["name"] is not '':
            try:
                update_data["name"] = ObjectId(update_data["name"])
            except Exception:
                raise HTTPException(status_code=400, detail="Invalid name id, must be a valid ObjectId")

        # Add updatedAt timestamp
        update_data["updatedAt"] = datetime.now(timezone.utc)

        # Perform update
        result = await all_capitals_collection.update_one(
            {"_id": ObjectId(capital_id)},
            {"$set": update_data}
        )

        if result.matched_count == 0:
            raise HTTPException(status_code=404, detail="Capital not found")
        updated_capital = await get_capital_details(ObjectId(capital_id))
        serialized = serialize(updated_capital)

        totals_pipeline = [
            {
                "$match": {"company_id": ObjectId(company_id)},
            },
            {
                "$group": {
                    "_id": None,
                    "totalPay": {"$sum": "$pay"},
                    "totalReceive": {"$sum": "$receive"}
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "pay": "$totalPay",
                    "receive": "$totalReceive",
                    "net": {"$subtract": ["$totalReceive", "$totalPay"]}
                }
            }
        ]
        cursor = await all_capitals_collection.aggregate(totals_pipeline)
        totals_result = await cursor.to_list(length=1)
        totals = totals_result[0] if totals_result else {"pay": 0, "receive": 0, "net": 0}

        await manager.broadcast({
            "type": "capital_updated",
            "data": serialized,
            "totals": totals
        })
        return {
            "message": "Capital updated successfully",
            "data": serialized,
            "totals": totals
        }

    except Exception as e:
        raise e

from typing import Optional

from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException

from app.core import security
from app.websocket_config import manager

from .common import (
    all_capitals_collection,
    all_outstanding_collection,
    parse_object_id,
    serialize,
    zero_if_none,
)
from .models import CapitalModel

router = APIRouter()


@router.get("/get_used_capital_names")
async def get_used_capital_names(data: dict = Depends(security.get_current_user)):
    company_id = ObjectId(data.get("company_id"))
    pipeline = [
        {
            "$match": {
                "company_id": company_id,
                "name": {"$nin": [None, ""]},
            }
        },
        {
            "$group": {
                "_id": "$name",
                "documents": {"$sum": 1},
            }
        },
        {
            "$lookup": {
                "from": "all_lists_values",
                "localField": "_id",
                "foreignField": "_id",
                "pipeline": [{"$project": {"name": 1}}],
                "as": "details",
            }
        },
        {
            "$set": {
                "name": {
                    "$ifNull": [
                        {"$arrayElemAt": ["$details.name", 0]},
                        "",
                    ]
                }
            }
        },
        {"$match": {"name": {"$ne": ""}}},
        {
            "$project": {
                "_id": {"$toString": "$_id"},
                "name": 1,
                "documents": 1,
            }
        },
        {"$sort": {"name": 1}},
    ]
    cursor = await all_capitals_collection.aggregate(pipeline)
    values = await cursor.to_list(length=None)
    return {"values": values}


@router.get("/get_all_capitals_or_outstanding/{get_type}")
async def get_all_capitals_or_outstanding(get_type: str, data: dict = Depends(security.get_current_user)):
    company_id = ObjectId(data.get("company_id"))

    pipeline = [
        {"$match": {"company_id": company_id}},
        {
            "$sort": {
                "date": 1
            }
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
            "$lookup": {
                "from": "all_lists_values",
                "localField": "account_name",
                "foreignField": "_id",
                "as": "account_name_details",
            }
        },
        {
            "$unwind": {
                "path": "$account_name_details",
                "preserveNullAndEmptyArrays": True
            }
        },
        {
            "$project": {
                "_id": 1,
                "name": {"$ifNull": ["$item.name", ""]},
                "name_id": {"$ifNull": ["$item._id", ""]},
                "account_name": {"$ifNull": ["$account_name_details.name", ""]},
                "account_name_id": {"$ifNull": ["$account_name_details._id", ""]},
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
    if get_type == "capitals":
        cursor = await all_capitals_collection.aggregate(pipeline)
    elif get_type == "outstanding":
        cursor = await all_outstanding_collection.aggregate(pipeline)
    else:
        raise HTTPException(status_code=400, detail="Invalid get_type. Use 'capitals' or 'outstanding'.")

    results = await cursor.to_list(length=None)
    if not results:
        return {"capitals": [], "totals": {"total_pay": 0, "total_receive": 0, "total_net": 0}}

    capitals = results[0].get("capitals", [])
    totals = results[0].get("totals", [])
    totals = totals[0] if totals else {"total_pay": 0, "total_receive": 0, "total_net": 0}
    return {
        "data": [serialize(c) for c in capitals],
        "totals": totals
    }


async def get_capital_or_outstanding_details(
        type_id: ObjectId,
        type_name: str,
        company_id: Optional[ObjectId] = None,
):
    try:
        match_stage = {"_id": type_id}
        if company_id is not None:
            match_stage["company_id"] = company_id
        pipeline = [
            {
                "$match": match_stage,
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
                "$lookup": {
                    "from": "all_lists_values",
                    "localField": "account_name",
                    "foreignField": "_id",
                    "as": "account_name_details",
                }
            },
            {
                "$unwind": {
                    "path": "$account_name_details",
                    "preserveNullAndEmptyArrays": True
                }
            },
            {
                "$project": {
                    "_id": 1,
                    "name": {"$ifNull": ["$item.name", ""]},
                    "name_id": {"$ifNull": ["$item._id", ""]},
                    "account_name": {"$ifNull": ["$account_name_details.name", ""]},
                    "account_name_id": {"$ifNull": ["$account_name_details._id", ""]},
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
        if type_name == "capitals":
            cursor = await all_capitals_collection.aggregate(pipeline)
        elif type_name == "outstanding":
            cursor = await all_outstanding_collection.aggregate(pipeline)
        else:
            raise HTTPException(status_code=400, detail="Invalid summary_type.")

        result = await cursor.to_list(length=1)
        if not result:
            raise HTTPException(status_code=404, detail=f"{type_name.capitalize()} not found")
        return result[0]

    except HTTPException:
        raise
    except Exception as e:
        raise e


@router.get("/get_capitals_or_outstanding_summary/{summary_type}")
async def get_capitals_or_outstanding_summary(summary_type: str, data: dict = Depends(security.get_current_user)):
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
    if summary_type == "capitals":
        cursor = await all_capitals_collection.aggregate(pipeline)
    elif summary_type == "outstanding":
        cursor = await all_outstanding_collection.aggregate(pipeline)
    else:
        raise HTTPException(status_code=400, detail="Invalid summary_type.")

    result = await cursor.to_list(None)

    summary = result[0] if result else {
        "total_pay": 0,
        "total_receive": 0,
        "total_net": 0,
        "count": 0
    }

    return {"summary": summary}


@router.post("/add_new_capital_or_outstanding/{add_type}")
async def add_new_capital_or_outstanding(add_type: str, capital: CapitalModel,
                                         data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        if add_type not in ("capitals", "outstanding"):
            raise HTTPException(status_code=400, detail="Invalid add_type.")
        if capital.date is None:
            raise HTTPException(status_code=400, detail="Date is required")
        name_id = parse_object_id(capital.name, "name")
        account_name_id = parse_object_id(capital.account_name, "account_name")
        capital_dict = {
            "company_id": company_id,
            "name": name_id,
            "pay": zero_if_none(capital.pay),
            "account_name": account_name_id,
            "receive": zero_if_none(capital.receive),
            "comment": capital.comment,
            "date": capital.date,
            "createdAt": security.now_utc(),
            "updatedAt": security.now_utc(),
        }
        result = None
        if add_type == "capitals":
            result = await all_capitals_collection.insert_one(capital_dict)
        elif add_type == "outstanding":
            result = await all_outstanding_collection.insert_one(capital_dict)
        if result:
            new_capital_or_outstanding = await get_capital_or_outstanding_details(result.inserted_id, add_type,
                                                                                  company_id)
            serialized = serialize(new_capital_or_outstanding)
            await manager.send_to_company(str(company_id), {
                "type": "capital_created" if add_type == "capitals" else "outstanding_created",
                "data": serialized
            })
            return {"message": f"{add_type.capitalize()} created successfully", "data": serialized}

    except HTTPException:
        raise
    except Exception as error:
        raise HTTPException(status_code=500, detail=str(error))


@router.delete("/delete_capital_or_outstanding/{type_name}/{type_id}")
async def delete_capital_or_outstanding(type_name: str, type_id: str, data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        type_object_id = parse_object_id(type_id, "type_id")
        if type_name == "capitals":
            result = await all_capitals_collection.find_one_and_delete(
                {"_id": type_object_id, "company_id": company_id},
            )

        elif type_name == "outstanding":
            result = await all_outstanding_collection.find_one_and_delete(
                {"_id": type_object_id, "company_id": company_id},
            )
        else:
            raise HTTPException(status_code=400, detail="Invalid type.")
        if not result:
            raise HTTPException(status_code=404, detail=f"{type_name.capitalize()} not found.")
        totals = {
            "pay": result.get("pay", 0),
            "receive": result.get("receive", 0),
        }
        await manager.send_to_company(str(company_id), {
            "type": "capital_deleted" if type_name == "capitals" else "outstanding_deleted",
            "data": {"_id": type_id},
        })
        return {
            "message": f"{type_name.capitalize()} deleted successfully!",
            "totals": totals
        }

    except HTTPException:
        raise
    except Exception as error:
        raise HTTPException(status_code=500, detail=str(error))


@router.patch("/update_capital_or_outstanding/{type_name}/{type_id}")
async def update_capital_or_outstanding(type_name: str, type_id: str, capital: CapitalModel,
                                        data: dict = Depends(security.get_current_user)
                                        ):
    try:
        update_data = capital.model_dump(exclude_unset=True)
        company_id = ObjectId(data.get("company_id"))
        type_object_id = parse_object_id(type_id, "type_id")

        if "name" in update_data:
            update_data["name"] = parse_object_id(update_data["name"], "name")
        if "account_name" in update_data:
            update_data["account_name"] = parse_object_id(update_data["account_name"], "account_name")
        if "pay" in update_data:
            update_data["pay"] = zero_if_none(update_data["pay"])
        if "receive" in update_data:
            update_data["receive"] = zero_if_none(update_data["receive"])

        update_data["updatedAt"] = security.now_utc()

        if type_name == "capitals":
            result = await all_capitals_collection.update_one(
                {"_id": type_object_id, "company_id": company_id},
                {"$set": update_data}
            )
        elif type_name == "outstanding":
            result = await all_outstanding_collection.update_one(
                {"_id": type_object_id, "company_id": company_id},
                {"$set": update_data}
            )
        else:
            raise HTTPException(status_code=400, detail="Invalid type.")

        if result.matched_count == 0:
            raise HTTPException(status_code=404, detail=f"{type_name.capitalize()} not found")
        updated_capital = await get_capital_or_outstanding_details(type_object_id, type_name, company_id)
        serialized = serialize(updated_capital)

        totals_pipeline = [
            {
                "$match": {"company_id": company_id},
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
        if type_name == "capitals":
            cursor = await all_capitals_collection.aggregate(totals_pipeline)
        elif type_name == "outstanding":
            cursor = await all_outstanding_collection.aggregate(totals_pipeline)
        else:
            raise HTTPException(status_code=400, detail="Invalid type.")
        totals_result = await cursor.to_list(length=1)
        totals = totals_result[0] if totals_result else {"pay": 0, "receive": 0, "net": 0}

        await manager.send_to_company(str(company_id), {
            "type": "capital_updated" if type_name == "capitals" else "outstanding_updated",
            "data": serialized,
            "totals": totals
        })
        return {
            "message": "Capital updated successfully",
            "data": serialized,
            "totals": totals
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

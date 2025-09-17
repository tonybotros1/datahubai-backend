from typing import Optional
from bson import ObjectId
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from app.core import security
from app.database import get_collection
from datetime import datetime, timezone
from app.websocket_config import manager

router = APIRouter()
all_trades_collection = get_collection("all_trades")
all_capitals_collection = get_collection("all_capitals")
all_outstanding_collection = get_collection("all_outstanding")
all_general_expenses_collection = get_collection("all_general_expenses")


class CapitalModel(BaseModel):
    name: Optional[str] = None
    pay: Optional[float] = None
    receive: Optional[float] = None
    comment: Optional[str] = None
    date: Optional[datetime] = None


class GeneralExpensesModel(BaseModel):
    item: Optional[str] = None
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


def general_expenses_serialize(document: dict) -> dict:
    document["_id"] = str(document["_id"])
    document["company_id"] = str(document["company_id"])
    if document.get("item"):
        document["item"] = str(document["item"])
    if document.get("item_id"):
        document["item_id"] = str(document["item_id"])
    for key, value in document.items():
        if isinstance(value, datetime):
            document[key] = value.isoformat()
    return document


# =========================================== Capitals and Outstanding Section ===========================================

@router.get("/get_all_capitals_or_outstanding/{get_type}")
async def get_all_capitals_or_outstanding(get_type: str, data: dict = Depends(security.get_current_user)):
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


async def get_capital_or_outstanding_details(type_id: ObjectId, type_name: str):
    try:
        pipeline = [
            {
                "$match": {"_id": type_id},
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
        if type_name == "capitals":
            cursor = await all_capitals_collection.aggregate(pipeline)
        elif type_name == "outstanding":
            cursor = await all_outstanding_collection.aggregate(pipeline)
        else:
            raise HTTPException(status_code=400, detail="Invalid summary_type.")

        result = await cursor.to_list(length=1)
        return result[0]

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
        capital_dict = {
            "company_id": company_id,
            "name": ObjectId(capital.name) if capital.name else "",
            "pay": capital.pay,
            "receive": capital.receive,
            "comment": capital.comment,
            "date": capital.date,
            "createdAt": datetime.now(timezone.utc),
            "updatedAt": datetime.now(timezone.utc),
        }
        if add_type == "capitals":
            result = await all_capitals_collection.insert_one(capital_dict)
        elif add_type == "outstanding":
            result = await all_outstanding_collection.insert_one(capital_dict)
        else:
            raise HTTPException(status_code=400, detail="Invalid add_type.")
        new_capital_or_outstanding = await get_capital_or_outstanding_details(result.inserted_id, add_type)
        serialized = serialize(new_capital_or_outstanding)
        await manager.broadcast({
            "type": "capital_created" if add_type == "capitals" else "outstanding_created",
            "data": serialized
        })
        # return {"message": "Capital created successfully!", "capital": serialized}


    except Exception as error:
        raise error


@router.delete("/delete_capital_or_outstanding/{type_name}/{type_id}")
async def delete_capital_or_outstanding(type_name: str, type_id: str, _: dict = Depends(security.get_current_user)):
    try:
        if type_name == "capitals":
            result = await all_capitals_collection.find_one_and_delete({"_id": ObjectId(type_id)})

        elif type_name == "outstanding":
            result = await all_outstanding_collection.find_one_and_delete({"_id": ObjectId(type_id)})
        else:
            raise HTTPException(status_code=400, detail="Invalid type.")
        if not result:
            raise HTTPException(status_code=404, detail=f"{type_name.capitalize()} not found.")
        totals = {
            "pay": result.get("pay", 0),
            "receive": result.get("receive", 0),
        }
        await manager.broadcast({
            "type": "capital_deleted" if type_name == "capitals" else "outstanding_deleted",
            "data": {"_id": type_id},
        })
        return {
            "message": f"{type_name.capitalize()} deleted successfully!",
            "totals": totals
        }

    except Exception as error:
        raise error


@router.patch("/update_capital_or_outstanding/{type_name}/{type_id}")
async def update_capital_or_outstanding(type_name: str, type_id: str, capital: CapitalModel,
                                        data: dict = Depends(security.get_current_user)
                                        ):
    try:
        update_data = capital.model_dump(exclude_unset=True)
        company_id = ObjectId(data.get("company_id"))

        if "name" in update_data and update_data["name"] is not None and update_data["name"] is not '':
            try:
                update_data["name"] = ObjectId(update_data["name"])
            except Exception:
                raise HTTPException(status_code=400, detail="Invalid name id, must be a valid ObjectId")

        update_data["updatedAt"] = datetime.now(timezone.utc)

        if type_name == "capitals":
            result = await all_capitals_collection.update_one(
                {"_id": ObjectId(type_id)},
                {"$set": update_data}
            )
        elif type_name == "outstanding":
            result = await all_outstanding_collection.update_one(
                {"_id": ObjectId(type_id)},
                {"$set": update_data}
            )
        else:
            raise HTTPException(status_code=400, detail="Invalid type.")

        if result.matched_count == 0:
            raise HTTPException(status_code=404, detail=f"{type_name.capitalize()} not found")
        updated_capital = await get_capital_or_outstanding_details(ObjectId(type_id), type_name)
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
        if type_name == "capitals":
            cursor = await all_capitals_collection.aggregate(totals_pipeline)
        elif type_name == "outstanding":
            cursor = await all_outstanding_collection.aggregate(totals_pipeline)
        else:
            raise HTTPException(status_code=400, detail="Invalid type.")
        totals_result = await cursor.to_list(length=1)
        totals = totals_result[0] if totals_result else {"pay": 0, "receive": 0, "net": 0}

        await manager.broadcast({
            "type": "capital_updated" if type_name == "capitals" else "outstanding_updated",
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


# =========================================== General Expenses Section ===========================================


@router.get("/get_all_general_expenses")
async def get_all_general_expenses(data: dict = Depends(security.get_current_user)):
    company_id = ObjectId(data.get("company_id"))

    pipeline = [
        {"$match": {"company_id": company_id}},
        {
            "$lookup": {
                "from": "all_lists_values",
                "localField": "item",
                "foreignField": "_id",
                "as": "items",
            }
        },
        {
            "$unwind": {
                "path": "$items",
                "preserveNullAndEmptyArrays": True
            }
        },
        {
            "$project": {
                "_id": 1,
                "item": {"$ifNull": ["$items.name", ""]},
                "item_id": {"$ifNull": ["$items._id", ""]},
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
                "general_expenses": [{"$match": {}}],
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

    cursor = await all_general_expenses_collection.aggregate(pipeline)
    results = await cursor.to_list(length=None)
    if not results:
        return {"capitals": [], "totals": {"total_pay": 0, "total_receive": 0, "total_net": 0}}

    general_expenses = results[0].get("general_expenses", [])
    totals = results[0].get("totals", [])
    totals = totals[0] if totals else {"total_pay": 0, "total_receive": 0, "total_net": 0}
    return {
        "data": [general_expenses_serialize(g) for g in general_expenses],
        "totals": totals
    }


async def get_general_expenses_details(type_id: ObjectId):
    try:
        pipeline = [
            {
                "$match": {"_id": type_id},
            },
            {
                "$lookup": {
                    "from": "all_lists_values",
                    "localField": "item",
                    "foreignField": "_id",
                    "as": "items",
                }
            },
            {
                "$unwind": {
                    "path": "$items",
                    "preserveNullAndEmptyArrays": True
                }
            },
            {
                "$project": {
                    "_id": 1,
                    "item": {"$ifNull": ["$items.name", ""]},
                    "item_id": {"$ifNull": ["$items._id", ""]},
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
        cursor = await all_general_expenses_collection.aggregate(pipeline)
        result = await cursor.to_list(length=1)
        return result[0]

    except Exception as e:
        raise e


@router.get("/get_general_expenses_summary")
async def get_general_expenses_summary(data: dict = Depends(security.get_current_user)):
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

    cursor = await all_general_expenses_collection.aggregate(pipeline)
    result = await cursor.to_list(None)

    summary = result[0] if result else {
        "total_pay": 0,
        "total_receive": 0,
        "total_net": 0,
        "count": 0
    }

    return {"summary": summary}


@router.post("/add_new_general_expenses")
async def add_new_general_expenses(general: GeneralExpensesModel,
                                   data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        capital_dict = {
            "company_id": company_id,
            "item": ObjectId(general.item) if general.item else "",
            "pay": general.pay,
            "receive": general.receive,
            "comment": general.comment,
            "date": general.date,
            "createdAt": datetime.now(timezone.utc),
            "updatedAt": datetime.now(timezone.utc),
        }

        result = await all_general_expenses_collection.insert_one(capital_dict)

        new_capital_or_outstanding = await get_general_expenses_details(result.inserted_id)
        serialized = general_expenses_serialize(new_capital_or_outstanding)
        await manager.broadcast({
            "type": "general_expenses_created",
            "data": serialized
        })


    except Exception as error:
        raise error


@router.delete("/delete_general_expenses/{type_id}")
async def delete_general_expenses(type_id: str, _: dict = Depends(security.get_current_user)):
    try:

        result = await all_general_expenses_collection.find_one_and_delete({"_id": ObjectId(type_id)})
        if not result:
            raise HTTPException(status_code=404, detail="General expenses not found.")
        totals = {
            "pay": result.get("pay", 0),
            "receive": result.get("receive", 0),
        }
        await manager.broadcast({
            "type": "general_expenses_deleted",
            "data": {"_id": type_id},
        })
        return {
            "message": "General expenses deleted successfully!",
            "totals": totals
        }

    except Exception as error:
        raise error


@router.patch("/update_generale_expenses/{type_id}")
async def update_generale_expenses(type_id: str, general: GeneralExpensesModel,
                                   data: dict = Depends(security.get_current_user)
                                   ):
    try:
        update_data = general.model_dump(exclude_unset=True)
        company_id = ObjectId(data.get("company_id"))

        if "item" in update_data and update_data["item"] is not None and update_data["item"] is not '':
            try:
                update_data["item"] = ObjectId(update_data["item"])
            except Exception:
                raise HTTPException(status_code=400, detail="Invalid item id, must be a valid ObjectId")

        update_data["updatedAt"] = datetime.now(timezone.utc)

        result = await all_general_expenses_collection.update_one(
            {"_id": ObjectId(type_id)},
            {"$set": update_data}
        )

        if result.matched_count == 0:
            raise HTTPException(status_code=404, detail="General expenses not found")
        updated_capital = await get_general_expenses_details(ObjectId(type_id))
        serialized = general_expenses_serialize(updated_capital)

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
        cursor = await all_general_expenses_collection.aggregate(totals_pipeline)
        totals_result = await cursor.to_list(length=1)
        totals = totals_result[0] if totals_result else {"pay": 0, "receive": 0, "net": 0}

        await manager.broadcast({
            "type": "general_expenses_updated",
            "data": serialized,
            "totals": totals
        })
        return {
            "message": "General expenses updated successfully",
            "data": serialized,
            "totals": totals
        }

    except Exception as e:
        raise e

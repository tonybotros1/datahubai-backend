import traceback
from typing import Optional, List
from bson import ObjectId
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from pydantic_core import core_schema
from pymongo import UpdateOne
from pymongo.errors import PyMongoError

from app import database
from app.core import security
from app.database import get_collection
from datetime import datetime, timezone, timedelta
from app.websocket_config import manager

router = APIRouter()
all_trades_collection = get_collection("all_trades")
all_trades_items_collection = get_collection("all_trades_items")
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


# Custom type for ObjectId
class PyObjectId(ObjectId):
    @classmethod
    def __get_pydantic_core_schema__(cls, source, handler):
        return core_schema.no_info_after_validator_function(
            cls.validate,
            core_schema.str_schema()
        )

    @classmethod
    def validate(cls, v):
        if v in (None, ""):  # allow None or empty string
            return None
        if isinstance(v, ObjectId):
            return v
        if not ObjectId.is_valid(v):
            raise ValueError("Invalid ObjectId")
        return ObjectId(v)


class CarTradingItemsModel(BaseModel):
    uuid: Optional[str] = None
    item: Optional[str] = None
    item_id: Optional[PyObjectId]
    trade_id: Optional[PyObjectId] = None
    pay: Optional[float] = None
    receive: Optional[float] = None
    comment: Optional[str] = None
    date: Optional[datetime] = None
    deleted: Optional[bool] = False
    added: Optional[bool] = False
    modified: Optional[bool] = False

    model_config = {
        "arbitrary_types_allowed": True
    }


class CarTradingSearch(BaseModel):
    car_brand: Optional[PyObjectId] = None
    car_model: Optional[PyObjectId] = None
    specification: Optional[PyObjectId] = None
    engine_size: Optional[PyObjectId] = None
    bought_from: Optional[PyObjectId] = None
    sold_to: Optional[PyObjectId] = None
    status: Optional[str] = None
    from_date: Optional[datetime] = None
    to_date: Optional[datetime] = None
    all: Optional[bool] = False
    today: Optional[bool] = False
    this_month: Optional[bool] = False
    this_year: Optional[bool] = False


class CarTradingModel(BaseModel):
    date: Optional[datetime] = None
    mileage: Optional[float] = None
    color_out: Optional[PyObjectId] = None
    color_in: Optional[PyObjectId] = None
    car_brand: Optional[PyObjectId] = None
    car_model: Optional[PyObjectId] = None
    specification: Optional[PyObjectId] = None
    engine_size: Optional[PyObjectId] = None
    year: Optional[PyObjectId] = None
    bought_from: Optional[PyObjectId] = None
    sold_to: Optional[PyObjectId] = None
    note: Optional[str] = None
    status: Optional[str] = None
    items: Optional[List[CarTradingItemsModel]] = None

    model_config = {
        "arbitrary_types_allowed": True
    }


def bson_serializer(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    elif isinstance(obj, ObjectId):
        return str(obj)
    elif isinstance(obj, list):
        return [bson_serializer(item) for item in obj]
    elif isinstance(obj, dict):
        return {k: bson_serializer(v) for k, v in obj.items()}
    return obj


def car_trade_search_serializer(trade: dict) -> dict:
    return bson_serializer(trade)


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
            "$sort": {
                "date": 1
            }
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


# =========================================== Car Trading Section ===========================================

@router.post("/add_new_trade")
async def add_new_trade(trade: CarTradingModel, data: dict = Depends(security.get_current_user)):
    company_id = ObjectId(data.get("company_id"))
    async with database.client.start_session() as session:
        try:
            await session.start_transaction()
            uuid_map = []
            trade_dict = {
                "company_id": company_id if company_id else "",
                "date": trade.date,
                "mileage": trade.mileage,
                "color_in": trade.color_in if trade.color_in else "",
                "color_out": trade.color_out if trade.color_out else "",
                "car_brand": trade.car_brand if trade.car_brand else "",
                "car_model": trade.car_model if trade.car_model else "",
                "specification": trade.specification if trade.specification else "",
                "engine_size": trade.engine_size if trade.engine_size else "",
                "year": trade.year if trade.year else "",
                "status": "New",
                "bought_from": trade.bought_from if trade.bought_from else "",
                "sold_to": trade.sold_to if trade.sold_to else "",
                "note": trade.note,
                "createdAt": datetime.now(timezone.utc),
                "updatedAt": datetime.now(timezone.utc),
            }

            result = await all_trades_collection.insert_one(trade_dict, session=session)
            if not result.inserted_id:
                raise HTTPException(status_code=500, detail="Failed to insert trade")

            if trade.items:
                items_to_insert = [
                    {
                        "company_id": ObjectId(company_id) if company_id else "",
                        "trade_id": result.inserted_id,
                        "date": item.date,
                        "item": ObjectId(str(item.item_id)) if item.item_id else "",
                        "pay": item.pay,
                        "receive": item.receive,
                        "comment": item.comment,
                        "createdAt": datetime.now(timezone.utc),
                        "updatedAt": datetime.now(timezone.utc),
                    }
                    for item in trade.items
                ]
                items_result = await all_trades_items_collection.insert_many(items_to_insert, session=session)
                if not items_result.inserted_ids:
                    raise HTTPException(status_code=500, detail="Failed to insert trade items")

                for i, inserted_id in enumerate(items_result.inserted_ids):
                    uuid_val = getattr(trade.items[i], "uuid", None)
                    print(uuid_val)
                    if uuid_val:
                        uuid_map.append({"uuid": uuid_val, "db_id": str(inserted_id)})

            await session.commit_transaction()

            return {"message": "Trade added successfully", "trade_id": str(result.inserted_id), "items_map": uuid_map}
        except HTTPException:
            raise
        except Exception as e:
            await session.abort_transaction()
            traceback.print_exc()
            raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")


@router.patch("/update_trade/{trade_id}")
async def update_trade(trade_id: str, trade: CarTradingModel,
                       _: dict = Depends(security.get_current_user)):
    try:
        updated_trade = trade.model_dump(exclude_unset=True)

        updated_trade["updatedAt"] = datetime.now(timezone.utc)
        await all_trades_collection.update_one({"_id": ObjectId(trade_id)}, {"$set": updated_trade})

        return {"message": "Trade updated successfully", "trade_id": trade_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")


@router.patch("/update_trade_items")
async def update_trade_items(
        items: list[CarTradingItemsModel],
        data: dict = Depends(security.get_current_user),
):
    try:
        company_id = data.get("company_id")
        operations = []
        deleted_items = []
        added_items = []
        uuid_map = []

        for item in items:
            if item.modified and not item.deleted and not item.added:
                # UPDATE
                item_dict = item.model_dump(exclude_unset=True)
                updated = {
                    "date": item_dict["date"],
                    "item": ObjectId(str(item.item_id)) if item.item_id else "",
                    "pay": item_dict["pay"],
                    "receive": item_dict["receive"],
                    "comment": item_dict["comment"],
                    "updatedAt": datetime.now(timezone.utc),
                }
                operations.append(
                    UpdateOne(
                        {"_id": ObjectId(item_dict["uuid"])},
                        {"$set": updated},
                    )
                )

            elif item.modified and item.deleted and not item.added:
                # DELETE
                item_dict = item.model_dump(exclude_unset=True)
                deleted_items.append(ObjectId(item_dict["uuid"]))

            elif item.added and not item.deleted:
                # INSERT
                added_items.append(
                    {
                        "company_id": ObjectId(company_id) if company_id else "",
                        "trade_id": ObjectId(item.trade_id),
                        "date": item.date,
                        "item": ObjectId(str(item.item_id)) if item.item_id else None,
                        "pay": item.pay,
                        "receive": item.receive,
                        "comment": item.comment,
                        "createdAt": datetime.now(timezone.utc),
                        "updatedAt": datetime.now(timezone.utc),
                    }
                )
                uuid_map.append({"uuid": getattr(item, "uuid", "")})

        # ---- Execute in batch AFTER the loop ----
        if operations:
            await all_trades_items_collection.bulk_write(operations)

        if deleted_items:
            await all_trades_items_collection.delete_many(
                {"_id": {"$in": deleted_items}}
            )

        if added_items:
            items_result = await all_trades_items_collection.insert_many(added_items)
            for j, inserted_id in enumerate(items_result.inserted_ids):
                if "uuid" in uuid_map[j]:
                    uuid_map[j]["db_id"] = str(inserted_id)

        return {"message": "Success", "items_map": uuid_map}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")


@router.post("/search_engine_for_car_trading")
async def search_engine_for_car_trading(
        filter_trades: CarTradingSearch,
        data: dict = Depends(security.get_current_user)
):
    try:
        company_id = ObjectId(data.get("company_id"))
        pipeline: list[dict] = []

        # -------------------------------
        # Initial match stage
        # -------------------------------
        match_stage = {}
        if company_id:
            match_stage["company_id"] = company_id
        if filter_trades.car_brand:
            match_stage["car_brand"] = filter_trades.car_brand
        if filter_trades.car_model:
            match_stage["car_model"] = filter_trades.car_model
        if filter_trades.specification:
            match_stage["specification"] = filter_trades.specification
        if filter_trades.engine_size:
            match_stage["engine_size"] = filter_trades.engine_size
        if filter_trades.bought_from:
            match_stage["bought_from"] = filter_trades.bought_from
        if filter_trades.sold_to:
            match_stage["sold_to"] = filter_trades.sold_to
        if filter_trades.status:
            match_stage["status"] = filter_trades.status

        pipeline.append({"$match": match_stage})

        # -------------------------------
        # Lookups for brand/model/etc
        # -------------------------------
        lookups = [
            ("car_brand", "all_brands"),
            ("car_model", "all_brand_models"),
            ("color_in", "all_lists_values"),
            ("color_out", "all_lists_values"),
            ("specification", "all_lists_values"),
            ("engine_size", "all_lists_values"),
            ("year", "all_lists_values"),
            ("bought_from", "all_lists_values"),
            ("sold_to", "all_lists_values"),
        ]

        for local_field, collection in lookups:
            pipeline.append({
                "$lookup": {
                    "from": collection,
                    "let": {"field_id": f"${local_field}"},
                    "pipeline": [
                        {"$match": {"$expr": {"$eq": ["$_id", "$$field_id"]}}},
                        {"$project": {"name": 1}}
                    ],
                    "as": local_field
                }
            })
            pipeline.append({"$unwind": {"path": f"${local_field}", "preserveNullAndEmptyArrays": True}})

        # -------------------------------
        # Lookup trade items
        # -------------------------------
        pipeline.append({
            "$lookup": {
                "from": "all_trades_items",
                "localField": "_id",
                "foreignField": "trade_id",
                "as": "trade_items"
            }
        })
        pipeline.append({"$unwind": {"path": "$trade_items", "preserveNullAndEmptyArrays": True}})

        # Lookup item name
        pipeline.append({
            "$lookup": {
                "from": "all_lists_values",
                "let": {"item_id": "$trade_items.item"},
                "pipeline": [
                    {"$match": {"$expr": {"$eq": ["$_id", "$$item_id"]}}},
                    {"$project": {"name": 1}}
                ],
                "as": "item_detail"
            }
        })
        pipeline.append({"$unwind": {"path": "$item_detail", "preserveNullAndEmptyArrays": True}})

        # -------------------------------
        # Add temporary fields for BUY/SELL dates
        # -------------------------------
        pipeline.append({
            "$addFields": {
                "buy_date_tmp": {
                    "$cond": [
                        {"$eq": ["$item_detail.name", "BUY"]},
                        "$trade_items.date",
                        None
                    ]
                },
                "sell_date_tmp": {
                    "$cond": [
                        {"$eq": ["$item_detail.name", "SELL"]},
                        "$trade_items.date",
                        None
                    ]
                }
            }
        })

        # -------------------------------
        # Group per trade
        # -------------------------------
        pipeline.append({
            "$group": {
                "_id": "$_id",
                "date": {"$first": "$date"},
                "note": {"$first": "$note"},
                "status": {"$first": "$status"},
                "mileage": {"$first": "$mileage"},
                "car_brand": {"$first": "$car_brand"},
                "car_model": {"$first": "$car_model"},
                "car_year": {"$first": "$year"},
                "car_color_in": {"$first": "$color_in"},
                "car_color_out": {"$first": "$color_out"},
                "car_specification": {"$first": "$specification"},
                "car_engine_size": {"$first": "$engine_size"},
                "car_bought_from": {"$first": "$bought_from"},
                "car_sold_to": {"$first": "$sold_to"},
                "trade_items": {
                    "$push": {
                        "$cond": [
                            {"$ifNull": ["$trade_items._id", False]},
                            {
                                "_id": "$trade_items._id",
                                "company_id": "$trade_items.company_id",
                                "trade_id": "$trade_items.trade_id",
                                "date": "$trade_items.date",
                                "item_id": "$item_detail._id",
                                "item": "$item_detail.name",
                                "pay": "$trade_items.pay",
                                "receive": "$trade_items.receive",
                                "comment": "$trade_items.comment",
                                "createdAt": "$trade_items.createdAt",
                                "updatedAt": "$trade_items.updatedAt"
                            },
                            "$$REMOVE"
                        ]
                    }
                },

                "buy_date": {"$min": "$buy_date_tmp"},
                "sell_date": {"$min": "$sell_date_tmp"},
                "total_pay": {"$sum": {"$ifNull": ["$trade_items.pay", 0]}},
                "total_receive": {"$sum": {"$ifNull": ["$trade_items.receive", 0]}}
            }
        })

        # -------------------------------
        # Date filtering after group
        # -------------------------------
        now = datetime.now(timezone.utc)
        date_field = "buy_date"
        if filter_trades.status and filter_trades.status.lower() == "sold":
            date_field = "sell_date"

        date_filter = {}
        if filter_trades.today:
            start = datetime(now.year, now.month, now.day, tzinfo=timezone.utc)
            end = start + timedelta(days=1)
            print(start, end)
            date_filter[date_field] = {"$gte": start, "$lt": end}

        elif filter_trades.this_month:
            start = datetime(now.year, now.month, 1, tzinfo=timezone.utc)
            end = datetime(now.year + (now.month // 12), ((now.month % 12) + 1), 1)
            date_filter[date_field] = {"$gte": start, "$lt": end}

        elif filter_trades.this_year:
            start = datetime(now.year, 1, 1, tzinfo=timezone.utc)
            end = datetime(now.year + 1, 1, 1)
            date_filter[date_field] = {"$gte": start, "$lt": end}

        elif filter_trades.from_date or filter_trades.to_date:
            date_filter[date_field] = {}
            if filter_trades.from_date:
                print("from date")
                date_filter[date_field]["$gte"] = filter_trades.from_date
            if filter_trades.to_date:
                print("to date")
                date_filter[date_field]["$lte"] = filter_trades.to_date
            print(date_filter)

        if date_filter:
            pipeline.append({"$match": date_filter})

        # -------------------------------
        # Final projection
        # -------------------------------
        pipeline.append({
            "$project": {
                "_id": 1,  # usually always present
                "car_brand_id": {"$ifNull": ["$car_brand._id", ""]},
                "car_brand": {"$ifNull": ["$car_brand.name", ""]},
                "car_model_id": {"$ifNull": ["$car_model._id", ""]},
                "car_model": {"$ifNull": ["$car_model.name", ""]},
                "year_id": {"$ifNull": ["$car_year._id", ""]},
                "year": {"$ifNull": ["$car_year.name", ""]},
                "status": {"$ifNull": ["$status", ""]},
                "color_in_id": {"$ifNull": ["$car_color_in._id", ""]},
                "color_in": {"$ifNull": ["$car_color_in.name", ""]},
                "color_out_id": {"$ifNull": ["$car_color_out._id", ""]},
                "color_out": {"$ifNull": ["$car_color_out.name", ""]},
                "specification_id": {"$ifNull": ["$car_specification._id", ""]},
                "specification": {"$ifNull": ["$car_specification.name", ""]},
                "engine_size_id": {"$ifNull": ["$car_engine_size._id", ""]},
                "engine_size": {"$ifNull": ["$car_engine_size.name", ""]},
                "mileage": {"$ifNull": ["$mileage", 0]},
                "bought_from_id": {"$ifNull": ["$car_bought_from._id", ""]},
                "bought_from": {"$ifNull": ["$car_bought_from.name", ""]},
                "sold_to_id": {"$ifNull": ["$car_sold_to._id", ""]},
                "sold_to": {"$ifNull": ["$car_sold_to.name", ""]},
                "note": {"$ifNull": ["$note", ""]},
                "date": {"$ifNull": ["$date", ""]},
                "trade_items": {"$ifNull": ["$trade_items", []]},
                "buy_date": {"$ifNull": ["$buy_date", ""]},
                "sell_date": {"$ifNull": ["$sell_date", ""]},
                "total_pay": {"$toDouble": {"$ifNull": ["$total_pay", 0]}},
                "total_receive": {"$toDouble": {"$ifNull": ["$total_receive", 0]}},
                "net": {
                    "$subtract": [
                        {"$toDouble": {"$ifNull": ["$total_receive", 0]}},
                        {"$toDouble": {"$ifNull": ["$total_pay", 0]}}
                    ]
                }

            }
        })

        # -------------------------------
        # Sorting
        # -------------------------------
        sort_field = "sell_date" if (filter_trades.status and filter_trades.status.lower() == "sold") else "buy_date"
        pipeline.append({"$sort": {sort_field: -1}})

        # -------------------------------
        # Grand totals
        # -------------------------------
        pipeline.append({
            "$group": {
                "_id": None,
                "trades": {"$push": "$$ROOT"},
                "grand_total_pay": {"$sum": "$total_pay"},
                "grand_total_receive": {"$sum": "$total_receive"},
                "grand_net": {"$sum": "$net"}
            }
        })

        # Execute aggregation
        cursor = await all_trades_collection.aggregate(pipeline)
        results = await cursor.to_list(None)

        if results:
            return [car_trade_search_serializer(r) for r in results]
        else:
            return [{"trades": [], "grand_total_pay": 0, "grand_total_receive": 0, "grand_net": 0}]

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")


@router.delete("/delete_trade/{trade_id}")
async def delete_trade(trade_id: str, _: dict = Depends(security.get_current_user)):
    try:
        # Validate ObjectId
        if not ObjectId.is_valid(trade_id):
            raise HTTPException(status_code=400, detail="Invalid trade ID")

        async with database.client.start_session() as session:
            await session.start_transaction()  # <-- await, not async with
            result1 = await all_trades_collection.delete_one(
                {"_id": ObjectId(trade_id)}, session=session
            )
            await all_trades_items_collection.delete_many(
                {"trade_id": ObjectId(trade_id)}, session=session
            )
            await session.commit_transaction()  # commit the transaction

            if result1.deleted_count == 0:
                raise HTTPException(status_code=404, detail="Trade not found")

        return {"message": "Trade and its items deleted successfully"}

    except PyMongoError as e:
        print(e)
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")

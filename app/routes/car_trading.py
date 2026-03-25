import copy
from typing import Optional, Any
from bson import ObjectId
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from pydantic_core import core_schema
from pymongo.errors import PyMongoError

from app import database
from app.core import security
from app.database import get_collection
from datetime import datetime, timezone, timedelta

from app.routes.counters import create_custom_counter
from app.websocket_config import manager
from fastapi.encoders import jsonable_encoder

router = APIRouter()
all_trades_collection = get_collection("all_trades")
all_trades_items_collection = get_collection("all_trades_items")
all_trades_purchase_agreement_items_collection = get_collection("all_trades_purchase_agreement_items")
all_capitals_collection = get_collection("all_capitals")
all_outstanding_collection = get_collection("all_outstanding")
all_general_expenses_collection = get_collection("all_general_expenses")
all_trades_transfers_collection = get_collection("all_trades_transfers")


class CapitalModel(BaseModel):
    name: Optional[str] = None
    pay: Optional[float] = None
    account_name: Optional[str] = None
    receive: Optional[float] = None
    comment: Optional[str] = None
    date: Optional[datetime] = None


class GeneralExpensesModel(BaseModel):
    item: Optional[str] = None
    pay: Optional[float] = None
    receive: Optional[float] = None
    account_name: Optional[str] = None
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
    item: Optional[str] = None
    trade_id: Optional[str] = None
    pay: Optional[float] = None
    receive: Optional[float] = None
    account_name: Optional[str] = None
    comment: Optional[str] = None
    date: Optional[datetime] = None

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
    sold_by: Optional[PyObjectId] = None
    bought_by: Optional[PyObjectId] = None
    status: Optional[str] = None
    from_date: Optional[datetime] = None
    to_date: Optional[datetime] = None
    all: Optional[bool] = False
    today: Optional[bool] = False
    this_month: Optional[bool] = False
    this_year: Optional[bool] = False


class ExpensesSearchModel(BaseModel):
    from_date: Optional[datetime] = None
    to_date: Optional[datetime] = None
    all: Optional[bool] = False
    today: Optional[bool] = False
    this_month: Optional[bool] = False
    this_year: Optional[bool] = False


class CarTradingModel(BaseModel):
    date: Optional[datetime] = None
    warranty_end_date: Optional[datetime] = None
    service_contract_end_date: Optional[datetime] = None
    mileage: Optional[float] = None
    color_out: Optional[PyObjectId] = None
    color_in: Optional[PyObjectId] = None
    car_brand: Optional[PyObjectId] = None
    car_model: Optional[PyObjectId] = None
    specification: Optional[PyObjectId] = None
    engine_size: Optional[PyObjectId] = None
    year: Optional[PyObjectId] = None
    vin: Optional[str] = None
    bought_from: Optional[PyObjectId] = None
    sold_to: Optional[PyObjectId] = None
    note: Optional[str] = None
    status: Optional[str] = None
    bought_by: Optional[PyObjectId] = None
    sold_by: Optional[PyObjectId] = None
    # items: Optional[List[CarTradingItemsModel]] = None

    model_config = {
        "arbitrary_types_allowed": True
    }


class LastChangesFilter(BaseModel):
    min_amount: Optional[float] = None
    max_amount: Optional[float] = None
    account_name: Optional[PyObjectId] = None
    from_date: Optional[datetime] = None
    to_date: Optional[datetime] = None
    account: Optional[str] = None


class PurchaseAgreementModel(BaseModel):
    trade_id: Optional[str] = None
    agreement_date: Optional[datetime] = None
    agreement_note: Optional[str] = None
    buyer_name: Optional[str] = None
    buyer_ID: Optional[str] = None
    buyer_phone: Optional[str] = None
    buyer_email: Optional[str] = None
    seller_name: Optional[str] = None
    seller_ID: Optional[str] = None
    seller_phone: Optional[str] = None
    seller_email: Optional[str] = None
    note: Optional[str] = None
    agreement_amount: Optional[float] = None
    agreement_down_payment: Optional[float] = None


class TransferModel(BaseModel):
    date: Optional[datetime] = None
    from_account: Optional[str] = None
    to_account: Optional[str] = None
    amount: Optional[float] = None
    comment: Optional[str] = None


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
    if document.get("account_name"):
        document["account_name"] = str(document["account_name"])
        document["account_name_id"] = str(document["account_name_id"])
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
    if document.get("account_name"):
        document["account_name"] = str(document["account_name"])
        document["account_name_id"] = str(document["account_name_id"])
    for key, value in document.items():
        if isinstance(value, datetime):
            document[key] = value.isoformat()
    return document


# =========================================== Start of Transfers Section ===========================================
transfer_pipeline = [
    {
        '$lookup': {
            'from': 'all_lists_values',
            'let': {
                'fromAcc': '$from_account',
                'toAcc': '$to_account'
            },
            'pipeline': [
                {
                    '$match': {
                        '$expr': {
                            '$in': [
                                '$_id', [
                                    '$$fromAcc', '$$toAcc'
                                ]
                            ]
                        }
                    }
                }, {
                    '$project': {
                        'name': 1
                    }
                }
            ],
            'as': 'accounts'
        }
    }, {
        '$addFields': {
            'from_account_name': {
                '$let': {
                    'vars': {
                        'match': {
                            '$first': {
                                '$filter': {
                                    'input': '$accounts',
                                    'cond': {
                                        '$eq': [
                                            '$$this._id', '$from_account'
                                        ]
                                    }
                                }
                            }
                        }
                    },
                    'in': '$$match.name'
                }
            },
            'to_account_name': {
                '$let': {
                    'vars': {
                        'match': {
                            '$first': {
                                '$filter': {
                                    'input': '$accounts',
                                    'cond': {
                                        '$eq': [
                                            '$$this._id', '$to_account'
                                        ]
                                    }
                                }
                            }
                        }
                    },
                    'in': '$$match.name'
                }
            },
            '_id': {
                '$toString': '$_id'
            },
            'from_account': {
                '$toString': '$from_account'
            },
            'to_account': {
                '$toString': '$to_account'
            },
            'company_id': {
                '$toString': '$company_id'
            }
        }
    }, {
        '$project': {
            'accounts': 0
        }
    }
]

trade_item_details_pipeline = [
    {
        '$lookup': {
            'from': 'all_lists_values',
            'let': {
                'itemID': '$item',
                'accountID': '$account_name'
            },
            'pipeline': [
                {
                    '$match': {
                        '$expr': {
                            '$in': [
                                '$_id', [
                                    '$$itemID', '$$accountID'
                                ]
                            ]
                        }
                    }
                }, {
                    '$project': {
                        'name': 1
                    }
                }
            ],
            'as': 'details'
        }
    }, {
        '$addFields': {
            'item_id': {
                '$toString': '$item'
            },
            'item': {
                '$let': {
                    'vars': {
                        'match': {
                            '$first': {
                                '$filter': {
                                    'input': '$details',
                                    'cond': {
                                        '$eq': [
                                            '$$this._id', '$item'
                                        ]
                                    }
                                }
                            }
                        }
                    },
                    'in': '$$match.name'
                }
            },
            'account_name': {
                '$let': {
                    'vars': {
                        'match': {
                            '$first': {
                                '$filter': {
                                    'input': '$details',
                                    'cond': {
                                        '$eq': [
                                            '$$this._id', '$account_name'
                                        ]
                                    }
                                }
                            }
                        }
                    },
                    'in': '$$match.name'
                }
            },
            '_id': {
                '$toString': '$_id'
            },
            'company_id': {
                '$toString': '$company_id'
            },
            'trade_id': {
                '$toString': '$trade_id'
            }
        }
    }, {
        '$project': {
            'details': 0
        }
    }
]


@router.get("/get_all_transfers")
async def get_all_transfers(data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        all_data: Any = copy.deepcopy(transfer_pipeline)
        all_data.insert(0, {"$match": {"company_id": company_id}})
        cursor = await all_trades_transfers_collection.aggregate(all_data)
        results = await cursor.to_list(None)
        return {"transfers": results}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


async def get_transfer_details(transfer_id: ObjectId):
    try:
        details_pipeline: Any = copy.deepcopy(transfer_pipeline)
        details_pipeline.insert(0, {"$match": {"_id": transfer_id}})

        cursor = await all_trades_transfers_collection.aggregate(details_pipeline)
        results = await cursor.to_list(length=1)

        return results[0] if results else None

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/add_new_transfer")
async def add_new_transfer(transfer_data: TransferModel, data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        transfer_data = transfer_data.model_dump(exclude_unset=True)

        transfer_data.update({
            "company_id": company_id,
            "from_account": ObjectId(transfer_data["from_account"]) if transfer_data["from_account"] else None,
            "to_account": ObjectId(transfer_data["to_account"]) if transfer_data["to_account"] else None,
            "createdAt": security.now_utc(),
            "updatedAt": security.now_utc(),
        })

        result = await all_trades_transfers_collection.insert_one(transfer_data)
        if not result.inserted_id:
            raise HTTPException(status_code=500, detail="Failed to insert transfer item")

        added_transfer = await get_transfer_details(result.inserted_id)

        encoded_data = jsonable_encoder(added_transfer)
        await manager.send_to_company(str(company_id), {
            "type": "transfer_created",
            "data": encoded_data
        })


    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.patch("/update_new_transfer/{transfer_id}")
async def update_new_transfer(transfer_id: str, transfer_data: TransferModel,
                              data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        transfer_id = ObjectId(transfer_id)
        transfer_data = transfer_data.model_dump(exclude_unset=True)

        transfer_data.update({
            "company_id": company_id,
            "from_account": ObjectId(transfer_data["from_account"]) if transfer_data["from_account"] else None,
            "to_account": ObjectId(transfer_data["to_account"]) if transfer_data["to_account"] else None,
            "createdAt": security.now_utc(),
            "updatedAt": security.now_utc(),
        })

        await all_trades_transfers_collection.update_one({"_id": transfer_id}, {"$set": transfer_data})

        added_transfer = await get_transfer_details(transfer_id)

        encoded_data = jsonable_encoder(added_transfer)
        await manager.send_to_company(str(company_id), {
            "type": "transfer_updated",
            "data": encoded_data
        })

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/delete_transfer/{transfer_id}")
async def delete_transfer(transfer_id: str, data: dict = Depends(security.get_current_user)):
    try:
        company_id = data.get("company_id")
        transfer_id = ObjectId(transfer_id)
        result = await all_trades_transfers_collection.delete_one({"_id": transfer_id})
        if result.deleted_count == 0:
            raise HTTPException(status_code=404, detail="Transfer not found")

        await manager.send_to_company(company_id, {
            "type": "transfer_deleted",
            "data": {"_id": str(transfer_id)}
        })

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# =========================================== End of Transfers Section ===========================================

@router.get("/get_purchase_agreement_for_current_trade/{trade_id}")
async def get_purchase_agreement_for_current_trade(trade_id: str,
                                                   _: dict = Depends(security.get_current_user)):
    try:
        trade_id = ObjectId(trade_id)
        purchase_agreement_items_pipeline = [
            {
                '$match': {
                    'trade_id': trade_id
                }
            }, {
                '$sort': {
                    'agreement_number': 1
                }
            }, {
                '$addFields': {
                    '_id': {
                        '$toString': '$_id'
                    },
                    'trade_id': {
                        '$toString': '$trade_id'
                    },
                    'company_id': {
                        '$toString': '$company_id'
                    }
                }
            }
        ]
        cursor = await all_trades_purchase_agreement_items_collection.aggregate(purchase_agreement_items_pipeline)
        results = await cursor.to_list(None)
        return {'purchase_agreement_items': results if results else []}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/add_purchase_agreement_item")
async def add_purchase_agreement_item(purchase_agreement_item: PurchaseAgreementModel,
                                      data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get('company_id'))
        purchase_agreement_item_dict = purchase_agreement_item.model_dump(exclude_unset=True)
        trade_id = purchase_agreement_item_dict.get("trade_id")
        if not trade_id:
            raise HTTPException(status_code=400, detail="No trade id found")
        new_purchase_agreement_counter = await create_custom_counter("CMP", "CM", data=data,
                                                                     description='Compass Motors Purchase Agreement')

        purchase_agreement_item_dict.update({
            "trade_id": ObjectId(purchase_agreement_item_dict["trade_id"]) if purchase_agreement_item_dict[
                "trade_id"] else None,
            "company_id": company_id,
            "createdAt": security.now_utc(),
            "updatedAt": security.now_utc(),
            "agreement_number": new_purchase_agreement_counter['final_counter'] if new_purchase_agreement_counter[
                'success'] else None,
        })

        result = await all_trades_purchase_agreement_items_collection.insert_one(purchase_agreement_item_dict)
        if not result.inserted_id:
            raise HTTPException(status_code=500, detail="Failed to insert sales agreement item")

        purchase_agreement_item_dict.update({
            "_id": str(result.inserted_id),
            "company_id": str(purchase_agreement_item_dict["company_id"]),
            "trade_id": str(purchase_agreement_item_dict["trade_id"]),
        })

        encoded_data = jsonable_encoder(purchase_agreement_item_dict)
        await manager.send_to_company(str(company_id), {
            "type": "purchase_agreement_item_created",
            "data": encoded_data
        })


    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.patch("/update_purchase_agreement_item/{purchase_item_id}")
async def update_purchase_agreement_item(purchase_item_id: str, purchase_agreement_item: PurchaseAgreementModel,
                                         data: dict = Depends(security.get_current_user)):
    try:
        company_id = data.get('company_id')
        purchase_item_id = ObjectId(purchase_item_id)
        purchase_agreement_item_details_pipeline = [
            {
                '$match': {
                    '_id': purchase_item_id
                }
            }, {
                '$addFields': {
                    '_id': {
                        '$toString': '$_id'
                    },
                    'trade_id': {
                        '$toString': '$trade_id'
                    },
                    'company_id': {
                        '$toString': '$company_id'
                    }
                }
            }
        ]
        purchase_agreement_item_dict = purchase_agreement_item.model_dump(exclude_unset=True)
        purchase_agreement_item_dict.pop("trade_id")

        purchase_agreement_item_dict.update({
            "updatedAt": security.now_utc(),
        })

        result = await all_trades_purchase_agreement_items_collection.update_one({"_id": purchase_item_id},
                                                                                 {"$set": purchase_agreement_item_dict})

        if result.modified_count == 0:
            raise HTTPException(status_code=404, detail="Purchase Agreement Item not found")

        cursor = await all_trades_purchase_agreement_items_collection.aggregate(
            purchase_agreement_item_details_pipeline)
        result = await cursor.next()

        encoded_data = jsonable_encoder(result)
        await manager.send_to_company(company_id, {
            "type": "purchase_agreement_item_updated",
            "data": encoded_data
        })


    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/delete_purchase_agreement_item/{purchase_id}")
async def delete_purchase_agreement_item(purchase_id: str, data: dict = Depends(security.get_current_user)):
    try:
        company_id = data.get('company_id')
        result = await all_trades_purchase_agreement_items_collection.delete_one({"_id": ObjectId(purchase_id)})
        if result.deleted_count == 1:
            await manager.send_to_company(company_id, {
                "type": "purchase_agreement_item_deleted",
                "data": {"_id": purchase_id}
            })
            return {"message": "Purchase Agreement Item removed successfully!"}
        else:
            raise HTTPException(status_code=404, detail="item not found")

    except Exception as error:
        return {"message": str(error)}


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
            "account_name": ObjectId(capital.account_name) if capital.account_name else None,
            "receive": capital.receive,
            "comment": capital.comment,
            "date": capital.date,
            "createdAt": security.now_utc(),
            "updatedAt": security.now_utc(),
        }
        if add_type == "capitals":
            result = await all_capitals_collection.insert_one(capital_dict)
        elif add_type == "outstanding":
            result = await all_outstanding_collection.insert_one(capital_dict)
        else:
            raise HTTPException(status_code=400, detail="Invalid add_type.")
        new_capital_or_outstanding = await get_capital_or_outstanding_details(result.inserted_id, add_type)
        serialized = serialize(new_capital_or_outstanding)
        await manager.send_to_company(str(company_id), {
            "type": "capital_created" if add_type == "capitals" else "outstanding_created",
            "data": serialized
        })
        # return {"message": "Capital created successfully!", "capital": serialized}


    except Exception as error:
        raise error


@router.delete("/delete_capital_or_outstanding/{type_name}/{type_id}")
async def delete_capital_or_outstanding(type_name: str, type_id: str, data: dict = Depends(security.get_current_user)):
    try:
        company_id = data.get("company_id")
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
        await manager.send_to_company(company_id, {
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

        if "name" in update_data and update_data["name"] is not None and update_data["name"] != '':
            try:
                update_data["name"] = ObjectId(update_data["name"])
            except Exception:
                raise HTTPException(status_code=400, detail="Invalid name id, must be a valid ObjectId")
        if "account_name" in update_data and update_data["account_name"] is not None and update_data[
            "account_name"] != '':
            try:
                update_data["account_name"] = ObjectId(update_data["account_name"])
            except Exception:
                raise HTTPException(status_code=400, detail="Invalid account_name id, must be a valid ObjectId")

        update_data["updatedAt"] = security.now_utc()

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
                "item": {"$ifNull": ["$items.name", ""]},
                "item_id": {"$ifNull": ["$items._id", ""]},
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
                    "item": {"$ifNull": ["$items.name", ""]},
                    "item_id": {"$ifNull": ["$items._id", ""]},
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
        cursor = await all_general_expenses_collection.aggregate(pipeline)
        result = await cursor.to_list(length=1)
        return result[0]

    except Exception as e:
        raise e


@router.post("/get_general_expenses_summary")
async def get_general_expenses_summary(filter_expenses: ExpensesSearchModel,
                                       data: dict = Depends(security.get_current_user)):
    company_id = ObjectId(data.get("company_id"))

    expenses_search_pipeline = []
    expenses_search_pipeline.insert(0, {'$match': {'company_id': company_id}})

    now = security.now_utc()
    date_field = "date"
    date_filter = {}
    if filter_expenses.today:
        start = datetime(now.year, now.month, now.day, tzinfo=timezone.utc)
        end = start + timedelta(days=1)
        date_filter[date_field] = {"$gte": start, "$lt": end}

    elif filter_expenses.this_month:
        start = datetime(now.year, now.month, 1, tzinfo=timezone.utc)
        end = datetime(now.year + (now.month // 12), ((now.month % 12) + 1), 1)
        date_filter[date_field] = {"$gte": start, "$lt": end}

    elif filter_expenses.this_year:
        start = datetime(now.year, 1, 1, tzinfo=timezone.utc)
        end = datetime(now.year + 1, 1, 1)
        date_filter[date_field] = {"$gte": start, "$lt": end}

    elif filter_expenses.from_date or filter_expenses.to_date:
        date_filter[date_field] = {}
        if filter_expenses.from_date:
            date_filter[date_field]["$gte"] = filter_expenses.from_date
        if filter_expenses.to_date:
            date_filter[date_field]["$lte"] = filter_expenses.to_date

    if date_filter:
        expenses_search_pipeline.append({"$match": date_filter})

    expenses_search_pipeline.append(
        {
            "$group": {
                "_id": None,
                "total_pay": {"$sum": {"$ifNull": ["$pay", 0]}},
                "total_receive": {"$sum": {"$ifNull": ["$receive", 0]}},
                "count": {"$sum": 1}  # count all documents
            }
        },
    )
    expenses_search_pipeline.append(
        {
            "$addFields": {
                "total_net": {"$subtract": ["$total_receive", "$total_pay"]}
            }
        }
    )

    expenses_search_pipeline.append(
        {
            '$lookup': {
                'from': 'all_trades',
                'let': {
                    'companyId': company_id
                },
                'pipeline': [
                    {
                        '$match': {
                            '$expr': {
                                '$and': [
                                    {
                                        '$eq': [
                                            '$company_id', '$$companyId'
                                        ]
                                    }, {
                                        '$eq': [
                                            '$status', 'Sold'
                                        ]
                                    }
                                ]
                            }
                        }
                    }, {
                        '$lookup': {
                            'from': 'all_trades_items',
                            'let': {
                                'trade_id': '$_id'
                            },
                            'pipeline': [
                                {
                                    '$match': {
                                        '$expr': {
                                            '$eq': [
                                                '$trade_id', '$$trade_id'
                                            ]
                                        }
                                    }
                                }, {
                                    '$lookup': {
                                        'from': 'all_lists_values',
                                        'let': {
                                            'item_id': '$item'
                                        },
                                        'pipeline': [
                                            {
                                                '$match': {
                                                    '$expr': {
                                                        '$eq': [
                                                            '$_id', '$$item_id'
                                                        ]
                                                    }
                                                }
                                            }, {
                                                '$project': {
                                                    'name': 1
                                                }
                                            }
                                        ],
                                        'as': 'item_detail'
                                    }
                                }, {
                                    '$unwind': {
                                        'path': '$item_detail',
                                        'preserveNullAndEmptyArrays': True
                                    }
                                }, {
                                    '$addFields': {
                                        'buy_date_tmp': {
                                            '$cond': [
                                                {
                                                    '$eq': [
                                                        '$item_detail.name', 'BUY'
                                                    ]
                                                }, '$date', None
                                            ]
                                        },
                                        'sell_date_tmp': {
                                            '$cond': [
                                                {
                                                    '$eq': [
                                                        '$item_detail.name', 'SELL'
                                                    ]
                                                }, '$date', None
                                            ]
                                        }
                                    }
                                }
                            ],
                            'as': 'trade_items'
                        }
                    },
                    {
                        '$match': {
                            '$expr': {
                                '$or': [
                                    # If no date filter was created, allow all trades (True)
                                    {'$eq': [len(date_filter), 0]},
                                    # If date filter exists, check if any item in trade_items matches the range
                                    {
                                        '$gt': [
                                            {
                                                '$size': {
                                                    '$filter': {
                                                        'input': '$trade_items',
                                                        'as': 'item',
                                                        'cond': {
                                                            '$and': [
                                                                {'$ne': ['$$item.sell_date_tmp', None]},
                                                                {'$gte': ['$$item.sell_date_tmp',
                                                                          date_filter.get("date", {}).get("$gte",
                                                                                                          datetime(1, 1,
                                                                                                                   1,
                                                                                                                   tzinfo=timezone.utc))]},
                                                                {'$lt': ['$$item.sell_date_tmp',
                                                                         date_filter.get("date", {}).get("$lt",
                                                                                                         datetime(9999,
                                                                                                                  12,
                                                                                                                  31,
                                                                                                                  tzinfo=timezone.utc)) if "$lt" in date_filter.get(
                                                                             "date", {}) else datetime(9999, 12, 31,
                                                                                                       tzinfo=timezone.utc)]},
                                                                # Handle $lte if you used it in from_date/to_date
                                                                {'$lte': ['$$item.sell_date_tmp',
                                                                          date_filter.get("date", {}).get("$lte",
                                                                                                          datetime(9999,
                                                                                                                   12,
                                                                                                                   31,
                                                                                                                   tzinfo=timezone.utc))]}
                                                            ]
                                                        }
                                                    }
                                                }
                                            }, 0
                                        ]
                                    }
                                ]
                            }
                        }
                    },
                    {
                        '$unwind': '$trade_items'
                    }, {
                        '$group': {
                            '_id': None,
                            'total_trades_pay': {
                                '$sum': {
                                    '$ifNull': [
                                        '$trade_items.pay', 0
                                    ]
                                }
                            },
                            'total_trades_receive': {
                                '$sum': {
                                    '$ifNull': [
                                        '$trade_items.receive', 0
                                    ]
                                }
                            }
                        }
                    }, {
                        '$addFields': {
                            'total_trades_net': {
                                '$subtract': [
                                    '$total_trades_receive', '$total_trades_pay'
                                ]
                            }
                        }
                    }
                ],
                'as': 'trades'
            }
        }
    )

    expenses_search_pipeline.append({
        '$addFields': {
            'total_trades_net': {
                '$ifNull': [
                    {
                        '$arrayElemAt': [
                            '$trades.total_trades_net', 0
                        ]
                    }, 0
                ]
            }
        }
    })
    expenses_search_pipeline.append({
        '$addFields': {
            'net_profit': {
                '$add': [
                    '$total_trades_net', '$total_net'
                ]
            }
        }
    })

    expenses_search_pipeline.append({
        '$project': {
            'trades': 0,
            'total_trades_net': 0
        }
    })

    cursor = await all_general_expenses_collection.aggregate(expenses_search_pipeline)
    result = await cursor.to_list(None)

    summary = result[0] if result else {
        "total_pay": 0,
        "total_receive": 0,
        "total_net": 0,
        "count": 0,
        "net_profit": 0
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
            "account_name": ObjectId(general.account_name) if general.account_name else None,
            "comment": general.comment,
            "date": general.date,
            "createdAt": security.now_utc(),
            "updatedAt": security.now_utc()
        }

        result = await all_general_expenses_collection.insert_one(capital_dict)

        new_capital_or_outstanding = await get_general_expenses_details(result.inserted_id)
        serialized = general_expenses_serialize(new_capital_or_outstanding)
        await manager.send_to_company(str(company_id), {
            "type": "general_expenses_created",
            "data": serialized
        })


    except Exception as error:
        raise error


@router.delete("/delete_general_expenses/{type_id}")
async def delete_general_expenses(type_id: str, data: dict = Depends(security.get_current_user)):
    try:
        company_id = data.get("company_id")
        result = await all_general_expenses_collection.find_one_and_delete({"_id": ObjectId(type_id)})
        if not result:
            raise HTTPException(status_code=404, detail="General expenses not found.")
        totals = {
            "pay": result.get("pay", 0),
            "receive": result.get("receive", 0),
        }
        await manager.send_to_company(company_id, {
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

        if "item" in update_data and update_data["item"] is not None and update_data["item"] != '':
            try:
                update_data["item"] = ObjectId(update_data["item"])
            except Exception:
                raise HTTPException(status_code=400, detail="Invalid item id, must be a valid ObjectId")
        if "account_name" in update_data and update_data["account_name"] is not None and update_data[
            "account_name"] != '':
            try:
                update_data["account_name"] = ObjectId(update_data["account_name"])
            except Exception:
                raise HTTPException(status_code=400, detail="Invalid account_name id, must be a valid ObjectId")

        update_data["updatedAt"] = security.now_utc()

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

        await manager.send_to_company(str(company_id), {
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
    try:
        company_id = ObjectId(data.get("company_id"))
        trade_dict = {
            "company_id": company_id if company_id else "",
            "date": trade.date,
            "warranty_end_date": trade.warranty_end_date,
            "service_contract_end_date": trade.service_contract_end_date,
            "mileage": trade.mileage,
            "color_in": trade.color_in if trade.color_in else "",
            "color_out": trade.color_out if trade.color_out else "",
            "car_brand": trade.car_brand if trade.car_brand else "",
            "car_model": trade.car_model if trade.car_model else "",
            "specification": trade.specification if trade.specification else "",
            "engine_size": trade.engine_size if trade.engine_size else "",
            "year": trade.year if trade.year else "",
            "vin": trade.vin if trade.vin else "",
            "status": "New",
            "bought_from": trade.bought_from if trade.bought_from else "",
            "sold_to": trade.sold_to if trade.sold_to else "",
            "note": trade.note,
            "bought_by": trade.bought_by if trade.bought_by else "",
            "sold_by": trade.sold_by if trade.sold_by else "",
            "createdAt": security.now_utc(),
            "updatedAt": security.now_utc(),
        }

        result = await all_trades_collection.insert_one(trade_dict)
        if not result.inserted_id:
            raise HTTPException(status_code=500, detail="Failed to insert trade")

        return {"message": "Trade added successfully", "trade_id": str(result.inserted_id)}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")


async def get_trade_item_details(item_id: ObjectId):
    item_details_pipeline: Any = copy.deepcopy(trade_item_details_pipeline)
    item_details_pipeline.insert(0, {"$match": {"_id": ObjectId(item_id)}})
    cursor = await all_trades_items_collection.aggregate(item_details_pipeline)
    result = await cursor.to_list(length=1)
    return result[0] if result else None


@router.post("/add_trade_item")
async def add_trade_item(item_model: CarTradingItemsModel, data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        item_model = item_model.model_dump(exclude_unset=True)
        item_model.update({
            "item": ObjectId(item_model['item']) if item_model['item'] else "",
            "trade_id": ObjectId(item_model['trade_id']) if item_model['trade_id'] else "",
            "account_name": ObjectId(item_model['account_name']) if item_model['account_name'] else "",
            "company_id": company_id if company_id else None,
            "createdAt": security.now_utc(),
            "updatedAt": security.now_utc()
        })
        result = await all_trades_items_collection.insert_one(item_model)
        added_item = await get_trade_item_details(result.inserted_id)
        encoded_data = jsonable_encoder(added_item)

        await manager.send_to_company(str(company_id), {
            "type": "trade_item_added",
            "data": encoded_data
        })

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")


@router.patch("/update_trade_item/{item_id}")
async def update_trade_item(item_id: str, item_model: CarTradingItemsModel,
                            data: dict = Depends(security.get_current_user)):
    try:
        company_id = data.get("company_id")
        item_id = ObjectId(item_id)
        item_model = item_model.model_dump(exclude_unset=True)
        item_model.pop("trade_id")
        item_model.update({
            "item": ObjectId(item_model['item']) if item_model['item'] else "",
            "account_name": ObjectId(item_model['account_name']) if item_model['account_name'] else "",
            "updatedAt": security.now_utc()
        })
        await all_trades_items_collection.update_one({"_id": item_id}, {"$set": item_model})
        added_item = await get_trade_item_details(item_id)
        encoded_data = jsonable_encoder(added_item)

        await manager.send_to_company(company_id, {
            "type": "trade_item_updated",
            "data": encoded_data
        })

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")


@router.delete("/delete_trade_item/{item_id}")
async def delete_trade_item(item_id: str, data: dict = Depends(security.get_current_user)):
    try:
        company_id = data.get("company_id")
        item_id = ObjectId(item_id)
        await all_trades_items_collection.delete_one({"_id": item_id})
        await manager.send_to_company(company_id, {
            "type": "trade_item_deleted",
            "data": {"_id": str(item_id)}
        })

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")


@router.patch("/update_trade/{trade_id}")
async def update_trade(trade_id: str, trade: CarTradingModel,
                       _: dict = Depends(security.get_current_user)):
    try:
        updated_trade = trade.model_dump(exclude_unset=True)

        updated_trade["updatedAt"] = security.now_utc()
        await all_trades_collection.update_one({"_id": ObjectId(trade_id)}, {"$set": updated_trade})

        return {"message": "Trade updated successfully", "trade_id": trade_id}
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
        if filter_trades.bought_by:
            match_stage["bought_by"] = filter_trades.bought_by
        if filter_trades.sold_by:
            match_stage["sold_by"] = filter_trades.sold_by
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
            ("sold_by", "all_lists_values"),
            ("bought_by", "all_lists_values"),
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

        pipeline.append({
            "$lookup": {
                "from": "all_lists_values",
                "let": {"account_name_id": "$trade_items.account_name"},
                "pipeline": [
                    {"$match": {"$expr": {"$eq": ["$_id", "$$account_name_id"]}}},
                    {"$project": {"name": 1}}
                ],
                "as": "account_name_detail"
            }
        })
        pipeline.append({"$unwind": {"path": "$account_name_detail", "preserveNullAndEmptyArrays": True}})

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
                "warranty_end_date": {"$first": "$warranty_end_date"},
                "service_contract_end_date": {"$first": "$service_contract_end_date"},
                "note": {"$first": "$note"},
                "status": {"$first": "$status"},
                "mileage": {"$first": "$mileage"},
                "car_brand": {"$first": "$car_brand"},
                "car_model": {"$first": "$car_model"},
                "car_year": {"$first": "$year"},
                "vin": {"$first": "$vin"},
                "car_color_in": {"$first": "$color_in"},
                "car_color_out": {"$first": "$color_out"},
                "car_specification": {"$first": "$specification"},
                "car_engine_size": {"$first": "$engine_size"},
                "car_bought_from": {"$first": "$bought_from"},
                "car_sold_to": {"$first": "$sold_to"},
                "car_sold_by": {"$first": "$sold_by"},
                "car_bought_by": {"$first": "$bought_by"},
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
                                "account_name": "$account_name_detail.name",
                                "account_name_id": "$account_name_detail._id",
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

        pipeline.append({
            "$addFields": {
                "date_field_to_filter": {
                    "$cond": [
                        {"$eq": ["$status", "Sold"]},
                        "$sell_date",
                        "$buy_date"
                    ]
                }
            }
        })

        # -------------------------------
        # Date filtering after group
        # -------------------------------
        now = security.now_utc()
        date_field = "date_field_to_filter"
        if filter_trades.status and filter_trades.status.lower() == "sold":
            date_field = "sell_date"
        elif filter_trades.status and filter_trades.status.lower() == "buy":
            date_field = "buy_date"

        date_filter = {}
        if filter_trades.today:
            start = datetime(now.year, now.month, now.day, tzinfo=timezone.utc)
            end = start + timedelta(days=1)
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
                date_filter[date_field]["$gte"] = filter_trades.from_date
            if filter_trades.to_date:
                date_filter[date_field]["$lte"] = filter_trades.to_date

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
                "vin": {"$ifNull": ["$vin", ""]},
                "bought_from_id": {"$ifNull": ["$car_bought_from._id", ""]},
                "bought_from": {"$ifNull": ["$car_bought_from.name", ""]},
                "sold_to_id": {"$ifNull": ["$car_sold_to._id", ""]},
                "sold_to": {"$ifNull": ["$car_sold_to.name", ""]},
                "sold_by": {"$ifNull": ["$car_sold_by.name", ""]},
                "bought_by": {"$ifNull": ["$car_bought_by.name", ""]},
                "sold_by_id": {"$ifNull": ["$car_sold_by._id", ""]},
                "bought_by_id": {"$ifNull": ["$car_bought_by._id", ""]},
                "note": {"$ifNull": ["$note", ""]},
                "date": {"$ifNull": ["$date", ""]},
                "warranty_end_date": {"$ifNull": ["$warranty_end_date", ""]},
                "service_contract_end_date": {"$ifNull": ["$service_contract_end_date", ""]},
                "trade_items": {
                    "$sortArray": {
                        "input": {"$ifNull": ["$trade_items", []]},
                        "sortBy": {"date": 1}  # 1 = ASC, -1 = DESC
                    }
                },
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
            await all_trades_purchase_agreement_items_collection.delete_many(
                {"trade_id": ObjectId(trade_id)}, session=session
            )
            await session.commit_transaction()  # commit the transaction

            if result1.deleted_count == 0:
                raise HTTPException(status_code=404, detail="Trade not found")

        return {"message": "Trade and its items deleted successfully"}

    except PyMongoError as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")


@router.get("/get_cash_on_hand_or_bank_balance")
async def get_cash_on_hand_or_bank_balance(data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        cash_on_hand_pipeline = [
            {
                '$match': {
                    'company_id': company_id
                }
            }, {
                '$lookup': {
                    'from': 'all_trades_items',
                    'localField': '_id',
                    'foreignField': 'trade_id',
                    'as': 'item'
                }
            }, {
                '$unwind': '$item'
            }, {
                '$project': {
                    'account_id': '$item.account_name',
                    'cars_pay': {
                        '$ifNull': [
                            '$item.pay', 0
                        ]
                    },
                    'cars_receive': {
                        '$ifNull': [
                            '$item.receive', 0
                        ]
                    },
                    'capitals_pay': {
                        '$literal': 0
                    },
                    'capitals_receive': {
                        '$literal': 0
                    },
                    'outstanding_pay': {
                        '$literal': 0
                    },
                    'outstanding_receive': {
                        '$literal': 0
                    },
                    'expenses_pay': {
                        '$literal': 0
                    },
                    'expenses_receive': {
                        '$literal': 0
                    }
                }
            }, {
                '$unionWith': {
                    'coll': 'all_capitals',
                    'pipeline': [
                        {
                            '$match': {
                                'company_id': company_id
                            }
                        }, {
                            '$project': {
                                'account_id': '$account_name',
                                'cars_pay': {
                                    '$literal': 0
                                },
                                'cars_receive': {
                                    '$literal': 0
                                },
                                'capitals_pay': {
                                    '$ifNull': [
                                        '$pay', 0
                                    ]
                                },
                                'capitals_receive': {
                                    '$ifNull': [
                                        '$receive', 0
                                    ]
                                },
                                'outstanding_pay': {
                                    '$literal': 0
                                },
                                'outstanding_receive': {
                                    '$literal': 0
                                },
                                'expenses_pay': {
                                    '$literal': 0
                                },
                                'expenses_receive': {
                                    '$literal': 0
                                }
                            }
                        }
                    ]
                }
            }, {
                '$unionWith': {
                    'coll': 'all_outstanding',
                    'pipeline': [
                        {
                            '$match': {
                                'company_id': company_id
                            }
                        }, {
                            '$project': {
                                'account_id': '$account_name',
                                'cars_pay': {
                                    '$literal': 0
                                },
                                'cars_receive': {
                                    '$literal': 0
                                },
                                'capitals_pay': {
                                    '$literal': 0
                                },
                                'capitals_receive': {
                                    '$literal': 0
                                },
                                'outstanding_pay': {
                                    '$ifNull': [
                                        '$pay', 0
                                    ]
                                },
                                'outstanding_receive': {
                                    '$ifNull': [
                                        '$receive', 0
                                    ]
                                },
                                'expenses_pay': {
                                    '$literal': 0
                                },
                                'expenses_receive': {
                                    '$literal': 0
                                }
                            }
                        }
                    ]
                }
            }, {
                '$unionWith': {
                    'coll': 'all_general_expenses',
                    'pipeline': [
                        {
                            '$match': {
                                'company_id': company_id
                            }
                        }, {
                            '$project': {
                                'account_id': '$account_name',
                                'cars_pay': {
                                    '$literal': 0
                                },
                                'cars_receive': {
                                    '$literal': 0
                                },
                                'capitals_pay': {
                                    '$literal': 0
                                },
                                'capitals_receive': {
                                    '$literal': 0
                                },
                                'outstanding_pay': {
                                    '$literal': 0
                                },
                                'outstanding_receive': {
                                    '$literal': 0
                                },
                                'expenses_pay': {
                                    '$ifNull': [
                                        '$pay', 0
                                    ]
                                },
                                'expenses_receive': {
                                    '$ifNull': [
                                        '$receive', 0
                                    ]
                                }
                            }
                        }
                    ]
                }
            }, {
                '$unionWith': {
                    'coll': 'all_trades_transfers',
                    'pipeline': [
                        {
                            '$match': {
                                'company_id': company_id
                            }
                        }, {
                            '$project': {
                                'account_id': '$from_account',
                                'cars_pay': {
                                    '$literal': 0
                                },
                                'cars_receive': {
                                    '$literal': 0
                                },
                                'capitals_pay': {
                                    '$literal': 0
                                },
                                'capitals_receive': {
                                    '$literal': 0
                                },
                                'outstanding_pay': {
                                    '$literal': 0
                                },
                                'outstanding_receive': {
                                    '$literal': 0
                                },
                                'expenses_pay': {
                                    '$literal': 0
                                },
                                'expenses_receive': {
                                    '$literal': 0
                                },
                                'transfers_net': {
                                    '$multiply': [
                                        {
                                            '$ifNull': [
                                                '$amount', 0
                                            ]
                                        }, -1
                                    ]
                                }
                            }
                        }
                    ]
                }
            }, {
                '$unionWith': {
                    'coll': 'all_trades_transfers',
                    'pipeline': [
                        {
                            '$match': {
                                'company_id': company_id
                            }
                        }, {
                            '$project': {
                                'account_id': '$to_account',
                                'cars_pay': {
                                    '$literal': 0
                                },
                                'cars_receive': {
                                    '$literal': 0
                                },
                                'capitals_pay': {
                                    '$literal': 0
                                },
                                'capitals_receive': {
                                    '$literal': 0
                                },
                                'outstanding_pay': {
                                    '$literal': 0
                                },
                                'outstanding_receive': {
                                    '$literal': 0
                                },
                                'expenses_pay': {
                                    '$literal': 0
                                },
                                'expenses_receive': {
                                    '$literal': 0
                                },
                                'transfers_net': {
                                    '$ifNull': [
                                        '$amount', 0
                                    ]
                                }
                            }
                        }
                    ]
                }
            }, {
                '$group': {
                    '_id': '$account_id',
                    'cars_pay': {
                        '$sum': '$cars_pay'
                    },
                    'cars_receive': {
                        '$sum': '$cars_receive'
                    },
                    'capitals_pay': {
                        '$sum': '$capitals_pay'
                    },
                    'capitals_receive': {
                        '$sum': '$capitals_receive'
                    },
                    'outstanding_pay': {
                        '$sum': '$outstanding_pay'
                    },
                    'outstanding_receive': {
                        '$sum': '$outstanding_receive'
                    },
                    'expenses_pay': {
                        '$sum': '$expenses_pay'
                    },
                    'expenses_receive': {
                        '$sum': '$expenses_receive'
                    },
                    'transfers_net': {
                        '$sum': {
                            '$ifNull': [
                                '$transfers_net', 0
                            ]
                        }
                    }
                }
            }, {
                '$addFields': {
                    'total_cars_net': {
                        '$subtract': [
                            '$cars_receive', '$cars_pay'
                        ]
                    },
                    'total_capitals_net': {
                        '$subtract': [
                            '$capitals_receive', '$capitals_pay'
                        ]
                    },
                    'total_outstanding_net': {
                        '$subtract': [
                            '$outstanding_receive', '$outstanding_pay'
                        ]
                    },
                    'total_expenses_net': {
                        '$subtract': [
                            '$expenses_receive', '$expenses_pay'
                        ]
                    }
                }
            }, {
                '$addFields': {
                    'final_net': {
                        '$add': [
                            '$total_cars_net', '$total_capitals_net', '$total_outstanding_net', '$total_expenses_net',
                            "$transfers_net"
                        ]
                    }
                }
            }, {
                '$lookup': {
                    'from': 'all_lists_values',
                    'localField': '_id',
                    'foreignField': '_id',
                    'as': 'account'
                }
            }, {
                '$addFields': {
                    'account_name': {
                        '$ifNull': [
                            {
                                '$arrayElemAt': [
                                    '$account.name', 0
                                ]
                            }, 'Unknown'
                        ]
                    }
                }
            }, {
                '$project': {
                    '_id': 0,
                    'account_id': {
                        '$toString': '$_id'
                    },
                    'account_name': 1,
                    'total_cars_net': 1,
                    'total_capitals_net': 1,
                    'total_outstanding_net': 1,
                    'total_expenses_net': 1,
                    'final_net': 1
                }
            }, {
                '$sort': {
                    'account_name': 1
                }
            }, {
                '$addFields': {
                    'account_display': {
                        '$concat': [
                            {
                                '$switch': {
                                    'branches': [
                                        {
                                            'case': {
                                                '$regexMatch': {
                                                    'input': '$account_name',
                                                    'regex': 'cash',
                                                    'options': 'i'
                                                }
                                            },
                                            'then': '💵 '
                                        },
                                        {
                                            'case': {
                                                '$regexMatch': {
                                                    'input': '$account_name',
                                                    'regex': 'bank',
                                                    'options': 'i'
                                                }
                                            },
                                            'then': '🏦 '
                                        },
                                        {
                                            'case': {
                                                '$regexMatch': {
                                                    'input': '$account_name',
                                                    'regex': 'expense',
                                                    'options': 'i'
                                                }
                                            },
                                            'then': '🧾 '
                                        }
                                    ],
                                    'default': '📁 '
                                }
                            },
                            '$account_name'
                        ]
                    }
                }
            }
            , {
                '$group': {
                    '_id': None,
                    'all_accounts': {
                        '$push': {
                            'account_name': '$account_name',
                            'account_display': '$account_display',
                            'final_net': '$final_net'
                        }
                    },
                    'total_final_net': {
                        '$sum': '$final_net'
                    }
                }
            }, {
                '$project': {
                    '_id': 0,
                    'all_accounts': 1,
                    'total_final_net': 1
                }
            }
        ]
        cursor = await all_trades_collection.aggregate(cash_on_hand_pipeline)
        result = await cursor.next()
        return {"totals": result}

    except PyMongoError as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")


@router.post("/get_last_changes")
async def get_last_changes(data_filter: LastChangesFilter, data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        from datetime import datetime

        from_date = datetime.strptime('2000-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')
        to_date = datetime.strptime('2100-12-31 00:00:00', '%Y-%m-%d %H:%M:%S')
        account = None
        if data_filter.account is not None:
            account = data_filter.account

        if data_filter.from_date:
            from_date = data_filter.from_date
        if data_filter.to_date:
            to_date = data_filter.to_date
        amount_filter = {}
        if data_filter.min_amount is not None:
            amount_filter['$gte'] = data_filter.min_amount
        if data_filter.max_amount is not None:
            amount_filter['$lte'] = data_filter.max_amount

        last_changes_pipeline = [
            {
                '$match': {
                    'company_id': company_id
                }
            }, {
                '$lookup': {
                    'from': 'all_trades_items',
                    'let': {
                        'trade_id': '$_id'
                    },
                    'pipeline': [
                        {
                            '$match': {
                                '$expr': {
                                    '$eq': [
                                        '$trade_id', '$$trade_id'
                                    ]
                                }
                            }
                        }, {
                            '$lookup': {
                                'from': 'all_lists_values',
                                'localField': 'item',
                                'foreignField': '_id',
                                'as': 'item_details'
                            }
                        }, {
                            '$addFields': {
                                'item_name': {
                                    '$ifNull': [
                                        {
                                            '$arrayElemAt': [
                                                '$item_details.name', 0
                                            ]
                                        }, None
                                    ]
                                }
                            }
                        }, {
                            '$lookup': {
                                'from': 'all_lists_values',
                                'localField': 'account_name',
                                'foreignField': '_id',
                                'as': 'account_name_details'
                            }
                        }, {
                            '$addFields': {
                                'account_name_name': {
                                    '$ifNull': [
                                        {
                                            '$arrayElemAt': [
                                                '$account_name_details.name', 0
                                            ]
                                        }, None
                                    ]
                                }
                            }
                        }
                    ],
                    'as': 'trade_items'
                }
            }, {
                '$unwind': {
                    'path': '$trade_items',
                    'preserveNullAndEmptyArrays': True
                }
            }, {
                '$addFields': {
                    'updatedAt': {
                        '$cond': [
                            {
                                '$gt': [
                                    '$trade_items.updatedAt', '$updatedAt'
                                ]
                            }, '$trade_items.updatedAt', '$updatedAt'
                        ]
                    }
                }
            }, {
                '$match': {
                    'updatedAt': {
                        '$gte': from_date,
                        '$lte': to_date
                    }
                }
            }, {
                '$lookup': {
                    'from': 'all_brands',
                    'localField': 'car_brand',
                    'foreignField': '_id',
                    'as': 'brand_details'
                }
            }, {
                '$lookup': {
                    'from': 'all_brand_models',
                    'localField': 'car_model',
                    'foreignField': '_id',
                    'as': 'model_details'
                }
            }, {
                '$lookup': {
                    'from': 'all_lists_values',
                    'localField': 'year',
                    'foreignField': '_id',
                    'as': 'year_details'
                }
            }, {
                '$addFields': {
                    'brand_name': {
                        '$arrayElemAt': [
                            '$brand_details.name', 0
                        ]
                    },
                    'model_name': {
                        '$arrayElemAt': [
                            '$model_details.name', 0
                        ]
                    },
                    'description': {
                        '$ifNull': [
                            '$trade_items.comment', 0
                        ]
                    },
                    'pay': {
                        '$ifNull': [
                            '$trade_items.pay', 0
                        ]
                    },
                    'receive': {
                        '$ifNull': [
                            '$trade_items.receive', 0
                        ]
                    },
                    'year': {
                        '$arrayElemAt': [
                            '$year_details.name', 0
                        ]
                    },
                    'item_name': {
                        '$ifNull': [
                            '$trade_items.item_name', 0
                        ]
                    },
                    'account_name': {
                        '$ifNull': [
                            '$trade_items.account_name_name', 0
                        ]
                    }
                }
            }, {
                '$project': {
                    '_id': {
                        '$toString': '$_id'
                    },
                    'type': {
                        '$literal': 'car'
                    },
                    'brand_name': 1,
                    'model_name': 1,
                    'description': 1,
                    'pay': 1,
                    'receive': 1,
                    'year': 1,
                    'updatedAt': 1,
                    'item_name': 1,
                    'account_name': 1
                }
            }, {
                '$unionWith': {
                    'coll': 'all_general_expenses',
                    'pipeline': [
                        {
                            '$match': {
                                'company_id': company_id,
                                'updatedAt': {
                                    '$gte': from_date,
                                    '$lte': to_date
                                }
                            }
                        }, {
                            '$lookup': {
                                'from': 'all_lists_values',
                                'localField': 'item',
                                'foreignField': '_id',
                                'as': 'item_details'
                            }
                        }, {
                            '$lookup': {
                                'from': 'all_lists_values',
                                'localField': 'account_name',
                                'foreignField': '_id',
                                'as': 'account_name_details'
                            }
                        }, {
                            '$project': {
                                '_id': 0,
                                'type': {
                                    '$literal': 'expenses'
                                },
                                'brand_name': {
                                    '$literal': '-'
                                },
                                'model_name': {
                                    '$literal': '-'
                                },
                                'year': {
                                    '$literal': '-'
                                },
                                'description': '$comment',
                                'pay': '$pay',
                                'receive': '$receive',
                                'updatedAt': 1,
                                'item_name': {
                                    '$arrayElemAt': [
                                        '$item_details.name', 0
                                    ]
                                },
                                'account_name': {
                                    '$arrayElemAt': [
                                        '$account_name_details.name', 0
                                    ]
                                }
                            }
                        }
                    ]
                }
            }, {
                '$unionWith': {
                    'coll': 'all_outstanding',
                    'pipeline': [
                        {
                            '$match': {
                                'company_id': company_id,
                                'updatedAt': {
                                    '$gte': from_date,
                                    '$lte': to_date
                                }
                            }
                        }, {
                            '$lookup': {
                                'from': 'all_lists_values',
                                'localField': 'name',
                                'foreignField': '_id',
                                'as': 'item_details'
                            }
                        }, {
                            '$lookup': {
                                'from': 'all_lists_values',
                                'localField': 'account_name',
                                'foreignField': '_id',
                                'as': 'account_name_details'
                            }
                        }, {
                            '$project': {
                                '_id': 0,
                                'type': {
                                    '$literal': 'outstanding'
                                },
                                'brand_name': {
                                    '$literal': '-'
                                },
                                'model_name': {
                                    '$literal': '-'
                                },
                                'year': {
                                    '$literal': '-'
                                },
                                'description': '$comment',
                                'pay': '$pay',
                                'receive': '$receive',
                                'updatedAt': 1,
                                'item_name': {
                                    '$arrayElemAt': [
                                        '$item_details.name', 0
                                    ]
                                },
                                'account_name': {
                                    '$arrayElemAt': [
                                        '$account_name_details.name', 0
                                    ]
                                }
                            }
                        }
                    ]
                }
            }, {
                '$unionWith': {
                    'coll': 'all_capitals',
                    'pipeline': [
                        {
                            '$match': {
                                'company_id': company_id,
                                'updatedAt': {
                                    '$gte': from_date,
                                    '$lte': to_date
                                }
                            }
                        }, {
                            '$lookup': {
                                'from': 'all_lists_values',
                                'localField': 'name',
                                'foreignField': '_id',
                                'as': 'item_details'
                            }
                        }, {
                            '$lookup': {
                                'from': 'all_lists_values',
                                'localField': 'account_name',
                                'foreignField': '_id',
                                'as': 'account_name_details'
                            }
                        }, {
                            '$project': {
                                '_id': 0,
                                'type': {
                                    '$literal': 'capital'
                                },
                                'brand_name': {
                                    '$literal': '-'
                                },
                                'model_name': {
                                    '$literal': '-'
                                },
                                'year': {
                                    '$literal': '-'
                                },
                                'description': '$comment',
                                'pay': '$pay',
                                'receive': '$receive',
                                'updatedAt': 1,
                                'item_name': {
                                    '$arrayElemAt': [
                                        '$item_details.name', 0
                                    ]
                                },
                                'account_name': {
                                    '$arrayElemAt': [
                                        '$account_name_details.name', 0
                                    ]
                                }
                            }
                        }
                    ]
                }
            }, {
                '$unionWith': {
                    'coll': 'all_trades_transfers',
                    'pipeline': [
                        {
                            '$match': {
                                'company_id': company_id,
                                'updatedAt': {
                                    '$gte': from_date,
                                    '$lte': to_date
                                }
                            }
                        }, {
                            '$lookup': {
                                'from': 'all_lists_values',
                                'localField': 'from_account',
                                'foreignField': '_id',
                                'as': 'from_details'
                            }
                        }, {
                            '$lookup': {
                                'from': 'all_lists_values',
                                'localField': 'to_account',
                                'foreignField': '_id',
                                'as': 'to_details'
                            }
                        }, {
                            '$project': {
                                'entries': [
                                    {
                                        'type': 'transfer',
                                        'brand_name': '-',
                                        'model_name': '-',
                                        'year': '-',
                                        'description': '$comment',
                                        'pay': '$amount',
                                        'receive': 0,
                                        'updatedAt': "$updatedAt",
                                        'item_name': '-',
                                        'account_name': {
                                            '$first': '$from_details.name'
                                        }
                                    }, {
                                        'type': 'transfer',
                                        'brand_name': '-',
                                        'model_name': '-',
                                        'year': '-',
                                        'description': '$comment',
                                        'pay': 0,
                                        'receive': '$amount',
                                        'updatedAt': "$updatedAt",
                                        'item_name': '-',
                                        'account_name': {
                                            '$first': '$to_details.name'
                                        }
                                    }
                                ]
                            }
                        }, {
                            '$unwind': '$entries'
                        }, {
                            '$replaceRoot': {
                                'newRoot': '$entries'
                            }
                        }
                    ]
                }
            }, {
                '$sort': {
                    'updatedAt': -1
                }
            }
        ]
        match_map = {}
        if amount_filter:
            match_map['$or'] = [
                {'pay': amount_filter},
                {'receive': amount_filter}
            ],
        if account:
            match_map['account_name'] = account

        if match_map:
            last_changes_pipeline.append({"$match": match_map})

        cursor = await all_trades_collection.aggregate(last_changes_pipeline)
        results = await cursor.to_list(None)
        return {"last_changes": results}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

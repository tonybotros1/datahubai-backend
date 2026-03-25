import copy
from typing import Optional, Any
from bson import ObjectId
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from app.core import security
from app.database import get_collection
from datetime import datetime

from app.routes.car_trading import PyObjectId
from app.routes.counters import create_custom_counter
from app.websocket_config import manager
from fastapi.encoders import jsonable_encoder

router = APIRouter()
account_transfers_collection = get_collection("account_transfers")


class TransferModel(BaseModel):
    status: Optional[str] = None
    transfer_number: Optional[str] = None
    date: Optional[datetime] = None
    from_account: Optional[str] = None
    to_account: Optional[str] = None
    amount: Optional[float] = None
    comment: Optional[str] = None


class TransfersSearchModel(BaseModel):
    from_account: Optional[PyObjectId] = None
    to_account: Optional[PyObjectId] = None
    status: Optional[str] = None
    transfer_number: Optional[str] = None
    comment: Optional[str] = None
    amount: Optional[float] = None
    from_date: Optional[datetime] = None
    to_date: Optional[datetime] = None


transfer_pipeline = [
    {
        '$lookup': {
            'from': 'all_banks',
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
                        'account_number': 1
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
                    'in': '$$match.account_number'
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
                    'in': '$$match.account_number'
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


async def get_transfer_details(transfer_id: ObjectId):
    try:
        details_pipeline: Any = copy.deepcopy(transfer_pipeline)
        details_pipeline.insert(0, {"$match": {"_id": transfer_id}})

        cursor = await account_transfers_collection.aggregate(details_pipeline)
        results = await cursor.to_list(length=1)

        return results[0] if results else None

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/add_new_transfer")
async def add_new_transfer(transfer_data: TransferModel, data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        transfer_data = transfer_data.model_dump(exclude_unset=True)
        new_transfer_counter = await create_custom_counter("TN", "TN", data=data, description='Transfer Number', )

        transfer_data.update({
            "company_id": company_id,
            "status": "New",
            "from_account": ObjectId(transfer_data["from_account"]) if transfer_data["from_account"] else None,
            "to_account": ObjectId(transfer_data["to_account"]) if transfer_data["to_account"] else None,
            "transfer_number": new_transfer_counter['final_counter'] if new_transfer_counter['success'] else None,
            "createdAt": security.now_utc(),
            "updatedAt": security.now_utc(),
        })

        result = await account_transfers_collection.insert_one(transfer_data)
        if not result.inserted_id:
            raise HTTPException(status_code=500, detail="Failed to insert transfer item")

        added_transfer = await get_transfer_details(result.inserted_id)

        encoded_data = jsonable_encoder(added_transfer)
        return {"transfer": encoded_data}


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
            "status": transfer_data["status"] if transfer_data["status"] else None,
            "from_account": ObjectId(transfer_data["from_account"]) if transfer_data["from_account"] else None,
            "to_account": ObjectId(transfer_data["to_account"]) if transfer_data["to_account"] else None,
            "createdAt": security.now_utc(),
            "updatedAt": security.now_utc(),
        })

        await account_transfers_collection.update_one({"_id": transfer_id}, {"$set": transfer_data})

        added_transfer = await get_transfer_details(transfer_id)

        encoded_data = jsonable_encoder(added_transfer)
        await manager.send_to_company(company_id,{
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
        result = await account_transfers_collection.delete_one({"_id": transfer_id})
        if result.deleted_count == 0:
            raise HTTPException(status_code=404, detail="Transfer not found")

        await manager.send_to_company(company_id,{
            "type": "transfer_deleted",
            "data": {"_id": str(transfer_id)}
        })

    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/get_account_transfer_status/{transfer_id}")
async def get_account_transfer_status(transfer_id: str, _: dict = Depends(security.get_current_user)):
    try:
        if not ObjectId.is_valid(transfer_id):
            raise HTTPException(status_code=400, detail="Invalid transfer_id format")

        transfer_id = ObjectId(transfer_id)

        result = await account_transfers_collection.find_one(
            {"_id": transfer_id},
            {"_id": 0, "status": 1}
        )

        if not result:
            raise HTTPException(status_code=404, detail="Transfer not found")

        return {"status": "success", "data": result}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")


@router.post("/search_engine_for_account_transfers")
async def search_engine_for_account_transfers(filter_transfers: TransfersSearchModel,
                                              data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        match_stage = {}
        if company_id:
            match_stage['company_id'] = company_id
        if filter_transfers.status:
            match_stage['status'] = filter_transfers.status
        if filter_transfers.transfer_number:
            match_stage['transfer_number'] = filter_transfers.transfer_number
        if filter_transfers.from_date or filter_transfers.to_date:
            match_stage['date'] = {}
            if filter_transfers.from_date:
                match_stage['date']["$gte"] = filter_transfers.from_date
            if filter_transfers.to_date:
                match_stage['date']["$lte"] = filter_transfers.to_date

        if filter_transfers.from_account:
            match_stage["from_account"] = filter_transfers.from_account
        if filter_transfers.to_account:
            match_stage["to_account"] = filter_transfers.to_account
        if filter_transfers.comment:
            match_stage["comment"] = {
                "$regex": filter_transfers.comment,
                "$options": "i"
            }
        if filter_transfers.amount is not None:
            match_stage["amount"] = filter_transfers.amount

        search_transfer_pipeline = copy.deepcopy(transfer_pipeline)
        if match_stage:
            search_transfer_pipeline.insert(0, {"$match": match_stage})
        search_transfer_pipeline.insert(1, {"$sort": {'date': -1}})

        cursor = await account_transfers_collection.aggregate(search_transfer_pipeline)
        transfers = await cursor.to_list(None)
        transfers_count = await account_transfers_collection.count_documents(match_stage)
        return {
            "transfers": transfers if transfers else [],
            "count": transfers_count if transfers_count else 0,
        }


    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

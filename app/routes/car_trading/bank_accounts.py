import copy
from typing import Any, Optional

from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException
from fastapi.encoders import jsonable_encoder
from pymongo.errors import PyMongoError

from app.core import security
from app.websocket_config import manager

from .common import (
    all_trades_items_collection,
    all_trades_transfers_collection,
    parse_object_id,
    require_payload_field,
    zero_if_none,
)
from .models import TransferModel

router = APIRouter()


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


async def get_transfer_details(transfer_id: ObjectId, company_id: Optional[ObjectId] = None):
    try:
        details_pipeline: Any = copy.deepcopy(transfer_pipeline)
        match_stage = {"_id": transfer_id}
        if company_id is not None:
            match_stage["company_id"] = company_id
        details_pipeline.insert(0, {"$match": match_stage})

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
        from_account = parse_object_id(
            require_payload_field(transfer_data, "from_account", "From account"),
            "from_account",
        )
        to_account = parse_object_id(
            require_payload_field(transfer_data, "to_account", "To account"),
            "to_account",
        )
        if from_account == to_account:
            raise HTTPException(status_code=400, detail="From and to accounts must be different")
        require_payload_field(transfer_data, "date", "Date")

        transfer_data.update({
            "company_id": company_id,
            "from_account": from_account,
            "to_account": to_account,
            "amount": zero_if_none(transfer_data.get("amount")),
            "createdAt": security.now_utc(),
            "updatedAt": security.now_utc(),
        })

        result = await all_trades_transfers_collection.insert_one(transfer_data)
        if not result.inserted_id:
            raise HTTPException(status_code=500, detail="Failed to insert transfer item")

        added_transfer = await get_transfer_details(result.inserted_id, company_id)

        encoded_data = jsonable_encoder(added_transfer)
        await manager.send_to_company(str(company_id), {
            "type": "transfer_created",
            "data": encoded_data
        })
        return {"message": "Transfer added successfully", "data": encoded_data}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.patch("/update_new_transfer/{transfer_id}")
async def update_new_transfer(transfer_id: str, transfer_data: TransferModel,
                              data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        transfer_id = parse_object_id(transfer_id, "transfer_id")
        transfer_data = transfer_data.model_dump(exclude_unset=True)
        from_account = parse_object_id(
            require_payload_field(transfer_data, "from_account", "From account"),
            "from_account",
        )
        to_account = parse_object_id(
            require_payload_field(transfer_data, "to_account", "To account"),
            "to_account",
        )
        if from_account == to_account:
            raise HTTPException(status_code=400, detail="From and to accounts must be different")
        require_payload_field(transfer_data, "date", "Date")

        transfer_data.update({
            "from_account": from_account,
            "to_account": to_account,
            "amount": zero_if_none(transfer_data.get("amount")),
            "updatedAt": security.now_utc(),
        })

        result = await all_trades_transfers_collection.update_one(
            {"_id": transfer_id, "company_id": company_id},
            {"$set": transfer_data},
        )
        if result.matched_count == 0:
            raise HTTPException(status_code=404, detail="Transfer not found")

        added_transfer = await get_transfer_details(transfer_id, company_id)

        encoded_data = jsonable_encoder(added_transfer)
        await manager.send_to_company(str(company_id), {
            "type": "transfer_updated",
            "data": encoded_data
        })
        return {"message": "Transfer updated successfully", "data": encoded_data}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/delete_transfer/{transfer_id}")
async def delete_transfer(transfer_id: str, data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        transfer_id = parse_object_id(transfer_id, "transfer_id")
        result = await all_trades_transfers_collection.delete_one(
            {"_id": transfer_id, "company_id": company_id},
        )
        if result.deleted_count == 0:
            raise HTTPException(status_code=404, detail="Transfer not found")

        await manager.send_to_company(str(company_id), {
            "type": "transfer_deleted",
            "data": {"_id": str(transfer_id)}
        })
        return {"message": "Transfer deleted successfully"}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


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
                '$project': {
                    'account_id': '$account_name',
                    'total_cars_net': {
                        '$subtract': [
                            {
                                '$ifNull': [
                                    '$receive', 0
                                ]
                            }, {
                                '$ifNull': [
                                    '$pay', 0
                                ]
                            }
                        ]
                    },
                    'total_capitals_net': {
                        '$literal': 0
                    },
                    'total_outstanding_net': {
                        '$literal': 0
                    },
                    'total_expenses_net': {
                        '$literal': 0
                    },
                    'transfers_net': {
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
                                'total_cars_net': {
                                    '$literal': 0
                                },
                                'total_capitals_net': {
                                    '$subtract': [
                                        {
                                            '$ifNull': [
                                                '$receive', 0
                                            ]
                                        }, {
                                            '$ifNull': [
                                                '$pay', 0
                                            ]
                                        }
                                    ]
                                },
                                'total_outstanding_net': {
                                    '$literal': 0
                                },
                                'total_expenses_net': {
                                    '$literal': 0
                                },
                                'transfers_net': {
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
                                'total_cars_net': {
                                    '$literal': 0
                                },
                                'total_capitals_net': {
                                    '$literal': 0
                                },
                                'total_outstanding_net': {
                                    '$subtract': [
                                        {
                                            '$ifNull': [
                                                '$receive', 0
                                            ]
                                        }, {
                                            '$ifNull': [
                                                '$pay', 0
                                            ]
                                        }
                                    ]
                                },
                                'total_expenses_net': {
                                    '$literal': 0
                                },
                                'transfers_net': {
                                    '$literal': 0
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
                                'entries': [
                                    {
                                        'account_id': '$from_account',
                                        'transfers_net': {
                                            '$multiply': [
                                                {
                                                    '$ifNull': [
                                                        '$amount', 0
                                                    ]
                                                }, -1
                                            ]
                                        }
                                    }, {
                                        'account_id': '$to_account',
                                        'transfers_net': {
                                            '$ifNull': [
                                                '$amount', 0
                                            ]
                                        }
                                    }
                                ]
                            }
                        }, {
                            '$unwind': '$entries'
                        }, {
                            '$project': {
                                'account_id': '$entries.account_id',
                                'total_cars_net': {
                                    '$literal': 0
                                },
                                'total_capitals_net': {
                                    '$literal': 0
                                },
                                'total_outstanding_net': {
                                    '$literal': 0
                                },
                                'total_expenses_net': {
                                    '$literal': 0
                                },
                                'transfers_net': '$entries.transfers_net'
                            }
                        }
                    ]
                }
            }, {
                '$group': {
                    '_id': '$account_id',
                    'total_cars_net': {
                        '$sum': '$total_cars_net'
                    },
                    'total_capitals_net': {
                        '$sum': '$total_capitals_net'
                    },
                    'total_outstanding_net': {
                        '$sum': '$total_outstanding_net'
                    },
                    'total_expenses_net': {
                        '$sum': '$total_expenses_net'
                    },
                    'transfers_net': {
                        '$sum': '$transfers_net'
                    }
                }
            }, {
                '$set': {
                    'final_net': {
                        '$add': [
                            '$total_cars_net', '$total_capitals_net', '$total_outstanding_net', '$total_expenses_net',
                            '$transfers_net'
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
                '$set': {
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
                        '$convert': {
                            'input': '$_id',
                            'to': 'string',
                            'onError': '',
                            'onNull': ''
                        }
                    },
                    'account_name': 1,
                    'total_cars_net': 1,
                    'total_capitals_net': 1,
                    'total_outstanding_net': 1,
                    'total_expenses_net': 1,
                    'transfers_net': 1,
                    'final_net': 1
                }
            }, {
                '$set': {
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
                                        }, {
                                            'case': {
                                                '$regexMatch': {
                                                    'input': '$account_name',
                                                    'regex': 'bank',
                                                    'options': 'i'
                                                }
                                            },
                                            'then': '🏦 '
                                        }, {
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
                            }, '$account_name'
                        ]
                    }
                }
            }, {
                '$sort': {
                    'account_name': 1
                }
            }, {
                '$group': {
                    '_id': None,
                    'all_accounts': {
                        '$push': {
                            'account_id': '$account_id',
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
        cursor = await all_trades_items_collection.aggregate(cash_on_hand_pipeline)
        results = await cursor.to_list(length=1)
        result = results[0] if results else {"all_accounts": [], "total_final_net": 0}
        return {"totals": result}

    except PyMongoError as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")

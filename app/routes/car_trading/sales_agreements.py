import math

from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException
from fastapi.encoders import jsonable_encoder

from app.core import security
from app.routes.counters import create_custom_counter
from app.websocket_config import manager

from .common import (
    all_trades_purchase_agreement_items_collection,
    ensure_trade_belongs_to_company,
    parse_object_id,
    require_payload_field,
    zero_if_none,
)
from .models import PurchaseAgreementModel

router = APIRouter()


def normalize_agreement(document: dict) -> dict:
    """Read legacy agreements as sales without a bulk database migration."""
    normalized = dict(document)
    normalized.pop("profit_margin_scheme", None)
    for key, default in (
        ("agreement_type", "sell"),
        ("payment_method", ""),
        ("agreement_amount", 0),
        ("agreement_down_payment", 0),
    ):
        if normalized.get(key) is None:
            normalized[key] = default
    return normalized


def validate_agreement_values(document: dict):
    require_payload_field(document, "agreement_date", "Agreement date")
    require_payload_field(document, "seller_name", "Seller name")
    require_payload_field(document, "buyer_name", "Buyer name")
    amount = document.get("agreement_amount")
    paid = zero_if_none(document.get("agreement_down_payment"))
    if amount is None or not math.isfinite(amount) or amount <= 0:
        raise HTTPException(status_code=400, detail="Agreement amount must be greater than zero")
    if not math.isfinite(paid) or paid < 0 or paid > amount:
        raise HTTPException(
            status_code=400,
            detail="Amount paid must be between zero and the agreement amount",
        )


@router.get("/get_purchase_agreement_for_current_trade/{trade_id}")
async def get_purchase_agreement_for_current_trade(trade_id: str,
                                                   data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        trade_id = parse_object_id(trade_id, "trade_id")
        await ensure_trade_belongs_to_company(trade_id, company_id)
        purchase_agreement_items_pipeline = [
            {
                '$match': {
                    'trade_id': trade_id,
                    'company_id': company_id,
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
        return {'purchase_agreement_items': [normalize_agreement(item) for item in results]}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/add_purchase_agreement_item")
async def add_purchase_agreement_item(purchase_agreement_item: PurchaseAgreementModel,
                                      data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get('company_id'))
        purchase_agreement_item_dict = purchase_agreement_item.model_dump(exclude_unset=True)
        trade_id = parse_object_id(
            require_payload_field(purchase_agreement_item_dict, "trade_id", "Trade id"),
            "trade_id",
        )
        await ensure_trade_belongs_to_company(trade_id, company_id)
        purchase_agreement_item_dict = normalize_agreement(purchase_agreement_item_dict)
        validate_agreement_values(purchase_agreement_item_dict)
        new_purchase_agreement_counter = await create_custom_counter("CMP", "CM", data=data,
                                                                     description='Compass Motors Purchase Agreement')

        purchase_agreement_item_dict.update({
            "trade_id": trade_id,
            "company_id": company_id,
            "createdAt": security.now_utc(),
            "updatedAt": security.now_utc(),
            "agreement_amount": zero_if_none(purchase_agreement_item_dict.get("agreement_amount")),
            "agreement_down_payment": zero_if_none(purchase_agreement_item_dict.get("agreement_down_payment")),
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
        return {"message": "Agreement added successfully", "data": encoded_data}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.patch("/update_purchase_agreement_item/{purchase_item_id}")
async def update_purchase_agreement_item(purchase_item_id: str, purchase_agreement_item: PurchaseAgreementModel,
                                         data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get('company_id'))
        purchase_item_id = parse_object_id(purchase_item_id, "purchase_item_id")
        purchase_agreement_item_details_pipeline = [
            {
                '$match': {
                    '_id': purchase_item_id,
                    'company_id': company_id,
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
        purchase_agreement_item_dict.pop("trade_id", None)
        existing = await all_trades_purchase_agreement_items_collection.find_one(
            {"_id": purchase_item_id, "company_id": company_id}
        )
        if not existing:
            raise HTTPException(status_code=404, detail="Purchase Agreement Item not found")
        merged = normalize_agreement({**existing, **purchase_agreement_item_dict})
        validate_agreement_values(merged)

        purchase_agreement_item_dict.update({
            "agreement_type": merged["agreement_type"],
            "payment_method": merged["payment_method"],
            "agreement_amount": merged["agreement_amount"],
            "agreement_down_payment": merged["agreement_down_payment"],
            "updatedAt": security.now_utc(),
        })

        result = await all_trades_purchase_agreement_items_collection.update_one(
            {"_id": purchase_item_id, "company_id": company_id},
            {"$set": purchase_agreement_item_dict},
        )

        if result.matched_count == 0:
            raise HTTPException(status_code=404, detail="Purchase Agreement Item not found")

        cursor = await all_trades_purchase_agreement_items_collection.aggregate(
            purchase_agreement_item_details_pipeline)
        result = await cursor.to_list(length=1)
        if not result:
            raise HTTPException(status_code=404, detail="Purchase Agreement Item not found")

        encoded_data = jsonable_encoder(normalize_agreement(result[0]))
        await manager.send_to_company(str(company_id), {
            "type": "purchase_agreement_item_updated",
            "data": encoded_data
        })
        return {"message": "Agreement updated successfully", "data": encoded_data}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/delete_purchase_agreement_item/{purchase_id}")
async def delete_purchase_agreement_item(purchase_id: str, data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get('company_id'))
        purchase_id = parse_object_id(purchase_id, "purchase_id")
        result = await all_trades_purchase_agreement_items_collection.delete_one(
            {"_id": purchase_id, "company_id": company_id},
        )
        if result.deleted_count == 1:
            await manager.send_to_company(str(company_id), {
                "type": "purchase_agreement_item_deleted",
                "data": {"_id": str(purchase_id)}
            })
            return {"message": "Purchase Agreement Item removed successfully!"}
        raise HTTPException(status_code=404, detail="item not found")

    except HTTPException:
        raise
    except Exception as error:
        raise HTTPException(status_code=500, detail=str(error))

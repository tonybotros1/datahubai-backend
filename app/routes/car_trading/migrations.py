from fastapi import APIRouter, Depends, HTTPException

from app.core import security

from .common import all_general_expenses_collection, all_trades_items_collection

router = APIRouter()


@router.post("/migrate_all_general_expenses_to_trade_items")
async def migrate_all_general_expenses_to_trade_items(data: dict = Depends(security.get_current_user)):
    try:
        inserted_count = 0
        skipped_count = 0
        failed = []

        cursor = all_general_expenses_collection.find({})
        async for expense in cursor:
            expense_id = expense.get("_id")
            if expense_id is None:
                failed.append({"_id": "", "error": "Missing _id"})
                continue

            existing = await all_trades_items_collection.find_one(
                {"_id": expense_id},
                {"_id": 1},
            )
            if existing:
                skipped_count += 1
                continue

            trade_item = dict(expense)
            trade_item["trade_id"] = None

            try:
                await all_trades_items_collection.insert_one(trade_item)
                inserted_count += 1
            except Exception as error:
                failed.append({"_id": str(expense_id), "error": str(error)})

        return {
            "message": "General expenses migration completed",
            "inserted": inserted_count,
            "skipped": skipped_count,
            "failed": failed,
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Migration error: {str(e)}")

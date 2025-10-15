import asyncio
from datetime import datetime, timezone

from fastapi import FastAPI
from bson import ObjectId
import json

from app.database import get_collection

app = FastAPI()

brands_collection = get_collection("all_brands")
models_collection = get_collection("all_brand_models")
value_collection = get_collection("all_lists_values")
all_trades_collection = get_collection("all_trades")
all_trades_items_collection = get_collection("all_trades_items")


def parse_date(date_str):
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%d-%m-%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(date_str, fmt)
        except (ValueError, TypeError):
            continue
    return None


# --- Helpers ---
async def get_brand_id(brand_name: str):
    brand = await brands_collection.find_one({"name": brand_name.strip().upper()})
    return brand["_id"] if brand else ""


async def get_model_id(brand_id: ObjectId, model_name: str):
    model = await models_collection.find_one({
        "brand_id": brand_id,
        "name": model_name.strip().upper()
    })
    return model["_id"] if model else ""


async def get_list_value_id(value: str):
    doc = await value_collection.find_one({"name": value.strip()})
    return doc["_id"] if doc else ""


# --- Import trades ---
async def import_trades():
    with open("trades.json", "r", encoding="utf-8") as f:
        trades_json = json.load(f)

    inserted_trades = []

    for trade_key, trade_data in trades_json.items():
        # 1. Resolve IDs
        brand_id = await get_brand_id(trade_data.get("car_brand"))
        model_id = await get_model_id(brand_id, trade_data.get("car_model")) if brand_id else None
        color_in_id = await get_list_value_id(trade_data.get("color_in"))
        color_out_id = await get_list_value_id(trade_data.get("color_out"))
        engine_size_id = await get_list_value_id(trade_data.get("engine_size"))
        spec_id = await get_list_value_id(trade_data.get("specification"))
        year_id = await get_list_value_id(trade_data.get("year"))
        bought_from = await get_list_value_id(trade_data.get("bought_from"))
        sold_to = await get_list_value_id(trade_data.get("sold_to"))

        # 2. Insert into all_trades
        trade_doc = {
            "company_id": ObjectId("68bbfc4b56c35562f967422d"),
            "car_brand": brand_id,
            "car_model": model_id,
            "color_in": color_in_id,
            "color_out": color_out_id,
            "date": parse_date(trade_data.get("date")),
            "engine_size": engine_size_id,
            "mileage": float(trade_data.get("mileage")),
            "note": trade_data.get("note"),
            "specification": spec_id,
            "status": trade_data.get("status"),
            "bought_from": bought_from,
            "sold_to": sold_to,
            "year": year_id,
            "createdAt": datetime.now(timezone.utc),
            "updatedAt": datetime.now(timezone.utc),
        }
        result = await all_trades_collection.insert_one(trade_doc)
        new_trade_id = result.inserted_id

        # 3. Insert related items into all_trades_items
        for item in trade_data.get("items", []):
            item_id = await get_list_value_id(item.get("item"))
            item_doc = {
                "company_id": ObjectId("68bbfc4b56c35562f967422d"),
                "trade_id": new_trade_id,
                "comment": item.get("comment"),
                "date": parse_date(item.get("date")),
                "pay": item.get("pay", 0),
                "receive": item.get("receive", 0),
                "item": item_id,
                "createdAt": datetime.now(timezone.utc),
                "updatedAt": datetime.now(timezone.utc),
            }
            await all_trades_items_collection.insert_one(item_doc)

        print(f"✅ Trade {trade_key} inserted as {new_trade_id}")
        inserted_trades.append(str(new_trade_id))

    print("🎉 Import finished! Inserted trades:", inserted_trades)


if __name__ == "__main__":
    asyncio.run(import_trades())

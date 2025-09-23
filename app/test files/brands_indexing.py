import asyncio
import json
from fastapi import APIRouter
from datetime import datetime, timezone
from app.database import get_collection

router = APIRouter()
brands_collection = get_collection("all_brands")
models_collection = get_collection("all_brand_models")


async def entering():
    with open("brands.json", "r", encoding="utf-8") as f:
        brands_json = json.load(f)

    for brands_index, brands_values in brands_json.items():
        brand_dict = {
            "name": brands_values.get("name").strip(),
            "status": True,
            "logo": None,
            "logo_public_id": None,
            "createdAt": datetime.now(timezone.utc),
            "updatedAt": datetime.now(timezone.utc),
        }

        result = await brands_collection.insert_one(brand_dict)
        new_brand_id = result.inserted_id

        for model_index, models_values in brands_values.get("models", {}).items():
            model_dict = {
                "name": models_values.get("name").strip(),
                "status": True,
                "brand_id": new_brand_id,
                "createdAt": datetime.now(timezone.utc),
                "updatedAt": datetime.now(timezone.utc),
            }
            await models_collection.insert_one(model_dict)

        print(f"✅ Brand {brands_index} inserted as {new_brand_id}")

    print("🎉 Import finished! Inserted brands and models")


if __name__ == "__main__":
    asyncio.run(entering())

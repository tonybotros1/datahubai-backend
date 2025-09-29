import asyncio
import json

from bson import ObjectId
from fastapi import APIRouter
from datetime import datetime, timezone
from app.database import get_collection

router = APIRouter()
ap_payment_types_collection = get_collection("ap_payment_types")



async def entering():
    with open("ap_payment_types.json", "r", encoding="utf-8") as f:
        types_json = json.load(f)

    for index, value in types_json.items():
        type_dict = {
            'type': value,
            'company_id': ObjectId('68bbfc4b56c35562f967422d'),
            'createdAt': datetime.now(timezone.utc),
            'updatedAt': datetime.now(timezone.utc),
        }
        await ap_payment_types_collection.insert_one(type_dict)
        print(index, 'inserted')



if __name__ == "__main__":
    asyncio.run(entering())

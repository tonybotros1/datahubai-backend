import asyncio
from datetime import datetime, timezone
from fastapi import FastAPI
from bson import ObjectId
import json
from app.database import get_collection

app = FastAPI()

out_collection = get_collection("all_outstanding")
value_collection = get_collection("all_lists_values")


async def get_list_value_id(value: str):
    if not value:
        return ""
    doc = await value_collection.find_one({
        "list_id": ObjectId("68c3475bea4b81049ba91fc0"),
        "name": value.strip()
    })
    if doc:
        print(doc['_id'])
    else:
        print("not found")
    return doc["_id"] if doc else ""


def parse_date(date_str):
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%d-%m-%Y", "%d/%m/%Y",):
        try:
            return datetime.strptime(date_str, fmt)
        except (ValueError, TypeError):
            continue
    return None


async def adding_outstanding():
    with open("outstanding.json", "r", encoding="utf-8") as f:
        out_json = json.load(f)

    for out_index, out_data in out_json.items():
        name = await get_list_value_id(out_data.get("name").strip())
        out_dict = {
            "name": name,
            "company_id": ObjectId("68bbfc4b56c35562f967422d"),
            "pay": out_data.get("pay"),
            "receive": out_data.get("receive"),
            "comment": out_data.get("comment"),
            "date": parse_date(out_data.get("date")),
            "createdAt": datetime.now(timezone.utc),
            "updatedAt": datetime.now(timezone.utc),
        }
        res = await out_collection.insert_one(out_dict)
        # print(f"added {res.inserted_id}")
    print("doneeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee")


if __name__ == "__main__":
    asyncio.run(adding_outstanding())

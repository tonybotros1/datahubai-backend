from fastapi import APIRouter, Body
from app.database import get_collection
from datetime import datetime, timezone
from app.websocket_config import manager

router = APIRouter()
screens_collection = get_collection("screens")


def screen_serializer(screen) -> dict:
    return {
        "_id": str(screen["_id"]),  # <-- حوّل ObjectId لـ str
        "name": screen["name"],
        "description": screen["description"],
        "route_name": screen["route_name"],
        "createdAt": screen["createdAt"].isoformat(),
        "updatedAt": screen["updatedAt"].isoformat(),
    }


@router.get("/get_screens")
async def get_screens():
    screens = list(screens_collection.find({}))
    return {"screens": [screen_serializer(s) for s in screens]}


@router.post("/add_screen")
async def add_screen(name: str = Body(..., embed=True), route_name: str = Body(..., embed=True),
                     description: str = Body(..., embed=True)):
    try:
        screen_dict = {
            "name": name,
            "route_name": route_name,
            "description": description,
            "createdAt": datetime.now(timezone.utc),
            "updatedAt": datetime.now(timezone.utc)
        }

        result = screens_collection.insert_one(screen_dict)
        screen_dict['_id'] = str(result.inserted_id)
        serialized = screen_serializer(screen_dict)
        await manager.broadcast({
            "type": "screen_created",
            "data": serialized
        })
        return {"message": "Brand created successfully!", "brand": serialized}
    except Exception as e:
        return {"error": str(e)}


@router.delete("/remove_screen/{screen_id}")
async def remove_screen(screen_id: str):
    try:
        result = screens_collection.delete_one({"_id": screen_id})

    except Exception as e:
        return {"error": str(e)}

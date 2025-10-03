from bson import ObjectId
from fastapi import APIRouter, Body, HTTPException, Depends
from app.core import security
from app.database import get_collection
from datetime import datetime, timezone
from app.websocket_config import manager

router = APIRouter()
screens_collection = get_collection("screens")
menus_collection = get_collection("menus")


def screen_serializer(screen: dict) -> dict:
    screen["_id"] = str(screen["_id"])
    for key, value in screen.items():
        if isinstance(value, datetime):
            screen[key] = value.isoformat()
    return screen


@router.get("/get_screens")
async def get_screens(_: dict = Depends(security.get_current_user)):
    screens = await screens_collection.find({}).sort("name", 1).to_list()
    return {"screens": [screen_serializer(s) for s in screens]}


@router.post("/add_screen")
async def add_screen(name: str = Body(..., embed=True), route_name: str = Body(..., embed=True),
                     description: str = Body(..., embed=True), _: dict = Depends(security.get_current_user)):
    try:
        screen_dict = {
            "name": name,
            "route_name": route_name,
            "description": description,
            "createdAt": datetime.now(timezone.utc),
            "updatedAt": datetime.now(timezone.utc)
        }

        result = await screens_collection.insert_one(screen_dict)
        screen_dict['_id'] = str(result.inserted_id)
        serialized = screen_serializer(screen_dict)
        await manager.broadcast({
            "type": "screen_created",
            "data": serialized
        })
        return {"message": "Screen created successfully!", "Screen": serialized}
    except Exception as e:
        return {"error": str(e)}


@router.delete("/delete_screen/{screen_id}")
async def delete_screen(screen_id: str, _: dict = Depends(security.get_current_user)):
    try:
        # 1️⃣ Remove menuId from all parents' children arrays
        await menus_collection.update_many(
            {"children": ObjectId(screen_id)},
            {"$pull": {"children": ObjectId(screen_id)}}
        )
        result = await screens_collection.delete_one({"_id": ObjectId(screen_id)})
        if result.deleted_count == 0:
            raise HTTPException(status_code=404, detail="Screen not found")
        else:
            await manager.broadcast({
                "type": "screen_deleted",
                "data": {"_id": screen_id}
            })
            return {"message": "Screen deleted successfully"}


    except Exception as e:
        return {"error": str(e)}


@router.patch("/edit_screen/{screen_id}")
async def edit_screen(screen_id: str, name: str | None = Body(None), screen_route: str | None = Body(None),
                      description: str | None = None, _: dict = Depends(security.get_current_user)):
    try:
        screen = await screens_collection.find_one({"_id": ObjectId(screen_id)})
        if screen is None:
            raise HTTPException(status_code=404, detail="Screen not found")
        else:
            if name:
                screen["name"] = name

            if screen_route:
                screen["route_name"] = screen_route

            if description:
                screen["description"] = description

            screen["updatedAt"] = datetime.now(timezone.utc)

            await screens_collection.update_one({"_id": ObjectId(screen_id)}, {"$set": screen})
            serialized = screen_serializer(screen)
            await manager.broadcast({
                "type": "screen_updated",
                "data": serialized
            })

        return {"message": "Screen updated successfully!", "City": serialized}

    except Exception as e:
        return {"error": str(e)}

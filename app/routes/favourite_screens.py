from bson import ObjectId
from fastapi import APIRouter, Body, Depends, status
from app.core import security
from app.database import get_collection
from datetime import datetime, timezone
from app.websocket_config import manager
from app.widgets.helper import serialize_doc

router = APIRouter()
favourite_collection = get_collection("favourite_screens")


async def get_favourite_screen_details(fav_id: ObjectId):
    try:
        pipeline = [
            {"$match": {"_id": fav_id}},
            {
                "$lookup": {
                    "from": "screens",
                    "localField": "screen_id",
                    "foreignField": "_id",
                    "as": "screen",

                }
            },
            {
                "$unwind": {
                    "path": "$screen",
                    "preserveNullAndEmptyArrays": False,
                }
            },
            {
                "$project": {
                    "_id": 1,
                    "screen_name": "$screen.name",
                    "screen_route": "$screen.route_name",
                    "description": "$screen.description",
                    "createdAt": 1,
                    "updatedAt": 1,
                    "screen_id": 1

                }
            }

        ]
        cursor = await favourite_collection.aggregate(pipeline)
        result = await cursor.to_list()
        return result[0]

    except Exception as e:
        return {"message": str(e)}


@router.get("/get_favourite_screens")
async def get_favourite_screens(user: dict = Depends(security.get_current_user)):
    try:
        user_id = user.get("sub")
        if not user_id:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User ID not found in token")
        pipeline = [
            {
                "$match": {"user_id": ObjectId(user_id)}
            },
            {
                "$lookup": {
                    "from": "screens",
                    "localField": "screen_id",
                    "foreignField": "_id",
                    "as": "screen",
                }
            },
            {
                "$unwind": {
                    "path": "$screen",
                    "preserveNullAndEmptyArrays": False
                }
            },
            {
                "$project": {
                    "_id": 1,
                    "screen_name": "$screen.name",
                    "screen_route": "$screen.route_name",
                    "description": "$screen.description",
                    "createdAt": 1,
                    "updatedAt": 1,
                    "screen_id": 1

                }},
            {
                "$sort": {"createdAt": -1}  # sort newest first
            }
        ]
        cursor = await favourite_collection.aggregate(pipeline)
        result = await cursor.to_list()

        # data = result[0]
        return {"favourites": [serialize_doc(res) for res in result]}

    except Exception as e:
        return {"message": str(e)}


@router.post("/add_screen_to_favourites")
async def add_screen_to_favourites(screen_id: str = Body(..., embed=True),
                                   user: dict = Depends(security.get_current_user)):
    try:
        user_id = user.get("sub")

        favourite_dict = {
            "user_id": ObjectId(user_id),
            "screen_id": ObjectId(screen_id),
            "createdAt": datetime.now(timezone.utc),
            "updatedAt": datetime.now(timezone.utc),
        }
        result = await favourite_collection.insert_one(favourite_dict)
        fav_details = await get_favourite_screen_details(result.inserted_id)
        serialized = serialize_doc(fav_details)
        await manager.broadcast({
            "type": "favourite_added",
            "data": serialized
        })
        return {"message": "fav added successfully!", "fav": serialized}


    except Exception as e:
        return {"message": str(e)}


from fastapi import HTTPException


@router.delete("/remove_screen_from_favourites/{screen_id}")
async def remove_screen_from_favourites(
        screen_id: str,
        user_: dict = Depends(security.get_current_user)
):
    try:
        user_id = user_.get("sub")

        result = await favourite_collection.find_one_and_delete(
            {"screen_id": ObjectId(screen_id), "user_id": ObjectId(user_id)},
        )

        if not result:
            raise HTTPException(status_code=404, detail="Favourite not found")

        await manager.broadcast({
            "type": "favourite_deleted",
            "data": {"_id": str(result["_id"])}  # send deleted favourite id
        })

        return {"message": "Favourite removed successfully", "deleted": str(result["_id"])}

    except Exception as e:
        return {"message": str(e)}

from bson import ObjectId
from fastapi import APIRouter, Body, HTTPException
from pymongo import ReturnDocument

from app.database import get_collection
from datetime import datetime, timezone
from app.websocket_config import manager

router = APIRouter()
menus_collection = get_collection("menus")
screens_collection = get_collection("screens")


def menus_serializer(menus: dict) -> dict:
    menus["_id"] = str(menus["_id"])
    for key, value in menus.items():
        if isinstance(value, datetime):
            menus[key] = value.isoformat()
        # تحويل ObjectId في children إلى str
        if "children" in menus and isinstance(menus["children"], list):
            new_children = []
            for child in menus["children"]:
                if isinstance(child, ObjectId):
                    new_children.append(str(child))
                else:
                    new_children.append(child)
            menus["children"] = new_children
    return menus


# this is to get all menus
@router.get("/get_menus")
async def get_menus():
    try:
        menus = await menus_collection.find({}).sort("name", 1).to_list()
        return {"menus": [menus_serializer(menus) for menus in menus]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# this is to add new menu
@router.post("/add_new_menu")
async def add_new_menu(name: str | None = Body(None), code: str | None = Body(None), route: str | None = Body(None)):
    try:
        menu_dict = {
            "name": name,
            "code": code,
            "route_name": route,
            "children": [],
            "createdAt": datetime.now(timezone.utc),
            "updatedAt": datetime.now(timezone.utc),
        }

        result = await menus_collection.insert_one(menu_dict)
        menu_dict["_id"] = str(result.inserted_id)
        serialized = menus_serializer(menu_dict)
        await manager.broadcast({
            "type": "menu_created",
            "data": serialized
        })
        return {"message": "Menu created successfully!", "Menu": serialized}

    except Exception as e:
        return {"error": str(e)}


@router.delete("/delete_menu/{menu_id}")
async def delete_menu(menu_id: str):
    try:

        # 1️⃣ Remove menuId from all parents' children arrays
        await menus_collection.update_many(
            {"children": ObjectId(menu_id)},
            {"$pull": {"children": ObjectId(menu_id)}}
        )

        # 2️⃣ Delete the menu itself
        result = await menus_collection.delete_one({"_id": ObjectId(menu_id)})

        if result.deleted_count == 0:
            raise HTTPException(status_code=404, detail="Menu not found")

        await manager.broadcast({
            "type": "menu_deleted",
            "data": {"_id": menu_id}
        })

        # 3️⃣ Return success
        return {"message": "Menu deleted successfully", "menu_id": menu_id}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.patch("/update_menu/{menu_id}")
async def update_menu(menu_id: str, name: str | None = Body(None), code: str | None = Body(None),
                      route: str | None = Body(None)):
    try:
        menu = await menus_collection.find_one_and_update({"_id": ObjectId(menu_id)},
                                                          {"$set": {"updatedAt": datetime.now(timezone.utc),
                                                                    "name": name, "code": code, "route_name": route}},
                                                          return_document=ReturnDocument.AFTER

                                                          )

        serialized = menus_serializer(menu)
        await manager.broadcast({
            "type": "menu_updated",
            "data": serialized
        })
        return {"message": "Menu updated successfully!", "Menu": serialized}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# =============================================== Building the tree ===================================================


@router.get("/get_menu_tree/{menu_id}")
async def get_menu_tree(menu_id: str):
    try:
        # 1. Fetch menu tree using $graphLookup
        pipeline = [
            {"$match": {"_id": ObjectId(menu_id)}},
            {
                "$graphLookup": {
                    "from": "menus",
                    "startWith": "$children",
                    "connectFromField": "children",
                    "connectToField": "_id",
                    "as": "tree",
                    "restrictSearchWithMatch": {}  # Ensure index usage
                }
            }
        ]
        cursor = await menus_collection.aggregate(pipeline)
        result = await cursor.to_list()

        if not result:
            raise HTTPException(status_code=404, detail="Menu not found")

        root = result[0]
        menus_nodes = root["tree"] + [root]

        # 2. Build node map for menus
        node_map = {}
        all_children_ids = set()
        for n in menus_nodes:
            n_id = str(n["_id"])
            node_map[n_id] = {
                "_id": n_id,
                "name": n.get("name"),
                "children": [str(c) for c in n.get("children", [])],
                "isMenu": True,
                "can_remove": n_id != str(root["_id"]),
            }
            all_children_ids.update(node_map[n_id]["children"])

        # 3. Fetch all screens not in menus
        screen_ids_to_fetch = [
            ObjectId(cid) for cid in all_children_ids if cid not in node_map
        ]
        screens_cursor = screens_collection.find(
            {"_id": {"$in": screen_ids_to_fetch}},
            projection={"name": 1}  # Minimal projection
        )
        screens_list = await screens_cursor.to_list(length=1000)

        # 4. Add screens to node_map
        for screen in screens_list:
            s_id = str(screen["_id"])
            node_map[s_id] = {
                "_id": s_id,
                "name": screen.get("name"),
                "children": [],
                "isMenu": False,
                "can_remove": True,
            }

        # 5. Link children to parent nodes
        for node in node_map.values():
            node["children"] = [
                node_map[child_id] for child_id in node["children"]
                if child_id in node_map  # Ensure exists
            ]

        return node_map[str(root["_id"])]

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# this function is to remove node from the tree
@router.patch("/remove_node_from_the_tree/{menu_id}/{node_id}")
async def remove_node_from_the_tree(menu_id: str, node_id: str):
    try:
        await menus_collection.find_one_and_update({"_id": ObjectId(menu_id)},
                                                   {"$pull": {"children": ObjectId(node_id)}}
                                                   )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


async def is_loop_detected(child_ids: list[str], target_id: str) -> bool:
    pipeline = [
        {"$match": {"_id": {"$in": [ObjectId(cid) for cid in child_ids]}}},
        {
            "$graphLookup": {
                "from": "menus",
                "startWith": "$children",
                "connectFromField": "children",
                "connectToField": "_id",
                "as": "descendants"
            }
        },
        {
            "$project": {
                "ids": {"$concatArrays": [["$_id"], "$descendants._id"]}
            }
        }
    ]

    cursor = await menus_collection.aggregate(pipeline)
    results = await cursor.to_list(length=None)

    # Flatten all ids
    all_ids = set()
    for r in results:
        all_ids.update(str(x) for x in r["ids"])

    return target_id in all_ids


# this function is to add sub menu or screen to existing menu
@router.post("/add_sub_menus/{menu_id}")
async def add_existing_submenus(menu_id: str, submenus: list[str] = Body(..., embed=True),
                                is_menu: bool = Body(default=True)):
    try:
        if not submenus:
            raise HTTPException(status_code=400, detail="No submenus provided")

        parent = await menus_collection.find_one({"_id": ObjectId(menu_id)})
        if not parent:
            raise HTTPException(status_code=404, detail="Parent menu not found")


        new_children = [ObjectId(cid) for cid in submenus]
        if is_menu:
            if await is_loop_detected(submenus, menu_id):
                raise HTTPException(
                    status_code=400,
                    detail="Loop detected: One or more submenus would create a cycle"
                )

        await menus_collection.update_one(
            {"_id": ObjectId(menu_id)},
            {"$addToSet": {"children": {"$each": new_children}}},
        )
        return await get_menu_tree(menu_id)



    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

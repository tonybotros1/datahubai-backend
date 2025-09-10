from bson import ObjectId
from fastapi import APIRouter, Body, HTTPException, Depends, status
from pymongo import ReturnDocument

from app.core import security
from app.database import get_collection
from datetime import datetime, timezone
from app.websocket_config import manager

router = APIRouter()
menus_collection = get_collection("menus")
screens_collection = get_collection("screens")
users_collection = get_collection("sys-users")


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
async def get_menus(_: dict = Depends(security.get_current_user)):
    try:
        menus = await menus_collection.find({}).sort("name", 1).to_list()
        return {"menus": [menus_serializer(menus) for menus in menus]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# this is to add new menu
@router.post("/add_new_menu")
async def add_new_menu(name: str | None = Body(None), code: str | None = Body(None), route: str | None = Body(None)
                       , _: dict = Depends(security.get_current_user)):
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
async def delete_menu(menu_id: str, _: dict = Depends(security.get_current_user)):
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
                      route: str | None = Body(None), _: dict = Depends(security.get_current_user)):
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

def serialize_doc(doc):
    if isinstance(doc, dict):
        # Convert _id to string for serialization
        if "_id" in doc and isinstance(doc["_id"], ObjectId):
            doc["_id"] = str(doc["_id"])
        # Recursively serialize children
        for key, value in doc.items():
            doc[key] = serialize_doc(value)
    elif isinstance(doc, list):
        return [serialize_doc(item) for item in doc]
    return doc


@router.get("/get_user_menu_tree")
async def get_user_menu_tree(user_data: dict = Depends(security.get_current_user)):
    try:
        user_id = user_data.get("sub")
        if not user_id:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User ID not found in token")

        pipeline = [
            # ... your aggregation pipeline remains the same
            {
                "$match": {
                    "_id": ObjectId(user_id)
                }
            },
            {
                "$lookup": {
                    "from": "sys-roles",
                    "localField": "roles",
                    "foreignField": "_id",
                    "as": "userRoles"
                }
            },
            {
                "$unwind": {
                    "path": "$userRoles",
                    "preserveNullAndEmptyArrays": True
                }
            },
            {
                "$lookup": {
                    "from": "menus",
                    "localField": "userRoles.menu_id",
                    "foreignField": "_id",
                    "as": "userMenus"
                }
            },
            {
                "$unwind": {
                    "path": "$userMenus",
                    "preserveNullAndEmptyArrays": True
                }
            },
            {
                "$graphLookup": {
                    "from": "menus",
                    "startWith": "$userMenus.children",
                    "connectFromField": "children",
                    "connectToField": "_id",
                    "as": "menuTree"
                }
            },
            {
                "$group": {
                    "_id": "$_id",
                    "rootMenus": {"$push": "$userMenus"},
                    "treeNodes": {"$push": "$menuTree"}
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "rootMenus": "$rootMenus",
                    "tree": "$treeNodes"
                }
            }
        ]

        cursor = await users_collection.aggregate(pipeline)
        result = await cursor.to_list()

        if not result:
            raise HTTPException(status_code=404, detail="User not found.")

        data = result[0]

        # Safely access 'rootMenus' and 'tree'
        root_menus = data.get("rootMenus", [])
        tree_nodes_list = data.get("tree", [])

        if not root_menus:
            raise HTTPException(status_code=404, detail="No menus found for this user.")

        # Flatten the list of lists from tree_nodes
        all_nodes = root_menus + [node for sublist in tree_nodes_list for node in sublist]

        # 1. Build a node map and find all children IDs
        node_map = {}
        all_children_ids = set()

        for n in all_nodes:
            if n:
                n_id = str(n["_id"])
                node_map[n_id] = {
                    "_id": n_id,
                    "name": n.get("name"),
                    "children": [str(c) for c in n.get("children", [])],
                    "isMenu": True,
                    "can_remove": True,
                    "route_name": n.get("route_name")
                }
                all_children_ids.update(node_map[n_id]["children"])

        # 2. Fetch screens that are not menus
        screen_ids_to_fetch = [
            ObjectId(cid) for cid in all_children_ids if cid not in node_map
        ]

        screens_cursor = screens_collection.find(
            {"_id": {"$in": screen_ids_to_fetch}},
            projection={"name": 1, "route_name": 1}
        )
        screens_list = await screens_cursor.to_list(length=1000)

        # 3. Add screens to the node map
        for screen in screens_list:
            s_id = str(screen["_id"])
            node_map[s_id] = {
                "_id": s_id,
                "name": screen.get("name"),
                "children": [],
                "isMenu": False,
                "can_remove": True,
                "route_name": screen.get("route_name"),
            }

        # 4. Link children to parent nodes
        for node in node_map.values():
            node["children"] = [
                node_map[child_id] for child_id in node["children"]
                if child_id in node_map
            ]

        # 5. Build the final tree structure from root menus
        final_tree = []
        for root_menu in root_menus:
            root_id = str(root_menu["_id"])
            if root_id in node_map:
                final_tree.append(node_map[root_id])

        return {"root": final_tree}

    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")


@router.get("/get_menu_tree/{menu_id}")
async def get_menu_tree(menu_id: str, _: dict = Depends(security.get_current_user)):
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
async def remove_node_from_the_tree(menu_id: str, node_id: str, _: dict = Depends(security.get_current_user)):
    try:
        await menus_collection.find_one_and_update({"_id": ObjectId(menu_id)},
                                                   {
                                                       "$pull": {"children": ObjectId(node_id)},
                                                       "$set": {"updatedAt": datetime.now(timezone.utc)},
                                                   })
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
                                is_menu: bool = Body(default=True), _: dict = Depends(security.get_current_user)):
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

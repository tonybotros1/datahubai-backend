from bson import ObjectId
from fastapi import APIRouter, HTTPException, Form, File, UploadFile, Body
from app.database import get_collection
from datetime import datetime, timezone
from app.widgets import upload_images
from app.websocket_config import manager

router = APIRouter()
brands_collection = get_collection("all_brands")
models_collection = get_collection("all_brand_models")


# ====================================== Brands Section ============================================

def brand_serializer(brand) -> dict:
    return {
        "_id": str(brand["_id"]),  # <-- حوّل ObjectId لـ str
        "name": brand["name"],
        "logo": brand["logo"],
        "status": brand.get("status", True),
        "createdAt": brand["createdAt"].isoformat(),
        "updatedAt": brand["updatedAt"].isoformat(),
    }


def model_serializer(model) -> dict:
    return {
        "_id": str(model["_id"]),  # <-- حوّل ObjectId لـ str
        "name": model["name"],
        "status": model.get("status", True),
        "createdAt": model["createdAt"].isoformat(),
        "updatedAt": model["updatedAt"].isoformat(),
    }


@router.get("/get_all_brands")
def get_brands():
    """إرجاع كل البراندز"""
    brands = list(brands_collection.find({}))
    return {"brands": [brand_serializer(b) for b in brands]}


@router.post("/add_new_brand")
async def create_brand(name: str = Form(...), logo: UploadFile = File(...)):
    try:
        result = await upload_images.upload_image(logo, 'car_brands')

        brand_dict = {
            "name": name,
            "logo": result["url"],
            "logo_public_id": result["public_id"],
            "status": True,
            "createdAt": datetime.now(timezone.utc),
            "updatedAt": datetime.now(timezone.utc),
        }

        insert_result = brands_collection.insert_one(brand_dict)
        brand_dict["_id"] = str(insert_result.inserted_id)
        serialized = brand_serializer(brand_dict)

        await manager.broadcast({
            "type": "brand_created",
            "data": serialized
        })
        return {"message": "Brand created successfully!", "brand": serialized}
    except Exception as e:
        return {"error": str(e)}


@router.patch("/edit_brand/{brand_id}")
async def update_brand(brand_id: str, name: str | None = Form(None), logo: UploadFile | None = File(None)):
    brand = brands_collection.find_one({"_id": ObjectId(brand_id)})
    if not brand:
        raise HTTPException(status_code=404, detail="Brand not found")

    brand_data = {}

    if name:
        brand_data["name"] = name
    else:
        brand_data["name"] = brand["name"]

    if logo:
        if brand['logo']:
            await upload_images.delete_image_from_server(brand['logo_public_id'])
        result = await upload_images.upload_image(logo, 'car_brands')
        brand_data["logo"] = result["url"]
        brand_data["logo_public_id"] = result["public_id"]
    else:
        brand_data["logo"] = brand["logo"]
        brand_data["logo_public_id"] = brand["logo_public_id"]

    if not brand_data:
        raise HTTPException(status_code=400, detail="Nothing to update")

    brand_data['updatedAt'] = datetime.now(timezone.utc)
    brands_collection.update_one({"_id": ObjectId(brand_id)}, {"$set": brand_data})
    brand_data['_id'] = ObjectId(brand_id)
    brand_data['createdAt'] = brand['createdAt']
    brand_data["status"] = brand.get("status", True)
    serialized = brand_serializer(brand_data)

    await manager.broadcast({
        "type": "brand_updated",
        "data": serialized
    })

    return {"message": "Brand updated successfully!", "brand": serialized}


@router.delete("/delete_brand/{brand_id}")
async def delete_brand(brand_id: str):
    brand = brands_collection.find_one({"_id": ObjectId(brand_id)})
    if not brand:
        raise HTTPException(status_code=404, detail="Brand not found")

    try:
        if "logo" in brand and brand["logo"]:
            public_id = brand["logo_public_id"]
            if await upload_images.delete_image_from_server(public_id):
                result = brands_collection.delete_one({"_id": ObjectId(brand_id)})
                if result.deleted_count == 0:
                    raise HTTPException(status_code=404, detail="Brand not found")
                await manager.broadcast({
                    "type": "brand_deleted",
                    "data": {"_id": brand_id}
                })
                return {"message": "Brand deleted successfully!"}

    except Exception as e:
        # معالجة الأخطاء في حذف الصور
        print(f"Error deleting brand: {e}")


@router.patch("/edit_brand_status/{brand_id}")
async def edit_brand_status(brand_id: str, status: bool = Body(..., embed=True)):
    result = brands_collection.update_one(
        {"_id": ObjectId(brand_id)}, {"$set": {"status": status, "updatedAt": datetime.now(timezone.utc)}})
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Brand not found")

    updated_brand = brands_collection.find_one({"_id": ObjectId(brand_id)})

    await manager.broadcast({
        "type": "brand_updated",
        "data": brand_serializer(updated_brand)
    })

    return {"message": "Status updated successfully!", "brand": brand_serializer(updated_brand)}


# ====================================== Models Section ============================================

@router.get("/get_models/{brand_id}")
async def get_models(brand_id: str):
    models = list(models_collection.find({"brand_id": brand_id}))
    return {"models": [model_serializer(m) for m in models]}


@router.post("/add_new_model/{brand_id}")
async def add_new_model(brand_id: str, name: str = Form(...)):
    try:
        model_dict = {
            "name": name,
            "status": True,
            "brand_id": brand_id,
            "createdAt": datetime.now(timezone.utc),
            "updatedAt": datetime.now(timezone.utc),
        }

        insert_result = models_collection.insert_one(model_dict)
        model_dict["_id"] = str(insert_result.inserted_id)

        serialized = model_serializer(model_dict)
        await manager.broadcast({
            "type": "model_created",
            "data": serialized
        })

        return {"message": "Model created successfully!", "model": serialized}

    except Exception as e:
        return {"error": str(e)}


@router.patch("/edit_model_status/{model_id}")
async def edit_model_status(model_id: str, status: bool = Body(..., embed=True)):
    result = models_collection.update_one({"_id": ObjectId(model_id)},
                                          {"$set": {"status": status, "updatedAt": datetime.now(timezone.utc)}})
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Model not found")

    updated_model = models_collection.find_one({"_id": ObjectId(model_id)})

    await manager.broadcast({
        "type": "model_updated",
        "data": model_serializer(updated_model)
    })

    return {"message": "Status updated successfully!", "model": model_serializer(updated_model)}


@router.delete("/delete_model/{model_id}")
async def delete_model(model_id: str):
    try:
        models_collection.delete_one({"_id": ObjectId(model_id)})
        await manager.broadcast({
            "type": "model_deleted",
            "data": {"_id": model_id}
        })
        return {"message": "Model deleted successfully!"}

    except Exception as e:
        # معالجة الأخطاء في حذف الصور
        print(f"Error deleting model: {e}")

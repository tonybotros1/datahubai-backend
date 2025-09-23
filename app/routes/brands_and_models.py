from bson import ObjectId
from fastapi import APIRouter, HTTPException, Form, File, UploadFile, Body, Depends

from app.core import security
from app.database import get_collection
from datetime import datetime, timezone
from app.widgets import upload_images
from app.websocket_config import manager
from pymongo import ReturnDocument

router = APIRouter()
brands_collection = get_collection("all_brands")
models_collection = get_collection("all_brand_models")


# ====================================== Brands Section ============================================


def brand_serializer(brand: dict) -> dict:
    brand["_id"] = str(brand["_id"])
    for key, value in brand.items():
        if isinstance(value, datetime):
            brand[key] = value.isoformat()
    return brand


def model_serializer(model) -> dict:
    model["_id"] = str(model["_id"])
    model["brand_id"] = str(model["brand_id"])
    for key, value in model.items():
        if isinstance(value, datetime):
            model[key] = value.isoformat()
    return model


@router.get("/get_all_brands")
async def get_brands(_: dict = Depends(security.get_current_user)):
    brands = await brands_collection.find({}).sort("name", 1).to_list(length=None)
    return {"brands": [brand_serializer(b) for b in brands]}


from fastapi import Form, File, UploadFile
from datetime import datetime, timezone


@router.post("/add_new_brand")
async def create_brand(
        name: str = Form(None),  # name can be null
        logo: UploadFile = File(None),  # logo can be null
        _: dict = Depends(security.get_current_user)
):
    try:
        brand_dict = {
            "name": name,
            "logo": None,
            "logo_public_id": None,
            "status": True,
            "createdAt": datetime.now(timezone.utc),
            "updatedAt": datetime.now(timezone.utc),
        }

        # Upload logo only if provided
        if logo is not None:
            result = await upload_images.upload_image(logo, 'car_brands')
            brand_dict["logo"] = result["url"]
            brand_dict["logo_public_id"] = result["public_id"]

        insert_result = await brands_collection.insert_one(brand_dict)
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
async def update_brand(brand_id: str, name: str | None = Form(None), logo: UploadFile | None = File(None),
                       _: dict = Depends(security.get_current_user)):
    brand = await brands_collection.find_one({"_id": ObjectId(brand_id)})
    if not brand:
        raise HTTPException(status_code=404, detail="Brand not found")

    brand_data = {}

    if name:
        brand_data["name"] = name
    else:
        brand_data["name"] = brand["name"]

    if logo:
        if brand.get('logo'):
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
    await brands_collection.update_one({"_id": ObjectId(brand_id)}, {"$set": brand_data})
    brand_data['_id'] = ObjectId(brand_id)
    brand_data['createdAt'] = brand['createdAt']
    brand_data["status"] = brand.get("status", True)
    serialized = brand_serializer(brand_data)

    await manager.broadcast({
        "type": "brand_updated",
        "data": serialized
    })

    return {"message": "Brand updated successfully!", "brand": serialized}


from fastapi import HTTPException, Depends
from bson import ObjectId


@router.delete("/delete_brand/{brand_id}")
async def delete_brand(brand_id: str, _: dict = Depends(security.get_current_user)):
    # Find the brand first
    brand = await brands_collection.find_one({"_id": ObjectId(brand_id)})
    if not brand:
        raise HTTPException(status_code=404, detail="Brand not found")

    try:
        # If brand has a logo, try to delete it
        if brand.get("logo") and brand.get("logo_public_id"):
            await upload_images.delete_image_from_server(brand["logo_public_id"])

        # Delete the brand document regardless of logo
        result = await brands_collection.delete_one({"_id": ObjectId(brand_id)})
        if result.deleted_count == 0:
            raise HTTPException(status_code=404, detail="Brand not found")

        # Broadcast the deletion event
        await manager.broadcast({
            "type": "brand_deleted",
            "data": {"_id": brand_id}
        })

        return {"message": "Brand deleted successfully!"}

    except Exception as e:
        # Log and raise a 500 error if anything fails
        print(f"Error deleting brand: {e}")
        raise HTTPException(status_code=500, detail="Failed to delete brand")


@router.patch("/edit_brand_status/{brand_id}")
async def edit_brand_status(brand_id: str, status: bool = Body(..., embed=True),
                            _: dict = Depends(security.get_current_user)):
    result = await brands_collection.update_one(
        {"_id": ObjectId(brand_id)}, {"$set": {"status": status, "updatedAt": datetime.now(timezone.utc)}})
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Brand not found")

    updated_brand = await brands_collection.find_one({"_id": ObjectId(brand_id)})

    await manager.broadcast({
        "type": "brand_updated",
        "data": brand_serializer(updated_brand)
    })

    return {"message": "Status updated successfully!", "brand": brand_serializer(updated_brand)}


# ====================================== Models Section ============================================

@router.get("/get_models/{brand_id}")
async def get_models(brand_id: str, _: dict = Depends(security.get_current_user)):
    models = await models_collection.find({"brand_id": ObjectId(brand_id)}).sort("name", 1).to_list()
    return {"models": [model_serializer(m) for m in models]}


@router.post("/add_new_model/{brand_id}")
async def add_new_model(brand_id: str, name: str = Body(..., embed=True),
                        _: dict = Depends(security.get_current_user)
                        ):
    try:
        model_dict = {
            "name": name,
            "status": True,
            "brand_id": brand_id,
            "createdAt": datetime.now(timezone.utc),
            "updatedAt": datetime.now(timezone.utc),
        }

        insert_result = await models_collection.insert_one(model_dict)
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
async def edit_model_status(model_id: str, status: bool = Body(..., embed=True),
                            _: dict = Depends(security.get_current_user)):
    result = await models_collection.update_one({"_id": ObjectId(model_id)},
                                                {"$set": {"status": status, "updatedAt": datetime.now(timezone.utc)}})
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Model not found")

    updated_model = await models_collection.find_one({"_id": ObjectId(model_id)})

    await manager.broadcast({
        "type": "model_updated",
        "data": model_serializer(updated_model)
    })

    return {"message": "Status updated successfully!", "model": model_serializer(updated_model)}


@router.delete("/delete_model/{model_id}")
async def delete_model(model_id: str, _: dict = Depends(security.get_current_user)):
    try:
        await models_collection.delete_one({"_id": ObjectId(model_id)})
        await manager.broadcast({
            "type": "model_deleted",
            "data": {"_id": model_id}
        })
        return {"message": "Model deleted successfully!"}

    except Exception as e:
        # معالجة الأخطاء في حذف الصور
        print(f"Error deleting model: {e}")


@router.patch("/edit_model/{model_id}")
async def edit_model(model_id: str, name: str = Body(..., embed=True), _: dict = Depends(security.get_current_user)):
    updated_model = await models_collection.find_one_and_update(
        {"_id": ObjectId(model_id)},
        {"$set": {"updatedAt": datetime.now(timezone.utc), "name": name}},
        return_document=ReturnDocument.AFTER  # بترجع المستند بعد التعديل
    )

    if not updated_model:
        raise HTTPException(status_code=404, detail="Model not found")

    await manager.broadcast({
        "type": "model_updated",
        "data": model_serializer(updated_model)
    })

    return {"message": "Model updated successfully!", "model": model_serializer(updated_model)}


@router.get("/get_all_brands_by_status")
async def get_brands_by_status(_: dict = Depends(security.get_current_user)):
    brands = await brands_collection.find({"status": True}).sort("name", 1).to_list(length=None)
    return {"brands": [brand_serializer(b) for b in brands]}


@router.get("/get_models_by_status/{brand_id}")
async def get_models_by_status(brand_id: str, _: dict = Depends(security.get_current_user)):
    try:
        models = await models_collection.find({"brand_id": ObjectId(brand_id), "status": True}).sort("name", 1).to_list()
        return {"models": [model_serializer(m) for m in models]}
    except Exception as e:
        return {"error": str(e)}
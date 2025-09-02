from bson import ObjectId
from fastapi import APIRouter, HTTPException, Form, File, UploadFile
from app.database import get_collection
from app.schemas import BrandSchema
from datetime import datetime, timezone
from app.widgets import upload_images

router = APIRouter()
brands_collection = get_collection("all_brands")
models_collection = get_collection("all_models")


def brand_serializer(brand) -> dict:
    return {
        "_id": str(brand["_id"]),  # <-- حوّل ObjectId لـ str
        "name": brand["name"],
        "logo": brand["logo"],
        "status": brand.get("status", True),
        "createdAt": brand["createdAt"].isoformat(),
        "updatedAt": brand["updatedAt"].isoformat(),
    }


@router.get("/get_all_brands")
def get_brands():
    """إرجاع كل البراندز"""
    brands = list(brands_collection.find({}))
    return {"brands": [brand_serializer(b) for b in brands]}


@router.post("/add_new_brand")
async def create_brand(name: str = Form(...), logo: UploadFile = File(...)):
    try:
        # رفع اللوغو على Cloudinary
        result = await upload_images.upload_image(logo, 'car_brands')

        brand_dict = {
            "name": name,
            "logo": result["url"],
            "logo_public_id": result["public_id"],
            "status": True,
            "createdAt": datetime.now(timezone.utc),
            "updatedAt": datetime.now(timezone.utc),
        }

        brands_collection.insert_one(brand_dict)

        return {"message": "Brand created successfully!", "brand": brand_dict}
    except Exception as e:
        return {"error": str(e)}
    # brand_dict = brand.model_dump()
    # brand_dict["createdAt"] = datetime.now(timezone.utc)
    # brand_dict["updatedAt"] = datetime.now(timezone.utc)
    #
    # brands_collection.insert_one(brand_dict)


@router.put("/edit_brand/{brand_id}")
def update_brand(brand_id: str, brand: BrandSchema):
    """تعديل براند موجود"""
    brand_dict = brand.model_dump()
    brand_dict["updatedAt"] = datetime.now(timezone.utc)

    result = brands_collection.update_one(
        {"id": brand_id},
        {"$set": brand_dict}
    )
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Brand not found")
    return {"message": "Brand updated successfully!"}


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
                return {"message": "Brand deleted successfully!"}

    except Exception as e:
        # معالجة الأخطاء في حذف الصور
        print(f"Error deleting brand: {e}")

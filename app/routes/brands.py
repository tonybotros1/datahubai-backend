from fastapi import APIRouter, HTTPException
from database import get_collection
from schemas import BrandSchema
from datetime import datetime, timezone

router = APIRouter()
brands_collection = get_collection("all_brands")


@router.get("/")
def get_brands():
    """إرجاع كل البراندز"""
    brands = list(brands_collection.find({}, {"_id": 0}))
    return {"brands": brands}


@router.post("/")
def create_brand(brand: BrandSchema):
    """إضافة براند جديد"""
    if brands_collection.find_one({"id": brand.id}):
        raise HTTPException(status_code=400, detail="Brand with this ID already exists")

    brand_dict = brand.model_dump()
    brand_dict["createdAt"] = datetime.now(timezone.utc)
    brand_dict["updatedAt"] = datetime.now(timezone.utc)

    brands_collection.insert_one(brand_dict)
    return {"message": "Brand created successfully!"}


@router.put("/{brand_id}")
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


@router.delete("/{brand_id}")
def delete_brand(brand_id: str):
    """حذف براند"""
    result = brands_collection.delete_one({"id": brand_id})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Brand not found")
    return {"message": "Brand deleted successfully!"}

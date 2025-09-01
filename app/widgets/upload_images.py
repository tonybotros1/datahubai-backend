from fastapi import File, UploadFile, APIRouter
import cloudinary.uploader
from app.cloudinary_config import cloudinary

images = APIRouter()


@images.post("/")
async def upload_image(file: UploadFile = File(...), folder: str = "general"):
    try:
        # رفع الصورة على Cloudinary
        result = cloudinary.uploader.upload(file.file, folder=folder)

        # بترجع نتيجة فيها رابط مباشر للصورة
        return {"url": result["secure_url"]}
    except Exception as e:
        return {"error": str(e)}

from fastapi import File, UploadFile, APIRouter
import cloudinary.uploader
from app.cloudinary_config import cloudinary

images = APIRouter()


@images.post("/upload_image")
async def upload_image(file: UploadFile = File(...), folder: str = "general"):
    try:
        result = cloudinary.uploader.upload(file.file, folder=folder)

        return {"url": result["secure_url"], "public_id": result["public_id"],"file_name":file.filename}
    except Exception as e:
        return {"error": str(e)}


@images.post("/delete_image")
async def delete_image_from_server(public_id: str) -> bool:
    try:
        result = cloudinary.uploader.destroy(public_id)
        if result.get("result") != "ok":
            return False
        else:
            return True

    except Exception as e:
        print(f"Error deleting images from Cloudinary: {e}")
        return False

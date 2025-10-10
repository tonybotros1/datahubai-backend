from fastapi import File, UploadFile, HTTPException
import cloudinary.uploader
from app.cloudinary_config import cloudinary



async def upload_file(file: UploadFile = File(...), folder: str = "general"):
    try:
        # Upload to Cloudinary
        upload_result = cloudinary.uploader.upload(
            file.file,
            resource_type="auto",
            folder=folder
        )

        return {
            "message": "File uploaded successfully",
            "file_name": file.filename,
            "url": upload_result.get("secure_url"),
            "public_id": upload_result.get("public_id"),
            "resource_type": upload_result.get("resource_type"),
            "format": upload_result.get("format")
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Upload failed: {str(e)}")

import asyncio

from fastapi import File, UploadFile, HTTPException
import cloudinary.uploader
from app.cloudinary_config import cloudinary
from app.core import security


async def upload_file(file: UploadFile = File(...), folder: str = "general"):
    try:
        # Upload to Cloudinary
        upload_result = cloudinary.uploader.upload(
            file.file,
            resource_type="auto",
            folder=folder,
            public_id=f"{security.now_utc()}{file.filename}",
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



async def delete_file_from_server(public_id: str) -> bool:
    try:
        for resource_type in ["image", "video", "raw"]:
            result = await asyncio.to_thread(
                lambda: cloudinary.uploader.destroy(public_id, resource_type=resource_type)
            )
            if result.get("result") in ["ok", "not_found"]:
                return True
        return False
    except Exception as e:
        print(f"Error deleting file from Cloudinary: {e}")
        return False
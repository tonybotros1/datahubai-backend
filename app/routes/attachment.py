import copy
from typing import Optional, Dict, Any
from bson import ObjectId
from bson.errors import InvalidId
from fastapi import APIRouter, Depends, HTTPException, Body, Form, UploadFile, File
from pydantic import BaseModel
import json
from app.core import security
from app.database import get_collection
from datetime import datetime
from app.routes.car_trading import PyObjectId
from app.websocket_config import manager
from app.widgets import upload_images
from app.widgets.upload_files import delete_file_from_server
from app.widgets.upload_images import upload_image

router = APIRouter()
attachment_collection = get_collection("attachment")


class AttachmentModel(BaseModel):
    code: Optional[str] = None
    document_id: Optional[str] = None
    name: Optional[str] = None
    type: Optional[str] = None
    number: Optional[str] = None
    note: Optional[str] = None
    start_Date: Optional[datetime] = None
    end_Date: Optional[datetime] = None
    attachment: UploadFile = File(None)


class AttachmentSearchModel(BaseModel):
    code: Optional[str] = None
    document_id: Optional[PyObjectId] = None


attachment_details_pipeline = [
    {
        '$lookup': {
            'from': 'all_lists_values',
            'localField': 'attachment_type',
            'foreignField': '_id',
            'as': 'attachment_type_details'
        }
    }, {
        '$addFields': {
            '_id': {
                '$toString': '$_id'
            },
            'company_id': {
                '$toString': '$company_id'
            },
            'document_id': {
                '$toString': '$document_id'
            },
            'attachment_type': {
                '$toString': '$attachment_type'
            },
            'attachment_type_name': {
                '$ifNull': [
                    {
                        '$first': '$attachment_type_details.name'
                    }, None
                ]
            }
        }
    }, {
        '$project': {
            'attachment_type_details': 0
        }
    }
]


@router.post("/get_all_attachment")
async def get_all_attachment(attach_search: AttachmentSearchModel, data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        match_stage = {}
        if company_id:
            match_stage["company_id"] = company_id
        if attach_search.code:
            match_stage["code"] = attach_search.code
        if attach_search.document_id:
            match_stage["document_id"] = attach_search.document_id

        new_pipeline = copy.deepcopy(attachment_details_pipeline)
        new_pipeline.insert(0, {"$match": match_stage})
        cursor = await attachment_collection.aggregate(new_pipeline)
        results = await cursor.to_list(None)

        return {"results": results}

    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=str(e))


async def get_attachment_details(attachment_id: ObjectId):
    try:
        new_pipeline: Any = copy.deepcopy(attachment_details_pipeline)
        new_pipeline.insert(0, {"$match": {"_id": attachment_id}})
        cursor = await attachment_collection.aggregate(new_pipeline)
        results = await cursor.to_list(1)
        return results[0] if len(results) > 0 else None

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/add_new_attachment")
async def add_new_attachment(code: str = Form(None),
                             document_id: str = Form(None),
                             name: str = Form(None),
                             attachment: UploadFile = File(None),
                             start_date: datetime = Form(None),
                             end_date: datetime = Form(None),
                             note: str = Form(None),
                             number: str = Form(None),
                             attachment_type: str = Form(None),
                             data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        attach_public_id = None
        attach_url = None
        file_name = ""
        if attachment:
            result = await upload_image(attachment, folder="attachments")
            file_name = result["file_name"]
            attach_url = result["url"] if "url" in result else None
            attach_public_id = result['public_id'] if "public_id" in result else None

        attachment_doc = {
            "company_id": company_id,
            "document_id": ObjectId(document_id) if document_id else None,
            "code": code,
            "name": name,
            "start_date": start_date,
            "end_date": end_date,
            "note": note,
            "number": number,
            "attachment_type": ObjectId(attachment_type) if attachment_type else None,
            "file_name": file_name,
            "attach_public_id": attach_public_id,
            "attach_url": attach_url,
            "createdAt": security.now_utc(),
            "updatedAt": security.now_utc(),
        }
        new_attachment = await attachment_collection.insert_one(attachment_doc)
        new_attachment_details = await get_attachment_details(new_attachment.inserted_id)

        return {"result" : new_attachment_details}

    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/delete_attachment/{attachment_id}")
async def delete_attachment(
        attachment_id: str,
        _: dict = Depends(security.get_current_user),

):
    try:
        obj_id = ObjectId(attachment_id)
    except InvalidId:
        raise HTTPException(status_code=400, detail="Invalid attachment id")

    attachment = await attachment_collection.find_one({"_id": obj_id})
    if not attachment:
        raise HTTPException(status_code=404, detail="Attachment not found")

    attach_public_id = attachment.get("attach_public_id")
    if attach_public_id:
        deleted = await delete_file_from_server(attach_public_id)
        if not deleted:
            raise HTTPException(status_code=500, detail="Failed to delete file from storage")

    result = await attachment_collection.delete_one({"_id": obj_id})
    if result.deleted_count != 1:
        raise HTTPException(status_code=500, detail="Failed to delete attachment")

    return {"_id": str(obj_id)}

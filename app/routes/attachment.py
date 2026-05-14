import copy
from typing import Optional, Any, List
from bson import ObjectId
from bson.errors import InvalidId
from fastapi import APIRouter, Depends, HTTPException, Form, UploadFile, File
from pydantic import BaseModel
from app.core import security
from app.database import get_collection
from datetime import datetime
from app.widgets.upload_files import delete_file_from_server, upload_file

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
    document_id: Optional[str] = None


def optional_object_id(value: Optional[str], field_name: str) -> Optional[ObjectId]:
    raw_value = "" if value is None else str(value).strip()
    if raw_value == "" or raw_value.lower() == "null":
        return None
    try:
        return ObjectId(raw_value)
    except InvalidId:
        raise HTTPException(status_code=400, detail=f"Invalid {field_name}")


def parse_optional_form_datetime(value: Optional[str], field_name: str) -> Optional[datetime]:
    if value is None or str(value).strip() == "" or str(value).strip().lower() == "null":
        return None

    raw_value = str(value).strip()
    try:
        return datetime.fromisoformat(raw_value.replace("Z", "+00:00"))
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid {field_name}")


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
            match_stage["document_id"] = optional_object_id(attach_search.document_id, "document id")

        new_pipeline = copy.deepcopy(attachment_details_pipeline)
        new_pipeline.insert(0, {"$match": match_stage})
        cursor = await attachment_collection.aggregate(new_pipeline)
        results = await cursor.to_list(None)

        return {"results": results}

    except HTTPException:
        raise
    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=str(e))


async def get_attachment_details(attachment_id: ObjectId, company_id: Optional[ObjectId] = None):
    try:
        match_stage = {"_id": attachment_id}
        if company_id:
            match_stage["company_id"] = company_id

        new_pipeline: Any = copy.deepcopy(attachment_details_pipeline)
        new_pipeline.insert(0, {"$match": match_stage})
        cursor = await attachment_collection.aggregate(new_pipeline)
        results = await cursor.to_list(1)
        return results[0] if len(results) > 0 else None

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/add_new_attachment")
async def add_new_attachment(code: str = Form(None),
                             document_id: str = Form(None),
                             name: str = Form(None),
                             attachments: Optional[List[UploadFile]] = File(None),
                             start_date: Optional[str] = Form(None),
                             end_date: Optional[str] = Form(None),
                             note: str = Form(None),
                             number: str = Form(None),
                             attachment_type: str = Form(None),
                             data: dict = Depends(security.get_current_user)):
    try:
        if not code or not code.strip():
            raise HTTPException(status_code=400, detail="Attachment code is required")
        if not document_id or not document_id.strip():
            raise HTTPException(status_code=400, detail="Document id is required")
        if not name or not name.strip():
            raise HTTPException(status_code=400, detail="Name is required")
        if not number or not number.strip():
            raise HTTPException(status_code=400, detail="Number is required")
        if not attachment_type or not attachment_type.strip():
            raise HTTPException(status_code=400, detail="Attachment type is required")
        if not attachments:
            raise HTTPException(status_code=400, detail="At least one attachment is required")

        company_id = ObjectId(data.get("company_id"))
        document_object_id = optional_object_id(document_id, "document id")
        attachment_type_id = optional_object_id(attachment_type, "attachment type")
        start_date_value = parse_optional_form_datetime(start_date, "start date")
        end_date_value = parse_optional_form_datetime(end_date, "end date")
        attachments_list = []
        uploaded_public_ids = []
        for attachment in attachments:
            result = await upload_file(attachment, folder="attachments")
            file_name = result.get("file_name")
            attach_url = result.get("url")
            attach_public_id = result.get("public_id")
            if not attach_url or not attach_public_id:
                for public_id in uploaded_public_ids:
                    await delete_file_from_server(public_id)
                raise HTTPException(status_code=500, detail="Failed to upload attachment")

            uploaded_public_ids.append(attach_public_id)
            attachments_list.append({
                "attach_url": attach_url,
                "attach_public_id": attach_public_id,
                "file_name": file_name,
                "resource_type": result.get("resource_type"),
                "format": result.get("format"),
            })

        attachment_doc = {
            "company_id": company_id,
            "document_id": document_object_id,
            "code": code.strip(),
            "name": name.strip(),
            "start_date": start_date_value,
            "end_date": end_date_value,
            "note": note,
            "number": number.strip(),
            "attachment_type": attachment_type_id,
            "attachments": attachments_list,
            "createdAt": security.now_utc(),
            "updatedAt": security.now_utc(),
        }
        new_attachment = await attachment_collection.insert_one(attachment_doc)
        new_attachment_details = await get_attachment_details(new_attachment.inserted_id, company_id)

        return {"result": new_attachment_details}

    except HTTPException:
        raise
    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/delete_attachment/{attachment_id}")
async def delete_attachment(
        attachment_id: str,
        data: dict = Depends(security.get_current_user),

):
    try:
        obj_id = ObjectId(attachment_id)
    except InvalidId:
        raise HTTPException(status_code=400, detail="Invalid attachment id")

    company_id = ObjectId(data.get("company_id"))
    match_stage = {"_id": obj_id, "company_id": company_id}

    attachment = await attachment_collection.find_one(match_stage)
    if not attachment:
        raise HTTPException(status_code=404, detail="Attachment not found")

    delete_failures = []
    attachments = attachment.get("attachments")
    if attachments:
        for element in attachments:
            attach_public_id = element.get("attach_public_id")
            deleted_from_server = await delete_file_from_server(attach_public_id)
            if not deleted_from_server:
                delete_failures.append(element.get("file_name") or attach_public_id)

    if delete_failures:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to delete files from storage: {', '.join(delete_failures)}"
        )

    result = await attachment_collection.delete_one(match_stage)
    if result.deleted_count != 1:
        raise HTTPException(status_code=500, detail="Failed to delete attachment")

    return {"_id": str(obj_id)}

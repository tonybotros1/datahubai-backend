from typing import Optional
from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from pymongo import ReturnDocument

from app.core import security
from app.database import get_collection
from datetime import datetime
from app.websocket_config import manager

router = APIRouter()
sys_variables_collection = get_collection("system_variables")


def serializer(variable: dict) -> dict:
    variable["_id"] = str(variable["_id"])
    for key, value in variable.items():
        if isinstance(value, datetime):
            variable[key] = value.isoformat()
    return variable


class SystemVariablesModel(BaseModel):
    code: Optional[str] = None
    value: Optional[str] = None


@router.get("/get_all_sys_variables")
async def get_all_sys_variables(_: dict = Depends(security.get_current_user)):
    try:
        results = await sys_variables_collection.find({}).sort("code", 1).to_list()
        return {"results": [serializer(r) for r in results]}

    except Exception as e:
        raise e


@router.post("/add_new_variable")
async def add_new_variable(variable: SystemVariablesModel, _: dict = Depends(security.get_current_user)):
    try:
        variable = variable.model_dump(exclude_unset=True)
        variable_dict = {
            "value": variable["value"],
            "code": variable["code"],
            "createdAt": security.now_utc(),
            "updatedAt": security.now_utc(),
        }
        result = await sys_variables_collection.insert_one(variable_dict)
        variable_dict["_id"] = result.inserted_id
        serialized = serializer(variable_dict)
        await manager.broadcast({
            "type": "variable_added",
            "data": serialized
        })

    except Exception as e:
        raise e


@router.patch("/update_variable/{variable_id}")
async def update_variable(variable_id: str, variable: SystemVariablesModel,
                          _: dict = Depends(security.get_current_user)):
    try:
        if not ObjectId.is_valid(variable_id):
            raise HTTPException(status_code=400, detail="Invalid variable ID")
        variable_data = variable.model_dump(exclude_unset=True)
        if not variable_data:
            raise HTTPException(status_code=400, detail="No fields provided to update")
        result = await sys_variables_collection.find_one_and_update(
            {"_id": ObjectId(variable_id)},
            {"$set": variable_data},
            return_document=ReturnDocument.AFTER
        )
        if not result:
            raise HTTPException(status_code=404, detail="Variable not found")
        serialized = serializer(result)
        await manager.broadcast({
            "type": "variable_updated",
            "data": serialized
        })
        return {"status": "success", "data": serialized}

    except HTTPException:
        raise
    except Exception as e:
        # Log the error for debugging
        print("Error updating variable:", e)
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/delete_variable/{variable_id}")
async def delete_variable(variable_id: str, _: dict = Depends(security.get_current_user)):
    try:
        variable_id = ObjectId(variable_id)
        result = await sys_variables_collection.delete_one({"_id": variable_id})
        if result.deleted_count == 0:
            raise HTTPException(status_code=404, detail="Variable not found")
        await manager.broadcast({
            "type": "variable_deleted",
            "data": {"_id": str(variable_id)}
        })

    except Exception as e:
        raise e

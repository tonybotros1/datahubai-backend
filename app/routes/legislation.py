from typing import Optional, Any, List
from datetime import datetime
from bson import ObjectId
from bson.errors import InvalidId
from fastapi import APIRouter, HTTPException, Depends
from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel
from app.core import security
from app.database import get_collection
from app.websocket_config import manager

router = APIRouter()
legislations_collection = get_collection("legislations")
employees_collection = get_collection("employees")
employees_payrolls_collection = get_collection("employees_payrolls")
payroll_elements_collection = get_collection("payroll_elements")


class IncomeTaxBracketModel(BaseModel):
    from_amount: Optional[float] = None
    to_amount: Optional[float] = None
    percentage: Optional[float] = None


class SocialSecurityCeilingModel(BaseModel):
    employee_percentage: Optional[float] = None
    employer_percentage: Optional[float] = None
    ceiling: Optional[float] = None
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None


class LegislationModel(BaseModel):
    name: Optional[str] = None
    weekend: Optional[List[str]] = None
    number_of_paid_days_for_sick_leave: Optional[int] = None
    number_of_half_paid_days_for_sick_leave: Optional[int] = None
    number_of_unpaid_days_for_sick_leave: Optional[int] = None
    number_of_paid_days_for_maternity_leave: Optional[int] = None
    number_of_paid_days_for_compassionate_leave: Optional[int] = None
    number_of_paid_days_for_paternity_leave: Optional[int] = None
    number_of_working_hours_for_overtime_normal: Optional[float] = None
    number_of_working_hours_for_overtime_holidays: Optional[float] = None
    social_security_employee_percentage: Optional[float] = None
    social_security_employer_percentage: Optional[float] = None
    social_security_ceiling: Optional[float] = None
    social_security_ceiling_start_date: Optional[datetime] = None
    social_security_ceiling_end_date: Optional[datetime] = None
    social_security_ceilings: Optional[List[SocialSecurityCeilingModel]] = None
    service_tax_percentage: Optional[float] = None
    income_tax_percentage: Optional[float] = None
    income_tax_ceiling: Optional[float] = None
    income_tax_brackets: Optional[List[IncomeTaxBracketModel]] = None
    gratuity_first_5_years: Optional[int] = None
    gratuity_after_5_years: Optional[int] = None


class SearchModel(BaseModel):
    name: Optional[str] = None


@router.get("/get_all_legislations")
async def get_all_legislations(data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        results = await legislations_collection.find({"company_id": company_id}, {
            "name": 1,
        }).to_list(None)
        return {
            "all_legislations": jsonable_encoder(
                results,
                custom_encoder={ObjectId: str}
            )
        }


    except Exception as e:
        raise HTTPException(status_code=500, detail=f"str{e}")


@router.post("/add_new_legislation")
async def add_new_legislation(leg: LegislationModel, data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        leg = leg.model_dump(exclude_unset=True)
        leg['company_id'] = company_id
        leg['createdAt'] = security.now_utc()
        leg['updatedAt'] = security.now_utc()
        new_leg = await legislations_collection.insert_one(leg)

        leg['_id'] = str(new_leg.inserted_id)
        leg['company_id'] = str(company_id)
        leg = jsonable_encoder(leg)  # 🔥 this fixes datetime + ObjectId

        await manager.send_to_company(str(company_id), {
            "type": "leg_added",
            "data": leg
        })
        return {"message": "added successfully!", "new_leg": leg}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.patch("/update_legislation/{leg_id}")
async def update_legislation(leg_id: str, leg: LegislationModel, data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        leg = leg.model_dump(exclude_unset=True)
        leg['updatedAt'] = security.now_utc()
        updated_leg = await legislations_collection.update_one({"_id": ObjectId(leg_id)}, {"$set": leg})

        if updated_leg.matched_count == 0:
            raise HTTPException(status_code=404, detail="legislation not found")
        leg['_id'] = str(leg_id)
        leg = jsonable_encoder(leg)

        await manager.send_to_company(str(company_id), {
            "type": "leg_updated",
            "data": leg
        })
        return {"message": "updated successfully!", "updated_leg": leg}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/delete_legislation/{leg_id}")
async def delete_legislation(leg_id: str, data: dict = Depends(security.get_current_user)):
    try:
        company_id = data.get("company_id")
        result = await legislations_collection.delete_one({"_id": ObjectId(leg_id)})
        if result.deleted_count == 1:
            await manager.send_to_company(str(company_id), {
                "type": "leg_deleted",
                "data": {"_id": leg_id}
            })
            return {"message": "Element removed successfully!"}
        else:
            raise HTTPException(status_code=404, detail="Branch not found")

    except Exception as error:
        return {"message": str(error)}


@router.post("/search_engine_for_legislations")
async def search_engine_for_legislations(
        filters: SearchModel,
        data: dict = Depends(security.get_current_user)
):
    try:
        company_id = ObjectId(data.get("company_id"))
        match_stage: Any = {}
        if company_id:
            match_stage["company_id"] = company_id
        if filters.name:
            match_stage["name"] = {"$regex": filters.name, "$options": "i"}

        legislations_elements_pipeline = [
            {"$match": match_stage},
            {"$set": {
                "_id": {
                    "$toString": "$_id"
                }
            }},
            {"$project": {
                "company_id": 0,

            }}
        ]
        cursor = await legislations_collection.aggregate(legislations_elements_pipeline)
        legislations_elements = await cursor.to_list(None)
        return {"legislations_elements": legislations_elements if legislations_elements else []}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{legislation_id}/social_security_employee_values")
async def get_social_security_employee_values(
        legislation_id: str,
        data: dict = Depends(security.get_current_user)
):
    """Return employee-entered ceilings for the Social Security Employee element."""
    try:
        company_id = ObjectId(data.get("company_id"))
        legislation_object_id = ObjectId(legislation_id)
    except (InvalidId, TypeError):
        raise HTTPException(status_code=400, detail="Invalid legislation")

    legislation = await legislations_collection.find_one({
        "_id": legislation_object_id,
        "company_id": company_id,
    }, {"_id": 1})
    if not legislation:
        raise HTTPException(status_code=404, detail="Legislation not found")

    social_security_elements = await payroll_elements_collection.find({
        "company_id": company_id,
        "function": {
            "$regex": "^PY_SOCIAL_SECURITY_EMPLOYEE_FF$",
            "$options": "i",
        },
    }, {"_id": 1}).to_list(None)
    element_ids = [element["_id"] for element in social_security_elements]
    if not element_ids:
        return {"employee_values": []}

    pipeline = [
        {
            "$match": {
                "company_id": company_id,
                "name": {"$in": element_ids},
            }
        },
        {
            "$set": {
                "has_override": {
                    "$and": [
                        {"$ne": [{"$type": "$value"}, "missing"]},
                        {"$ne": ["$value", None]},
                    ]
                }
            }
        },
        {
            "$lookup": {
                "from": "employees",
                "let": {"employee_id": "$employee_id"},
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$and": [
                                    {"$eq": ["$_id", "$$employee_id"]},
                                    {"$eq": ["$company_id", company_id]},
                                    {"$eq": ["$legislation", legislation_object_id]},
                                ]
                            }
                        }
                    },
                    {
                        "$project": {
                            "full_name": 1,
                            "people_counter": 1,
                            "social_security_registration_number": 1,
                        }
                    },
                ],
                "as": "employee",
            }
        },
        {"$unwind": "$employee"},
        {
            "$lookup": {
                "from": "payroll_elements",
                "let": {"element_id": "$name"},
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$and": [
                                    {"$eq": ["$_id", "$$element_id"]},
                                    {"$eq": ["$company_id", company_id]},
                                ]
                            }
                        }
                    },
                    {"$project": {"name": 1}},
                ],
                "as": "payroll_element",
            }
        },
        {
            "$project": {
                "_id": {"$toString": "$_id"},
                "employee_id": {"$toString": "$employee_id"},
                "employee_name": "$employee.full_name",
                "employee_number": "$employee.people_counter",
                "social_security_registration_number": (
                    "$employee.social_security_registration_number"
                ),
                "payroll_element_name": {
                    "$ifNull": [
                        {"$first": "$payroll_element.name"},
                        "Social Security Employee",
                    ]
                },
                "value": 1,
                "has_override": 1,
                "start_date": 1,
                "end_date": 1,
            }
        },
        {"$sort": {"has_override": -1, "employee_name": 1, "start_date": -1}},
    ]
    cursor = await employees_payrolls_collection.aggregate(pipeline)
    employee_values = await cursor.to_list(None)
    return {
        "employee_values": jsonable_encoder(
            employee_values,
            custom_encoder={ObjectId: str},
        )
    }


@router.patch(
    "/{legislation_id}/social_security_employee_values/{assignment_id}/clear_override"
)
async def clear_social_security_employee_override(
        legislation_id: str,
        assignment_id: str,
        data: dict = Depends(security.get_current_user)
):
    try:
        company_id = ObjectId(data.get("company_id"))
        legislation_object_id = ObjectId(legislation_id)
        assignment_object_id = ObjectId(assignment_id)
    except (InvalidId, TypeError):
        raise HTTPException(status_code=400, detail="Invalid social security assignment")

    legislation = await legislations_collection.find_one({
        "_id": legislation_object_id,
        "company_id": company_id,
    }, {"_id": 1})
    if not legislation:
        raise HTTPException(status_code=404, detail="Legislation not found")

    assignment = await employees_payrolls_collection.find_one({
        "_id": assignment_object_id,
        "company_id": company_id,
    }, {"employee_id": 1, "name": 1})
    if not assignment:
        raise HTTPException(status_code=404, detail="Social security assignment not found")

    element = await payroll_elements_collection.find_one({
        "_id": assignment.get("name"),
        "company_id": company_id,
        "function": {
            "$regex": "^PY_SOCIAL_SECURITY_EMPLOYEE_FF$",
            "$options": "i",
        },
    }, {"_id": 1})
    if not element:
        raise HTTPException(status_code=404, detail="Social security assignment not found")

    employee = await employees_collection.find_one({
        "_id": assignment.get("employee_id"),
        "company_id": company_id,
        "legislation": legislation_object_id,
    }, {"_id": 1})
    if not employee:
        raise HTTPException(status_code=404, detail="Social security assignment not found")

    result = await employees_payrolls_collection.update_one(
        {
            "_id": assignment_object_id,
            "company_id": company_id,
            "employee_id": assignment.get("employee_id"),
            "name": assignment.get("name"),
        },
        {
            "$unset": {"value": ""},
            "$set": {"updatedAt": security.now_utc()},
        },
    )
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Social security assignment not found")
    return {"cleared_assignment_id": str(assignment_object_id)}

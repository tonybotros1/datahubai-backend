from typing import Optional, List
from bson import ObjectId
from fastapi import APIRouter, Body, HTTPException, Depends
from pydantic import BaseModel
from pymongo import ReturnDocument
from app.core import security
from app.database import get_collection
from datetime import datetime, timezone
from app.websocket_config import manager

router = APIRouter()
patients_collection = get_collection("patients")
medications_collection = get_collection("medications")
medications_schedule_collection = get_collection("medications_schedule")
timezones_collection = get_collection("timezones")


class PatientModel(BaseModel):
    patient_name: Optional[str] = None
    timezone: Optional[str] = None


class MedicationModel(BaseModel):
    medication_name: Optional[str] = None


class MedicationScheduleModel(BaseModel):
    patient_id: Optional[str] = None
    medication_id: Optional[str] = None
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    times: Optional[List[str]] = None


class TimeZoneModel(BaseModel):
    timezone: Optional[str] = None



# [
#   {
#     $match: {
#       patient_id: ObjectId(
#         "69d030abd059d33747d37893"
#       ),
#       $expr: { $gt: ["$end_date", "$$NOW"] }
#     }
#   },
#
#   {
#     $lookup: {
#       from: "patients",
#       localField: "patient_id",
#       foreignField: "_id",
#       as: "patient_details"
#     }
#   },
#   {
#     $unwind: {
#       path: "$patient_details",
#       preserveNullAndEmptyArrays: true
#     }
#   },
#   {
#     $addFields: {
#       timeones_ids: [
#         "$patient_details.home_timezone",
#         "$patient_details.current_timezone"
#       ]
#     }
#   },
#   {
#     $lookup: {
#       from: "timezones",
#       let: {
#         ids: "$timeones_ids"
#       },
#       pipeline: [
#         {
#           $match: {
#             $expr: {
#               $in: ["$_id", "$$ids"]
#             }
#           }
#         }
#       ],
#       as: "timezones_data"
#     }
#   },
#   {
#     $addFields: {
#       home_timezone: {
#         $let: {
#           vars: {
#             match: {
#               $first: {
#                 $filter: {
#                   input: "$timezones_data",
#                   cond: {
#                     $eq: [
#                       "$$this._id",
#                       "$patient_details.home_timezone"
#                     ]
#                   }
#                 }
#               }
#             }
#           },
#           in: "$$match.timezone"
#         }
#       },
#       current_timezone: {
#         $let: {
#           vars: {
#             match: {
#               $first: {
#                 $filter: {
#                   input: "$timezones_data",
#                   cond: {
#                     $eq: [
#                       "$$this._id",
#                       "$patient_details.current_timezone"
#                     ]
#                   }
#                 }
#               }
#             }
#           },
#           in: "$$match.timezone"
#         }
#       }
#     }
#   },
#   {
#     $unwind: {
#       path: "$times",
#       preserveNullAndEmptyArrays: true
#     }
#   },
#   {
#     $addFields: {
#       UTC_time: {
#         $dateFromString: {
#           dateString: {
#             $concat: [
#               {
#                 $dateToString: {
#                   format: "%Y-%m-%d",
#                   date: "$$NOW"
#                 }
#               },
#               " ",
#               "$times"
#             ]
#           },
#           timezone: "$home_timezone"
#         }
#       }
#       // tttt: "$$NOW"
#     }
#   },
#   {
#     $addFields: {
#       display_time: {
#         $dateToString: {
#           date: "$UTC_time",
#           timezone: "$current_timezone",
#           format: "%H:%M"
#         }
#       }
#     }
#   },
#
#   {
#     $sort: {
#       UTC_time: 1
#     }
#   }
# ]


# ================ ADD NEW PATIENT SECTION ================
@router.post("/add_new_patient")
async def add_new_patient(patient: PatientModel):
    try:
        patient = patient.model_dump(exclude_unset=True)
        if not patient:
            raise HTTPException(status_code=400, detail="no data found")
        patient['current_timezone'] = ObjectId(patient['timezone']) if "timezone" in patient else None
        patient['home_timezone'] = ObjectId(patient['timezone']) if "timezone" in patient else None
        patient['createdAt'] = security.now_utc()
        patient['updatedAt'] = security.now_utc()
        new_patient = await patients_collection.insert_one(patient)
        if not new_patient.inserted_id:
            raise HTTPException(status_code=500, detail="failed to create new patient")
        patient['_id'] = str(new_patient.inserted_id)
        if "home_timezone" in patient:
            patient['home_timezone'] = str(patient['home_timezone'])
        if "current_timezone" in patient:
            patient['current_timezone'] = str(patient['current_timezone'])
        patient['_id'] = str(new_patient.inserted_id)
        return {"new_patient": patient}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ================ ADD NEW MEDICATION SECTION ================
@router.post("/add_new_medication")
async def add_new_medication(medication: MedicationModel):
    try:
        medication = medication.model_dump(exclude_unset=True)
        if not medication:
            raise HTTPException(status_code=400, detail="no data found")
        medication['createdAt'] = datetime.now(timezone.utc)
        medication['updatedAt'] = datetime.now(timezone.utc)
        new_medication = await medications_collection.insert_one(medication)
        if not new_medication.inserted_id:
            raise HTTPException(status_code=500, detail="failed to create new medication")
        medication['_id'] = str(new_medication.inserted_id)
        return {"new_medication": medication}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ================ ADD NEW MEDICATION SCHEDULE SECTION ================
@router.post("/add_new_medication_schedule")
async def add_new_medication_schedule(medication_schedule: MedicationScheduleModel):
    try:
        medication_schedule = medication_schedule.model_dump(exclude_unset=True)
        if not medication_schedule:
            raise HTTPException(status_code=400, detail="no data found")
        medication_schedule['patient_id'] = ObjectId(medication_schedule['patient_id']) if 'patient_id' in medication_schedule else None
        medication_schedule['medication_id'] = ObjectId(medication_schedule['medication_id']) if 'medication_id' in medication_schedule else None
        medication_schedule['createdAt'] = datetime.now(timezone.utc)
        medication_schedule['updatedAt'] = datetime.now(timezone.utc)
        new_medication_schedule = await medications_schedule_collection.insert_one(medication_schedule)
        if not new_medication_schedule.inserted_id:
            raise HTTPException(status_code=500, detail="failed to create new medication schedule")
        medication_schedule['_id'] = str(new_medication_schedule.inserted_id)
        if "patient_id" in medication_schedule:
            medication_schedule['patient_id'] = str(medication_schedule['patient_id'])
        if "medication_id" in medication_schedule:
            medication_schedule['medication_id'] = str(medication_schedule['medication_id'])
        return {"new_medication_schedule": medication_schedule}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ================ ADD NEW TIMEZONES SECTION ================
@router.post("/add_new_timezone")
async def add_new_timezone(timezone_data: TimeZoneModel):
    try:
        timezone_data = timezone_data.model_dump(exclude_unset=True)
        if not timezone_data:
            raise HTTPException(status_code=400, detail="no data found")
        timezone_data['createdAt'] = datetime.now(timezone.utc)
        timezone_data['updatedAt'] = datetime.now(timezone.utc)
        new_timezone = await timezones_collection.insert_one(timezone_data)
        if not new_timezone.inserted_id:
            raise HTTPException(status_code=500, detail="failed to create new timezone")
        timezone_data['_id'] = str(new_timezone.inserted_id)
        return {"new_timezone": timezone_data}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))



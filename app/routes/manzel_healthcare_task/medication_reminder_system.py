from fastapi import APIRouter
from app.database import get_collection
import math
from datetime import date, datetime, timedelta, timezone
from typing import Any, Optional
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
from bson import ObjectId
from fastapi import HTTPException
from app.routes.manzel_healthcare_task.helpers_functions import normalize_clock_time, parse_clock_time, \
    roundtrip_matches, ensure_utc_datetime, utc_now, patient_local_anchor_to_utc
from app.routes.manzel_healthcare_task.models import PatientModel, MedicationModel, TimeZoneModel, \
    MedicationScheduleFixedTimeModel, MedicationScheduleIntervalHoursModel

UTC = timezone.utc
router = APIRouter()
patients_collection = get_collection("patients")
medications_collection = get_collection("medications")
medications_schedule_collection = get_collection("medications_schedule")
timezones_collection = get_collection("timezones")


# ================ ADD NEW PATIENT SECTION ================
@router.post("/add_new_patient")
async def add_new_patient(patient: PatientModel):
    try:
        if not patient.patient_name:
            raise HTTPException(status_code=400, detail="Patient name is required")
        if not patient.current_timezone:
            raise HTTPException(status_code=400, detail="Patient timezone is required")
        if not ObjectId.is_valid(patient.current_timezone):
            raise HTTPException(status_code=400, detail="Invalid current_timezone_id")
        patient_doc = {
            "patient_name": patient.patient_name,
            "current_timezone": ObjectId(patient.current_timezone),
            "createdAt": utc_now(),
            "updatedAt": utc_now(),
        }
        new_patient = await patients_collection.insert_one(patient_doc)
        if not new_patient.inserted_id:
            raise HTTPException(status_code=500, detail="failed to create new patient document")
        patient_doc['_id'] = (str(new_patient.inserted_id))
        patient_doc['current_timezone'] = (str(patient_doc['current_timezone']))

        return {"patient_details": patient_doc}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# =====================================================================


# ====================== UPDATE PATIENT SECTION =======================
@router.patch("/update_patient/{patient_id}")
async def update_patient(patient_id: str, patient: PatientModel):
    try:
        if not patient_id:
            raise HTTPException(status_code=400, detail="Patient id is required")
        if not ObjectId.is_valid(patient_id):
            raise HTTPException(status_code=400, detail="Invalid patient_id")
        patient_id = ObjectId(patient_id)
        update_fields = {}
        if patient.patient_name is not None:
            if not patient.patient_name:
                raise HTTPException(status_code=400, detail="Patient name cannot be empty")
            update_fields["patient_name"] = patient.patient_name
        if patient.current_timezone is not None:
            if not ObjectId.is_valid(patient.current_timezone):
                raise HTTPException(status_code=400, detail="Invalid current_timezone_id")
            update_fields["current_timezone"] = ObjectId(patient.current_timezone)
        if not update_fields:
            raise HTTPException(status_code=400, detail="No fields provided to update")
        update_fields["updatedAt"] = utc_now()
        updated_patient = await patients_collection.update_one(
            {"_id": patient_id},
            {"$set": update_fields}
        )
        if updated_patient.matched_count == 0:
            raise HTTPException(status_code=404, detail="Patient not found")
        update_fields["_id"] = str(patient_id)
        if "current_timezone" in update_fields:
            update_fields["current_timezone"] = str(update_fields["current_timezone"])
        return {"patient_details": update_fields}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# =====================================================================


# ==================== ADD NEW MEDICATION SECTION =====================
@router.post("/add_new_medication")
async def add_new_medication(medication: MedicationModel):
    try:
        if not medication.medication_name:
            raise HTTPException(status_code=400, detail="Medication name is required")

        medication_doc = {
            "medication_name": medication.medication_name,
            "createdAt": utc_now(),
            "updatedAt": utc_now(),
        }
        new_medication = await medications_collection.insert_one(medication_doc)
        if not new_medication.inserted_id:
            raise HTTPException(status_code=500, detail="failed to create new medication document")
        medication_doc['_id'] = (str(new_medication.inserted_id))
        return {"new_medication": medication_doc}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# =====================================================================


# ===================== ADD NEW TIMEZONES SECTION =====================
@router.post("/add_new_timezone")
async def add_new_timezone(timezone_data: TimeZoneModel):
    try:
        if not timezone_data.timezone:
            raise HTTPException(status_code=400, detail="Timezone data is required")

        timezone_doc = {
            "timezone": timezone_data.timezone,
            "createdAt": utc_now(),
            "updatedAt": utc_now(),
        }
        new_timezone = await timezones_collection.insert_one(timezone_doc)
        if not new_timezone.inserted_id:
            raise HTTPException(status_code=500, detail="failed to create new timezone document")
        timezone_doc['_id'] = (str(new_timezone.inserted_id))
        return {"new_timezone": timezone_doc}


    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# =====================================================================


# ================ ADD NEW MEDICATION SCHEDULE SECTION ================
# =====================================================================
# =================== ADD NEW FIXED TIME MEDICATION ===================
@router.post("/add_new_medication_schedule_fixed_time")
async def add_new_medication_schedule_fixed_time(medication_schedule: MedicationScheduleFixedTimeModel):
    try:
        if not ObjectId.is_valid(medication_schedule.patient_id):
            raise HTTPException(status_code=400, detail="Invalid patient_id")
        if not ObjectId.is_valid(medication_schedule.medication_id):
            raise HTTPException(status_code=400, detail="Invalid medication_id")

        patient = await patients_collection.find_one({"_id": ObjectId(medication_schedule.patient_id)})
        if not patient:
            raise HTTPException(status_code=404, detail="Patient not found")

        medication = await medications_collection.find_one({"_id": ObjectId(medication_schedule.medication_id)})
        if not medication:
            raise HTTPException(status_code=404, detail="Medication not found")

        doc = {
            "patient_id": ObjectId(medication_schedule.patient_id),
            "medication_id": ObjectId(medication_schedule.medication_id),
            "schedule_type": "fixed_times",
            "start_date": medication_schedule.start_date,
            "end_date": medication_schedule.end_date,
            "times": medication_schedule.times,
            "dst_gap_policy": medication_schedule.dst_gap_policy,
            "dst_overlap_policy": medication_schedule.dst_overlap_policy,
            "createdAt": utc_now(),
            "updatedAt": utc_now(),
        }

        result = await medications_schedule_collection.insert_one(doc)
        doc["_id"] = result.inserted_id

        return {
            "schedule": {
                "id": str(doc["_id"]),
                "schedule_type": doc["schedule_type"],
                "times": doc["times"],
            }
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# =====================================================================

# ================= ADD NEW INTERVAL HOURS MEDICATION =================
@router.post("/add_new_medication_schedule_interval_hours")
async def add_new_medication_schedule_interval_hours(medication_schedule: MedicationScheduleIntervalHoursModel):
    if not ObjectId.is_valid(medication_schedule.patient_id):
        raise HTTPException(status_code=400, detail="Invalid patient_id")
    if not ObjectId.is_valid(medication_schedule.medication_id):
        raise HTTPException(status_code=400, detail="Invalid medication_id")
    if medication_schedule.interval_hours <= 0:
        raise HTTPException(status_code=400, detail="interval_hours must be greater than 0")

    patient = await patients_collection.find_one({"_id": ObjectId(medication_schedule.patient_id)})
    if not patient:
        raise HTTPException(status_code=404, detail="Patient not found")

    medication = await medications_collection.find_one({"_id": ObjectId(medication_schedule.medication_id)})
    if not medication:
        raise HTTPException(status_code=404, detail="Medication not found")

    timezone_name = await get_patient_current_timezone_name(
        patient
    )

    interval_anchor_at = patient_local_anchor_to_utc(
        medication_schedule.interval_anchor_local,
        timezone_name
    )

    now = utc_now()

    doc = {
        "patient_id": ObjectId(medication_schedule.patient_id),
        "medication_id": ObjectId(medication_schedule.medication_id),
        "schedule_type": "interval_hours",
        "start_date": medication_schedule.start_date,
        "end_date": medication_schedule.end_date,
        "interval_hours": medication_schedule.interval_hours,
        "interval_anchor_at": interval_anchor_at,
        "createdAt": now,
        "updatedAt": now,
    }

    result = await medications_schedule_collection.insert_one(doc)
    doc["_id"] = result.inserted_id

    return {
        "schedule": {
            "id": str(doc["_id"]),
            "schedule_type": doc["schedule_type"],
            "interval_hours": doc["interval_hours"],
            "interval_anchor_at": doc["interval_anchor_at"],
        }
    }


# =====================================================================

# =================== GET PATIENT REMINDERS SECTION ===================
@router.get("/get_patient_reminders/{patient_id}")
async def get_patient_reminders(patient_id: str, hours: int = 24):
    try:
        if not ObjectId.is_valid(patient_id):
            raise HTTPException(status_code=400, detail="Invalid patient_id")

        if hours <= 0:
            raise HTTPException(status_code=400, detail="hours must be greater than 0")

        patient = await patients_collection.find_one({"_id": ObjectId(patient_id)})
        if not patient:
            raise HTTPException(status_code=404, detail="Patient not found")

        timezone_name = await get_patient_current_timezone_name(patient)

        window_start = utc_now()
        # window_start = datetime(2026, 3, 29, tzinfo=timezone.utc)
        window_end = window_start + timedelta(hours=hours)

        schedules = await medications_schedule_collection.find(
            {
                "patient_id": ObjectId(patient_id),
                "$and": [
                    {
                        "$or": [
                            {"start_date": None},
                            # {"start_date": {"$exists": False}},
                            {"start_date": {"$lt": window_end}},
                        ]
                    },
                    {
                        "$or": [
                            {"end_date": None},
                            # {"end_date": {"$exists": False}},
                            {"end_date": {"$gte": window_start}},
                        ]
                    },
                ],
            }
        ).to_list(length=None)

        medication_ids = list(
            {
                schedule["medication_id"]
                for schedule in schedules
                if schedule.get("medication_id") is not None
            }
        )

        medications = await medications_collection.find(
            {"_id": {"$in": medication_ids}}
        ).to_list(length=None)

        medication_map = {med["_id"]: med for med in medications}

        reminders: Any = []

        for schedule in schedules:
            medication = medication_map.get(schedule.get("medication_id"))
            if not medication:
                continue

            schedule_type = schedule.get("schedule_type")

            if schedule_type == "fixed_times":
                reminders.extend(
                    await  build_fixed_times_reminders(
                        schedule=schedule,
                        patient=patient,
                        medication=medication,
                        timezone_name=timezone_name,
                        window_start=window_start,
                        window_end=window_end,
                    )
                )

            elif schedule_type == "interval_hours":
                reminders.extend(
                    await  build_interval_hours_reminders(
                        schedule=schedule,
                        patient=patient,
                        medication=medication,
                        timezone_name=timezone_name,
                        window_start=window_start,
                        window_end=window_end,
                    )
                )

        reminders.sort(key=lambda item: item["reminder_at_utc"])
        grouped_reminders = group_reminders_by_time(reminders)

        return {
            "patient_id": str(patient["_id"]),
            "patient_name": patient.get("patient_name"),
            "current_timezone": timezone_name,
            "window_start_utc": window_start.isoformat(),
            "window_end_utc": window_end.isoformat(),
            "time_slot_count": len(grouped_reminders),
            "reminder_count": len(reminders),
            "grouped_reminders": grouped_reminders,
        }
    except HTTPException:
        raise
    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=str(e))


def group_reminders_by_time(reminders: list[dict]) -> list[dict]:
    grouped_map = {}

    for reminder in reminders:
        key = reminder["reminder_at_utc"]

        if key not in grouped_map:
            grouped_map[key] = {
                "reminder_at_utc": reminder["reminder_at_utc"],
                "reminder_at_local": reminder["reminder_at_local"],
                "current_timezone": reminder["current_timezone"],
                "dst_handling": reminder.get("dst_handling"),
                "medication_count": 0,
                "medications": [],
            }

        grouped_map[key]["medications"].append(
            {
                "schedule_id": reminder["schedule_id"],
                "schedule_type": reminder["schedule_type"],
                "medication_id": reminder["medication_id"],
                "medication_name": reminder["medication_name"],
                "scheduled_time": reminder.get("scheduled_time"),
                "interval_hours": reminder.get("interval_hours"),
            }
        )

        grouped_map[key]["medication_count"] += 1

    grouped = list(grouped_map.values())
    grouped.sort(key=lambda item: item["reminder_at_utc"])

    for group in grouped:
        group["medications"].sort(key=lambda item: item["medication_name"] or "")

    return grouped


def group_reminders_by_medication(reminders: list[dict]) -> list[dict]:
    grouped_map = {}

    for reminder in reminders:
        key = reminder["medication_name"]

        if key not in grouped_map:
            grouped_map[key] = {
                "reminder_at_utc": reminder["reminder_at_utc"],
                "reminder_at_local": reminder["reminder_at_local"],
                "current_timezone": reminder["current_timezone"],
                "dst_handling": reminder.get("dst_handling"),
                "medication_count": 0,
                "medications": [],
            }

        grouped_map[key]["medications"].append(
            {
                "schedule_id": reminder["schedule_id"],
                "schedule_type": reminder["schedule_type"],
                "medication_id": reminder["medication_id"],
                "medication_name": reminder["medication_name"],
                "scheduled_time": reminder.get("scheduled_time"),
                "interval_hours": reminder.get("interval_hours"),
            }
        )

        grouped_map[key]["medication_count"] += 1

    grouped = list(grouped_map.values())
    grouped.sort(key=lambda item: item["reminder_at_utc"])

    for group in grouped:
        group["medications"].sort(key=lambda item: item["medication_name"] or "")

    return grouped


def resolve_local_datetime(
        local_day: date,
        clock_time: str,
        timezone_name: str,
        gap_policy: str = "shift_forward",
        overlap_policy: str = "first_occurrence",
) -> tuple[Optional[datetime], Optional[str]]:
    zone = ZoneInfo(timezone_name)
    normalized_time = normalize_clock_time(clock_time)
    naive_local = datetime.combine(local_day, parse_clock_time(normalized_time))

    first = naive_local.replace(tzinfo=zone, fold=0)
    second = naive_local.replace(tzinfo=zone, fold=1)

    first_valid = roundtrip_matches(first, zone, naive_local)
    second_valid = roundtrip_matches(second, zone, naive_local)

    if first_valid and second_valid:
        if first.utcoffset() != second.utcoffset():
            if overlap_policy == "second_occurrence":
                return second, "ambiguous_local_time_used_second_occurrence"
            return first, "ambiguous_local_time_used_first_occurrence"
        return first, None

    if first_valid:
        return first, None

    if second_valid:
        return second, None

    if gap_policy == "skip":
        return None, "nonexistent_local_time_skipped"

    shifted_local = naive_local
    for _ in range(180):
        shifted_local += timedelta(minutes=1)

        first = shifted_local.replace(tzinfo=zone, fold=0)
        second = shifted_local.replace(tzinfo=zone, fold=1)

        first_valid = roundtrip_matches(first, zone, shifted_local)
        second_valid = roundtrip_matches(second, zone, shifted_local)

        if first_valid and second_valid and first.utcoffset() != second.utcoffset():
            return first, f"nonexistent_local_time_shifted_forward_from_{normalized_time}"
        if first_valid:
            return first, f"nonexistent_local_time_shifted_forward_from_{normalized_time}"
        if second_valid:
            return second, f"nonexistent_local_time_shifted_forward_from_{normalized_time}"

    return None, "nonexistent_local_time_unresolved"


async def get_patient_current_timezone_name(patient: dict[str, Any]) -> str:
    current_timezone = patient.get("current_timezone")

    if isinstance(current_timezone, str) and "/" in current_timezone:
        try:
            ZoneInfo(current_timezone)
            return current_timezone
        except ZoneInfoNotFoundError:
            raise HTTPException(status_code=400, detail="Invalid patient current_timezone")

    if isinstance(current_timezone, ObjectId):
        timezone_doc = await timezones_collection.find_one({"_id": current_timezone})
        if not timezone_doc or not timezone_doc.get("timezone"):
            raise HTTPException(status_code=400, detail="Patient timezone not found")
        try:
            ZoneInfo(timezone_doc["timezone"])
        except ZoneInfoNotFoundError:
            raise HTTPException(status_code=400, detail="Invalid timezone value in timezones collection")
        return timezone_doc["timezone"]

    raise HTTPException(status_code=400, detail="Patient current_timezone is missing or invalid")


async def build_fixed_times_reminders(
        schedule: dict[str, Any],
        patient: dict[str, Any],
        medication: dict[str, Any],
        timezone_name: str,
        window_start: datetime,
        window_end: datetime,
) -> list[dict[str, Any]]:
    reminders: list[dict[str, Any]] = []

    local_zone = ZoneInfo(timezone_name)
    local_start = window_start.astimezone(local_zone)
    local_end = window_end.astimezone(local_zone)

    start_date = ensure_utc_datetime(schedule.get("start_date"))
    end_date = ensure_utc_datetime(schedule.get("end_date"))

    gap_policy = schedule.get("dst_gap_policy", "shift_forward")
    overlap_policy = schedule.get("dst_overlap_policy", "first_occurrence")

    day_span = (local_end.date() - local_start.date()).days + 2
    # print(f"day_span: {day_span}")
    candidate_days = [local_start.date() + timedelta(days=i) for i in range(day_span)]
    # print(f"candidate_days: {candidate_days}")

    for local_day in candidate_days:
        for raw_time in schedule.get("times", []):
            try:
                normalized_time = normalize_clock_time(raw_time)
                # print(normalized_time)
            except ValueError:
                continue

            local_occurrence, dst_note = resolve_local_datetime(
                local_day=local_day,
                clock_time=normalized_time,
                timezone_name=timezone_name,
                gap_policy=gap_policy,
                overlap_policy=overlap_policy,
            )

            if local_occurrence is None:
                continue

            occurrence_utc = local_occurrence.astimezone(UTC)

            if occurrence_utc < window_start or occurrence_utc >= window_end:
                continue

            if start_date is not None and occurrence_utc < start_date:
                continue

            if end_date is not None and occurrence_utc >= end_date:
                continue

            reminders.append(
                {
                    "schedule_id": str(schedule["_id"]),
                    "schedule_type": "fixed_times",
                    "patient_id": str(patient["_id"]),
                    "patient_name": patient.get("patient_name"),
                    "medication_id": str(medication["_id"]),
                    "medication_name": medication.get("medication_name"),
                    "scheduled_time": normalized_time,
                    "reminder_at_utc": occurrence_utc.isoformat(),
                    "reminder_at_local": local_occurrence.isoformat(),
                    "current_timezone": timezone_name,
                    "dst_handling": dst_note,
                }
            )

    return reminders


async def build_interval_hours_reminders(
        schedule: dict[str, Any],
        patient: dict[str, Any],
        medication: dict[str, Any],
        timezone_name: str,
        window_start: datetime,
        window_end: datetime,
) -> list[dict[str, Any]]:
    reminders: list[dict[str, Any]] = []

    local_zone = ZoneInfo(timezone_name)

    start_date = ensure_utc_datetime(schedule.get("start_date"))
    end_date = ensure_utc_datetime(schedule.get("end_date"))
    anchor = ensure_utc_datetime(schedule.get("interval_anchor_at"))
    interval_hours = schedule.get("interval_hours")

    if anchor is None or interval_hours is None:
        return reminders

    if interval_hours <= 0:
        return reminders

    interval = timedelta(hours=interval_hours)
    interval_seconds = interval.total_seconds()

    effective_start = max(
        dt for dt in [window_start, start_date, anchor] if dt is not None
    )

    elapsed_seconds = (effective_start - anchor).total_seconds()

    if elapsed_seconds <= 0:
        next_occurrence = anchor
    else:
        step_count = math.ceil(elapsed_seconds / interval_seconds)
        next_occurrence = anchor + (interval * step_count)

    while next_occurrence < window_end:
        if start_date is not None and next_occurrence < start_date:
            next_occurrence += interval
            continue

        if end_date is not None and next_occurrence >= end_date:
            break

        local_occurrence = next_occurrence.astimezone(local_zone)

        reminders.append(
            {
                "schedule_id": str(schedule["_id"]),
                "schedule_type": "interval_hours",
                "patient_id": str(patient["_id"]),
                "patient_name": patient.get("patient_name"),
                "medication_id": str(medication["_id"]),
                "medication_name": medication.get("medication_name"),
                "interval_hours": interval_hours,
                "reminder_at_utc": next_occurrence.isoformat(),
                "reminder_at_local": local_occurrence.isoformat(),
                "current_timezone": timezone_name,
                "dst_handling": None,
            }
        )

        next_occurrence += interval

    return reminders

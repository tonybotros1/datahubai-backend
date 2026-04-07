from datetime import datetime
from typing import Optional, List
from pydantic import BaseModel


class PatientModel(BaseModel):
    patient_name: Optional[str] = None
    current_timezone: Optional[str] = None


class MedicationModel(BaseModel):
    medication_name: Optional[str] = None


class MedicationScheduleFixedTimeModel(BaseModel):
    patient_id: Optional[str] = None
    medication_id: Optional[str] = None
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    times: Optional[List[str]] = None
    active: Optional[bool] = None
    dst_gap_policy: str = "shift_forward"
    dst_overlap_policy: str = "first_occurrence"


class MedicationScheduleIntervalHoursModel(BaseModel):
    patient_id: str
    medication_id: str
    start_date: datetime
    end_date: Optional[datetime] = None
    active: Optional[bool] = None
    interval_hours: int
    # interval_anchor_at: datetime
    interval_anchor_local: str



class TimeZoneModel(BaseModel):
    timezone: Optional[str] = None

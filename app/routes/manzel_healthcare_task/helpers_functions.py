import re
from datetime import datetime, UTC, time, timezone, timedelta
from typing import Optional
from zoneinfo import ZoneInfo

TIME_RE = re.compile(r"^(?:[01]?\d|2[0-3]):[0-5]\d$")

def utc_now():
    return datetime.now(timezone.utc)


def ensure_utc_datetime(value: Optional[datetime]) -> Optional[datetime]:
    if value is None:
        return None

    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=UTC)

    return value.astimezone(UTC)


def parse_clock_time(value: str) -> time:
    value = value.strip()
    if not TIME_RE.fullmatch(value):
        raise ValueError(f"Invalid time format: {value}. Expected HH:MM")
    hour, minute = value.split(":")
    return time(hour=int(hour), minute=int(minute))


def normalize_clock_time(value: str) -> str:
    parsed = parse_clock_time(value)
    return f"{parsed.hour:02d}:{parsed.minute:02d}"


def roundtrip_matches(candidate: datetime, zone: ZoneInfo, naive_local: datetime) -> bool:
    roundtrip = candidate.astimezone(UTC).astimezone(zone).replace(tzinfo=None)
    return roundtrip == naive_local



def patient_local_anchor_to_utc(
    interval_anchor_local: str,
    timezone_name: str,
    gap_policy: str = "shift_forward",
    overlap_policy: str = "first_occurrence",
) -> datetime:
    naive_local = datetime.fromisoformat(interval_anchor_local)

    if naive_local.tzinfo is not None:
        raise ValueError("interval_anchor_local must not include timezone")

    zone = ZoneInfo(timezone_name)

    first = naive_local.replace(tzinfo=zone, fold=0)
    second = naive_local.replace(tzinfo=zone, fold=1)

    first_valid = roundtrip_matches(first, zone, naive_local)
    second_valid = roundtrip_matches(second, zone, naive_local)

    if first_valid and second_valid:
        if first.utcoffset() != second.utcoffset():
            if overlap_policy == "second_occurrence":
                return second.astimezone(UTC)
            return first.astimezone(UTC)
        return first.astimezone(UTC)

    if first_valid:
        return first.astimezone(UTC)

    if second_valid:
        return second.astimezone(UTC)

    if gap_policy == "skip":
        raise ValueError("interval_anchor_local falls in DST gap")

    shifted_local = naive_local
    for _ in range(180):
        shifted_local += timedelta(minutes=1)

        first = shifted_local.replace(tzinfo=zone, fold=0)
        second = shifted_local.replace(tzinfo=zone, fold=1)

        first_valid = roundtrip_matches(first, zone, shifted_local)
        second_valid = roundtrip_matches(second, zone, shifted_local)

        if first_valid and second_valid and first.utcoffset() != second.utcoffset():
            return first.astimezone(UTC)
        if first_valid:
            return first.astimezone(UTC)
        if second_valid:
            return second.astimezone(UTC)

    raise ValueError("Could not resolve interval anchor local time")

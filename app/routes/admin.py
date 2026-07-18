import asyncio
import secrets
from datetime import datetime, timezone
from typing import Any

from bson import ObjectId
from fastapi import APIRouter, Body, Depends, Header, HTTPException
from pydantic import BaseModel

from app.core import security
from app.database import get_collection
from app.websocket_config import manager


router = APIRouter()

ADMIN_SCREEN_PASSWORD = "d@t@hub@i"

users_collection = get_collection("sys-users")
refresh_tokens_collection = get_collection("refresh_tokens")
roles_collection = get_collection("sys-roles")
branches_collection = get_collection("branches")
audit_collection = get_collection("admin_session_audit")


class UserStatusChange(BaseModel):
    status: bool


def _as_utc(value: Any) -> datetime | None:
    if not isinstance(value, datetime):
        return None
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def _iso(value: Any) -> str | None:
    parsed = _as_utc(value)
    return parsed.isoformat() if parsed else None


def _object_id(value: Any, label: str) -> ObjectId:
    if not ObjectId.is_valid(str(value or "")):
        raise HTTPException(status_code=400, detail=f"Invalid {label}")
    return ObjectId(str(value))


async def _current_session_id(data: dict, user_id: ObjectId) -> str:
    claimed_session_id = str(data.get("session_id", "") or "")
    if claimed_session_id:
        return claimed_session_id
    access_jti = str(data.get("jti", "") or "")
    if not access_jti:
        return ""
    session = await refresh_tokens_collection.find_one(
        {"user_id": user_id, "access_jti": access_jti},
        {"jti": 1},
    )
    return str(session.get("jti", "") or "") if session else ""


async def _admin_access(
        x_admin_password: str = Header(default="", alias="X-Admin-Password"),
        data: dict = Depends(security.get_current_user),
) -> dict:
    if not secrets.compare_digest(x_admin_password, ADMIN_SCREEN_PASSWORD):
        raise HTTPException(status_code=403, detail="Invalid admin screen password")

    user_id = _object_id(data.get("sub"), "current user")
    company_id = _object_id(data.get("company_id"), "company")
    current_user = await users_collection.find_one(
        {
            "_id": user_id,
            "company_id": company_id,
            "status": True,
            "is_admin": True,
        },
        {"_id": 1},
    )
    if not current_user:
        raise HTTPException(
            status_code=403,
            detail="An active administrator account is required",
        )
    return data


async def _audit(
        company_id: ObjectId,
        actor_id: ObjectId,
        action: str,
        target_user_id: ObjectId | None = None,
        details: dict | None = None,
) -> None:
    await audit_collection.insert_one({
        "company_id": company_id,
        "actor_id": actor_id,
        "target_user_id": target_user_id,
        "action": action,
        "details": details or {},
        "created_at": datetime.now(timezone.utc),
    })


async def _company_target(company_id: ObjectId, user_id: str) -> dict:
    target_id = _object_id(user_id, "user")
    target = await users_collection.find_one(
        {"_id": target_id, "company_id": company_id},
        {"_id": 1, "user_name": 1, "email": 1, "status": 1},
    )
    if not target:
        raise HTTPException(status_code=404, detail="User not found in your company")
    return target


async def _revoke_user_sessions(
        company_id: ObjectId,
        actor_id: ObjectId,
        target: dict,
        reason: str,
) -> tuple[int, int]:
    now = datetime.now(timezone.utc)
    target_id = target["_id"]
    await users_collection.update_one(
        {"_id": target_id, "company_id": company_id},
        {
            "$inc": {"session_version": 1},
            "$set": {
                "forced_logout_at": now,
                "last_logout_at": now,
                "last_seen_at": now,
                "updatedAt": now,
            },
        },
    )
    deleted = await refresh_tokens_collection.delete_many({"user_id": target_id})
    disconnected = await manager.disconnect_user(str(target_id), reason=reason)
    await _audit(
        company_id,
        actor_id,
        "force_logout",
        target_id,
        {
            "revoked_refresh_tokens": deleted.deleted_count,
            "disconnected_connections": disconnected,
        },
    )
    return deleted.deleted_count, disconnected


@router.get("/users_overview")
async def users_overview(data: dict = Depends(_admin_access)):
    company_id = _object_id(data.get("company_id"), "company")
    current_user_id = _object_id(data.get("sub"), "current user")
    current_session_id = await _current_session_id(data, current_user_id)
    now = datetime.now(timezone.utc)

    users = await users_collection.find(
        {"company_id": company_id},
        {
            "password_hash": 0,
            "company_id": 0,
        },
    ).sort("user_name", 1).to_list(None)
    user_ids = [user["_id"] for user in users]

    role_ids = {role for user in users for role in user.get("roles", []) if isinstance(role, ObjectId)}
    branch_ids = {
        branch
        for user in users
        for branch in user.get("branches", [])
        if isinstance(branch, ObjectId)
    }
    role_rows = await roles_collection.find(
        {"_id": {"$in": list(role_ids)}},
        {"role_name": 1},
    ).to_list(None) if role_ids else []
    branch_rows = await branches_collection.find(
        {"_id": {"$in": list(branch_ids)}},
        {"name": 1},
    ).to_list(None) if branch_ids else []
    role_names = {row["_id"]: row.get("role_name", "") for row in role_rows}
    branch_names = {row["_id"]: row.get("name", "") for row in branch_rows}

    active_session_rows = await refresh_tokens_collection.find(
        {"user_id": {"$in": user_ids}, "expires_at": {"$gt": now}},
        {
            "user_id": 1,
            "jti": 1,
            "access_jti": 1,
            "created_at": 1,
            "last_seen_at": 1,
            "expires_at": 1,
            "ip_address": 1,
            "user_agent": 1,
        },
    ).to_list(None) if user_ids else []
    sessions_by_user: dict[ObjectId, list[dict]] = {}
    for session in active_session_rows:
        sessions_by_user.setdefault(session["user_id"], []).append(session)

    presence = manager.get_company_presence(str(company_id))
    result: list[dict] = []
    online_users = 0
    enabled_users = 0
    disabled_users = 0
    expired_users = 0
    active_sessions = 0

    for user in users:
        user_id = user["_id"]
        user_presence = presence.get(str(user_id), {})
        online = bool(user_presence)
        sessions = sessions_by_user.get(user_id, [])
        active_sessions += len(sessions)
        if online:
            online_users += 1

        expiry = _as_utc(user.get("expiry_date"))
        expired = bool(expiry and expiry <= now)
        enabled = bool(user.get("status", False))
        if enabled and not expired:
            enabled_users += 1
        if not enabled:
            disabled_users += 1
        if expired:
            expired_users += 1

        latest_session = max(
            sessions,
            key=lambda row: _as_utc(row.get("created_at")) or datetime.min.replace(tzinfo=timezone.utc),
            default={},
        )
        connected_at = user_presence.get("connected_at")
        last_seen = (
            user_presence.get("last_seen_at")
            or user.get("last_seen_at")
            or user.get("last_logout_at")
            or user.get("last_login_at")
        )
        connection_count = int(user_presence.get("connection_count", 0) or 0)
        ip_addresses = sorted(user_presence.get("ip_addresses", set()))
        user_agents = sorted(user_presence.get("user_agents", set()))
        ip_address = (
            (ip_addresses[0] if ip_addresses else "")
            or latest_session.get("ip_address", "")
            or user.get("last_login_ip", "")
        )
        user_agent = (
            (user_agents[0] if user_agents else "")
            or latest_session.get("user_agent", "")
            or user.get("last_user_agent", "")
        )
        live_session_ids = user_presence.get("session_ids", set())
        session_details = [
            {
                "id": str(session.get("jti", "") or ""),
                "is_current_session": (
                    user_id == current_user_id
                    and bool(current_session_id)
                    and session.get("jti") == current_session_id
                ),
                "is_online": bool(
                    session.get("jti")
                    and session.get("jti") in live_session_ids
                ),
                "login_at": _iso(session.get("created_at")),
                "last_seen_at": _iso(session.get("last_seen_at")),
                "expires_at": _iso(session.get("expires_at")),
                "ip_address": session.get("ip_address", ""),
                "user_agent": session.get("user_agent", ""),
            }
            for session in sorted(
                sessions,
                key=lambda row: _as_utc(row.get("created_at"))
                or datetime.min.replace(tzinfo=timezone.utc),
                reverse=True,
            )
            if session.get("jti")
        ]

        result.append({
            "id": str(user_id),
            "user_name": user.get("user_name", ""),
            "email": user.get("email", ""),
            "is_current_user": user_id == current_user_id,
            "is_admin": bool(user.get("is_admin", False)),
            "enabled": enabled,
            "expired": expired,
            "is_online": online,
            "connection_count": connection_count,
            "active_sessions": len(sessions),
            "sessions": session_details,
            "login_at": _iso(user.get("last_login_at") or latest_session.get("created_at")),
            "online_since": _iso(connected_at),
            "last_seen_at": _iso(last_seen),
            "last_logout_at": _iso(user.get("last_logout_at")),
            "forced_logout_at": _iso(user.get("forced_logout_at")),
            "expiry_date": _iso(user.get("expiry_date")),
            "created_at": _iso(user.get("createdAt")),
            "updated_at": _iso(user.get("updatedAt")),
            "ip_address": ip_address,
            "user_agent": user_agent,
            "roles": [role_names.get(role, str(role)) for role in user.get("roles", [])],
            "branches": [branch_names.get(branch, str(branch)) for branch in user.get("branches", [])],
            "primary_branch": branch_names.get(user.get("primary_branch"), ""),
        })

    result.sort(key=lambda row: (not row["is_online"], row["user_name"].lower()))
    return {
        "generated_at": now.isoformat(),
        "current_user_id": str(current_user_id),
        "summary": {
            "total_users": len(result),
            "online_users": online_users,
            "enabled_users": enabled_users,
            "disabled_users": disabled_users,
            "expired_users": expired_users,
            "active_sessions": active_sessions,
            "live_connections": sum(row["connection_count"] for row in result),
        },
        "users": result,
    }


@router.delete("/users/{user_id}/sessions/{session_id}")
async def force_logout_session(
        user_id: str,
        session_id: str,
        data: dict = Depends(_admin_access),
):
    company_id = _object_id(data.get("company_id"), "company")
    actor_id = _object_id(data.get("sub"), "current user")
    current_session_id = await _current_session_id(data, actor_id)
    target = await _company_target(company_id, user_id)

    if target["_id"] == actor_id and session_id == current_session_id:
        raise HTTPException(
            status_code=400,
            detail="Use the normal logout button for this device",
        )

    session = await refresh_tokens_collection.find_one({
        "user_id": target["_id"],
        "jti": session_id,
    })
    if not session:
        raise HTTPException(status_code=404, detail="This session is no longer active")

    now = datetime.now(timezone.utc)
    deleted = await refresh_tokens_collection.delete_one({"_id": session["_id"]})
    revoked_push = {
        "revoked_session_ids": {
            "$each": [session_id],
            "$slice": -100,
        }
    }
    if session.get("access_jti"):
        revoked_push["revoked_access_jtis"] = {
            "$each": [session["access_jti"]],
            "$slice": -100,
        }
    await users_collection.update_one(
        {"_id": target["_id"], "company_id": company_id},
        {
            "$set": {
                "last_forced_session_logout_at": now,
                "updatedAt": now,
            },
            "$push": revoked_push,
        },
    )
    disconnected = await manager.disconnect_session(
        str(target["_id"]),
        session_id,
        reason="This device was signed out by your company administrator",
    )
    await _audit(
        company_id,
        actor_id,
        "force_logout_session",
        target["_id"],
        {
            "session_id": session_id,
            "ip_address": session.get("ip_address", ""),
            "user_agent": session.get("user_agent", ""),
            "revoked_refresh_tokens": deleted.deleted_count,
            "disconnected_connections": disconnected,
        },
    )
    await manager.send_to_company(str(company_id), {
        "type": "admin_users_changed",
        "data": {
            "user_id": user_id,
            "action": "force_logout_session",
            "session_id": session_id,
        },
    })
    return {
        "message": "Device session logged out",
        "session_id": session_id,
        "disconnected_connections": disconnected,
    }


@router.post("/users/{user_id}/force_logout")
async def force_logout_user(user_id: str, data: dict = Depends(_admin_access)):
    company_id = _object_id(data.get("company_id"), "company")
    actor_id = _object_id(data.get("sub"), "current user")
    target = await _company_target(company_id, user_id)
    if target["_id"] == actor_id:
        raise HTTPException(status_code=400, detail="Use the normal logout button for your own account")

    revoked, disconnected = await _revoke_user_sessions(
        company_id,
        actor_id,
        target,
        reason="Your session was ended by your company administrator",
    )
    await manager.send_to_company(str(company_id), {
        "type": "admin_users_changed",
        "data": {"user_id": user_id, "action": "force_logout"},
    })
    return {
        "message": f"{target.get('user_name') or target.get('email') or 'User'} was logged out",
        "revoked_refresh_tokens": revoked,
        "disconnected_connections": disconnected,
    }


@router.post("/force_logout_all")
async def force_logout_all(data: dict = Depends(_admin_access)):
    company_id = _object_id(data.get("company_id"), "company")
    actor_id = _object_id(data.get("sub"), "current user")
    targets = await users_collection.find(
        {"company_id": company_id, "_id": {"$ne": actor_id}},
        {"_id": 1},
    ).to_list(None)
    target_ids = [row["_id"] for row in targets]
    if not target_ids:
        return {"message": "No other users to log out", "users": 0, "connections": 0}

    now = datetime.now(timezone.utc)
    await users_collection.update_many(
        {"_id": {"$in": target_ids}, "company_id": company_id},
        {
            "$inc": {"session_version": 1},
            "$set": {
                "forced_logout_at": now,
                "last_logout_at": now,
                "last_seen_at": now,
                "updatedAt": now,
            },
        },
    )
    deleted = await refresh_tokens_collection.delete_many({"user_id": {"$in": target_ids}})
    disconnected_counts = await asyncio.gather(*[
        manager.disconnect_user(
            str(target_id),
            reason="All company sessions were ended by the administrator",
        )
        for target_id in target_ids
    ])
    await _audit(
        company_id,
        actor_id,
        "force_logout_all",
        details={
            "users": len(target_ids),
            "revoked_refresh_tokens": deleted.deleted_count,
            "disconnected_connections": sum(disconnected_counts),
        },
    )
    await manager.send_to_company(str(company_id), {
        "type": "admin_users_changed",
        "data": {"action": "force_logout_all"},
    })
    return {
        "message": "All other company users were logged out",
        "users": len(target_ids),
        "revoked_refresh_tokens": deleted.deleted_count,
        "disconnected_connections": sum(disconnected_counts),
    }


@router.patch("/users/{user_id}/status")
async def change_user_status(
        user_id: str,
        change: UserStatusChange = Body(...),
        data: dict = Depends(_admin_access),
):
    company_id = _object_id(data.get("company_id"), "company")
    actor_id = _object_id(data.get("sub"), "current user")
    target = await _company_target(company_id, user_id)
    if target["_id"] == actor_id and not change.status:
        raise HTTPException(status_code=400, detail="You cannot disable your own account here")

    now = datetime.now(timezone.utc)
    await users_collection.update_one(
        {"_id": target["_id"], "company_id": company_id},
        {"$set": {"status": change.status, "updatedAt": now}},
    )
    if not change.status:
        await _revoke_user_sessions(
            company_id,
            actor_id,
            target,
            reason="Your account was disabled by your company administrator",
        )
    else:
        await _audit(company_id, actor_id, "enable_user", target["_id"])

    await manager.send_to_company(str(company_id), {
        "type": "admin_users_changed",
        "data": {"user_id": user_id, "action": "status", "status": change.status},
    })
    return {
        "message": f"User account {'enabled' if change.status else 'disabled'}",
        "user_id": user_id,
        "status": change.status,
    }

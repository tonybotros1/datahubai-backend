# app/websocket_manager.py
from fastapi import WebSocket
from typing import Any, Dict, List, Set
from datetime import datetime, timezone
import json


async def send_personal_message(message: str, websocket: WebSocket):
    await websocket.send_text(message)


class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []
        self.user_connections: Dict[str, Set[WebSocket]] = {}
        self.company_connections: Dict[str, Set[WebSocket]] = {}
        self.connection_metadata: Dict[WebSocket, Dict[str, Any]] = {}

    async def connect(
            self,
            websocket: WebSocket,
            user_id: str | None = None,
            company_id: str | None = None,
            session_id: str = "",
            client_ip: str = "",
            user_agent: str = "",
    ):
        await websocket.accept()
        connected_at = datetime.now(timezone.utc)
        self.active_connections.append(websocket)
        self.connection_metadata[websocket] = {
            "user_id": user_id or "",
            "company_id": company_id or "",
            "session_id": session_id,
            "connected_at": connected_at,
            "last_seen_at": connected_at,
            "ip_address": client_ip,
            "user_agent": user_agent,
        }
        if user_id is not None:
            self.user_connections.setdefault(user_id, set()).add(websocket)
        if company_id is not None:
            self.company_connections.setdefault(company_id, set()).add(websocket)

    def disconnect(self, websocket: WebSocket, user_id: str | None = None, company_id: str | None = None):
        metadata = self.connection_metadata.pop(websocket, {})
        user_id = user_id or metadata.get("user_id")
        company_id = company_id or metadata.get("company_id")
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
        if user_id is not None and user_id in self.user_connections:
            self.user_connections[user_id].discard(websocket)
            if not self.user_connections[user_id]:
                del self.user_connections[user_id]
        if company_id and company_id in self.company_connections:
            self.company_connections[company_id].discard(websocket)
            if not self.company_connections[company_id]:
                del self.company_connections[company_id]

    def touch(self, websocket: WebSocket):
        metadata = self.connection_metadata.get(websocket)
        if metadata is not None:
            metadata["last_seen_at"] = datetime.now(timezone.utc)

    def get_company_presence(self, company_id: str) -> Dict[str, Dict[str, Any]]:
        presence: Dict[str, Dict[str, Any]] = {}
        for websocket in list(self.company_connections.get(company_id, set())):
            metadata = self.connection_metadata.get(websocket)
            if not metadata:
                continue
            user_id = str(metadata.get("user_id") or "")
            if not user_id:
                continue
            current = presence.setdefault(user_id, {
                "connection_count": 0,
                "connected_at": metadata["connected_at"],
                "last_seen_at": metadata["last_seen_at"],
                "ip_addresses": set(),
                "user_agents": set(),
                "session_ids": set(),
            })
            current["connection_count"] += 1
            current["connected_at"] = min(current["connected_at"], metadata["connected_at"])
            current["last_seen_at"] = max(current["last_seen_at"], metadata["last_seen_at"])
            if metadata.get("ip_address"):
                current["ip_addresses"].add(metadata["ip_address"])
            if metadata.get("user_agent"):
                current["user_agents"].add(metadata["user_agent"])
            if metadata.get("session_id"):
                current["session_ids"].add(metadata["session_id"])
        return presence

    async def disconnect_session(
            self,
            user_id: str,
            session_id: str,
            reason: str = "This device was signed out by the administrator",
    ) -> int:
        connections = [
            websocket
            for websocket in self.user_connections.get(user_id, set())
            if self.connection_metadata.get(websocket, {}).get("session_id") == session_id
        ]
        message = json.dumps({
            "type": "force_logout",
            "data": {"reason": reason, "session_id": session_id},
        })
        for websocket in connections:
            try:
                await websocket.send_text(message)
            except Exception:
                pass
            try:
                await websocket.close(code=4001, reason=reason)
            except Exception:
                pass
            self.disconnect(websocket, user_id=user_id)
        return len(connections)

    async def disconnect_user(self, user_id: str, reason: str = "Signed out by administrator") -> int:
        connections = list(self.user_connections.get(user_id, set()))
        message = json.dumps({
            "type": "force_logout",
            "data": {"reason": reason},
        })
        for websocket in connections:
            try:
                await websocket.send_text(message)
            except Exception:
                pass
            try:
                await websocket.close(code=4001, reason=reason)
            except Exception:
                pass
            self.disconnect(websocket, user_id=user_id)
        return len(connections)

    async def broadcast(self, message: dict):
        # تحويل الرسالة إلى JSON
        message_json = json.dumps(message)
        # إرسال الرسالة لجميع المتصلين
        for connection in list(self.active_connections):
            try:
                await connection.send_text(message_json)
            except Exception:
                self.disconnect(connection)

    async def send_to_user(self, user_id: str, message: dict):
        data = json.dumps(message)
        for ws in list(self.user_connections.get(user_id, [])):
            try:
                await ws.send_text(data)
            except Exception:
                self.disconnect(ws, user_id=user_id)

    async def send_to_company(self, company_id: str, message: dict):
        data = json.dumps(message)

        for ws in list(self.company_connections.get(company_id, [])):
            try:
                await ws.send_text(data)
            except Exception:
                self.disconnect(ws, company_id=company_id)

    async def send_progress(self, percent: int):
        await self.broadcast({
            "type": "progress",
            "progress": percent
        })


# إنشاء نسخة عامة من المدير
manager = ConnectionManager()

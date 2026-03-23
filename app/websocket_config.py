# app/websocket_manager.py
from fastapi import WebSocket
from typing import List, Dict, Set
import json


async def send_personal_message(message: str, websocket: WebSocket):
    await websocket.send_text(message)


class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []
        self.user_connections: Dict[str, Set[WebSocket]] = {}
        self.company_connections: Dict[str, Set[WebSocket]] = {}

    async def connect(self, websocket: WebSocket, user_id: str | None = None, company_id: str | None = None):
        await websocket.accept()
        self.active_connections.append(websocket)
        if user_id is not None:
            self.user_connections.setdefault(user_id, set()).add(websocket)
        if company_id is not None:
            self.company_connections.setdefault(company_id, set()).add(websocket)

    def disconnect(self, websocket: WebSocket, user_id: str | None = None, company_id: str | None = None):
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

    async def broadcast(self, message: dict):
        # تحويل الرسالة إلى JSON
        message_json = json.dumps(message)
        # إرسال الرسالة لجميع المتصلين
        for connection in self.active_connections:
            await connection.send_text(message_json)

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

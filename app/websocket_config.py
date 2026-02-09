# app/websocket_manager.py
from fastapi import WebSocket
from typing import List
import json


async def send_personal_message(message: str, websocket: WebSocket):
    await websocket.send_text(message)


class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    async def broadcast(self, message: dict):
        # تحويل الرسالة إلى JSON
        message_json = json.dumps(message)
        # إرسال الرسالة لجميع المتصلين
        for connection in self.active_connections:
            await connection.send_text(message_json)

    async def send_progress(self, percent: int):
        await self.broadcast({
            "type": "progress",
            "progress": percent
        })

# إنشاء نسخة عامة من المدير
manager = ConnectionManager()

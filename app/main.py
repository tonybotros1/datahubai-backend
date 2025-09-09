from contextlib import asynccontextmanager

from app.database import get_collection
from app.widgets import upload_images
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from app.routes import brands_and_models
from app.routes import countries_and_cities
from app.routes import functions
from app.routes import menus
from app.routes import responsibilities
from app.routes import test
from app.routes import auth
from app.websocket_config import manager

users = get_collection("sys-users")
companies = get_collection("companies")


@asynccontextmanager
async def lifespan(app_: FastAPI):
    await companies.create_index("company_name", unique=True)
    await users.create_index("email", unique=True)
    print("✅ Unique indexes ensured at startup")
    yield
    print("👋 App is shutting down")


app = FastAPI(title="DataHub AI", lifespan=lifespan)

origins = [

    "http://localhost",  # وقت التجريب من المتصفح مباشرة
    "http://localhost:3000",  # لو بتجرب React/Vue/Next أو Flutter web local
    "http://127.0.0.1:8000",  # وقت تشغل باكند لوكال
    "https://compass-automatic-gear.web.app",  # موقعك المبني على Firebase

]

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Routers
app.include_router(auth.router, prefix="/auth", tags=["Authentication"])
app.include_router(upload_images.images, prefix="/upload-image", tags=["Images"])
app.include_router(brands_and_models.router, prefix="/brands", tags=["Brands"])
app.include_router(countries_and_cities.router, prefix="/countries", tags=["Countries"])

app.include_router(responsibilities.router, prefix="/responsibilities", tags=["Responsibilities"])
app.include_router(menus.router, prefix="/menus", tags=["Menus"])
app.include_router(functions.router, prefix="/functions", tags=["Functions"])
app.include_router(test.router, prefix="/test", tags=["Test"])


# نقطة نهاية WebSocket العامة
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            # يمكنك معالجة الرسائل الواردة من العميل إذا لزم الأمر
            data = await websocket.receive_text()
            await manager.broadcast({"echo": data})
    except WebSocketDisconnect:
        manager.disconnect(websocket)


@app.get("/")
def home():
    return {"message": "DataHubAI"}

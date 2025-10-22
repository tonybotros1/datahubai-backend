from contextlib import asynccontextmanager

from app.database import get_collection
from app.widgets import upload_images
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from app.routes import brands_and_models, users, countries_and_cities, functions, menus, responsibilities, auth, \
    companies, favourite_screens, list_of_values, counters, branches, car_trading, salesman, system_variables, \
    currencies, entity_information, ap_payment_types, banks_and_others, technician, invoice_items, job_cards, \
    quotation_cards, job_tasks, time_sheets, employees_performance
from app.routes import test
from app.websocket_config import manager

users_collection = get_collection("sys-users")
companies_collection = get_collection("companies")


@asynccontextmanager
async def lifespan(_: FastAPI):
    await companies_collection.create_index("company_name", unique=True)
    await users_collection.create_index("email", unique=True)
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
app.include_router(favourite_screens.router, prefix="/favourite_screens", tags=["Favourite screens"])
app.include_router(upload_images.images, prefix="/upload-image", tags=["Images"])
app.include_router(brands_and_models.router, prefix="/brands", tags=["Brands"])
app.include_router(countries_and_cities.router, prefix="/countries", tags=["Countries"])
app.include_router(companies.router, prefix="/companies", tags=["Companies"])
app.include_router(list_of_values.router, prefix="/list_of_values", tags=["List Of Values"])
app.include_router(counters.router, prefix="/counters", tags=["Counters"])
app.include_router(responsibilities.router, prefix="/responsibilities", tags=["Responsibilities"])
app.include_router(menus.router, prefix="/menus", tags=["Menus"])
app.include_router(functions.router, prefix="/functions", tags=["Functions"])
app.include_router(branches.router, prefix="/branches", tags=["Branches"])
app.include_router(users.router, prefix="/users", tags=["Users"])
app.include_router(car_trading.router, prefix="/car_trading", tags=["Car Trading"])
app.include_router(salesman.router, prefix="/salesman", tags=["Salesman"])
app.include_router(system_variables.router, prefix="/system_variables", tags=["System Variables"])
app.include_router(currencies.router, prefix="/currencies", tags=["Currencies"])
app.include_router(entity_information.router, prefix="/entity_information", tags=["Entity Information"])
app.include_router(ap_payment_types.router, prefix="/ap_payment_types", tags=["AP Payment Types"])
app.include_router(banks_and_others.router, prefix="/banks_and_others", tags=["Banks and Others"])
app.include_router(technician.router, prefix="/technicians", tags=["Technicians"])
app.include_router(invoice_items.router, prefix="/invoice_items", tags=["Invoice Items"])
app.include_router(job_cards.router, prefix="/job_cards", tags=["Job Cards"])
app.include_router(quotation_cards.router, prefix="/quotation_cards", tags=["Quotation Cards"])
app.include_router(job_tasks.router, prefix="/job_tasks", tags=["Job Tasks"])
app.include_router(time_sheets.router, prefix="/time_sheets", tags=["Time sheets"])
app.include_router(employees_performance.router, prefix="/employees_performance", tags=["Employee Performance"])
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

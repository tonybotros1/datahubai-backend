from fastapi import FastAPI
from app.routes import brands
from app.widgets import upload_images
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.routes import brands

app = FastAPI(title="DataHub AI")

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
# app.include_router(cars.router, prefix="/cars", tags=["Cars"])
# app.include_router(jobs.router, prefix="/jobs", tags=["Job Cards"])
app.include_router(brands.router, prefix="/brands", tags=["Brands"])
app.include_router(upload_images.images, prefix="/upload-image", tags=["Images"])


@app.get("/")
def home():
    return {"message": "DataHubAI"}

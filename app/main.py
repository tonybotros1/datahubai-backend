from fastapi import FastAPI
from app.routes import  brands  # ← هيك لازم تشتغل

app = FastAPI(title="DataHub AI")

# Routers
# app.include_router(cars.router, prefix="/cars", tags=["Cars"])
# app.include_router(jobs.router, prefix="/jobs", tags=["Job Cards"])
app.include_router(brands.router, prefix="/brands", tags=["Brands"])


@app.get("/")
def home():
    return {"message": "FastAPI on Render with MongoDB Atlas!"}

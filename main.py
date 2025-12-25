from dotenv import load_dotenv

# Load environment variables from .env file BEFORE any other imports
load_dotenv()

from fastapi import FastAPI
from src.api import router
from src.routers import data_quality

app = FastAPI(title="TestGen API", version="1.0.0")

app.include_router(router)
app.include_router(data_quality.router, prefix="/api")

@app.get("/health")
async def health_check():
    return {"status": "ok"}

from typing import Optional, List
from bson import ObjectId
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from app import database
from app.core import security
from app.database import get_collection
from datetime import datetime, timezone, timedelta
from app.routes.car_trading import PyObjectId
from app.routes.counters import create_custom_counter

router = APIRouter()



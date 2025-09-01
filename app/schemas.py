from pydantic import BaseModel
from datetime import datetime, timezone

# --- Brand Schema ---
class BrandSchema(BaseModel):
    id: str
    name: str
    logo: str
    status: bool = True
    createdAt: datetime = datetime.now(timezone.utc)
    updatedAt: datetime = datetime.now(timezone.utc)

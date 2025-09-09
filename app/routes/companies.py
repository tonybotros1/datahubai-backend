from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException, status
from app.core import security
from app.database import get_collection

router = APIRouter()
users = get_collection("sys-users")


def serialize_doc(doc):
    if isinstance(doc, dict):
        # Convert _id to string for serialization
        if "_id" in doc and isinstance(doc["_id"], ObjectId):
            doc["_id"] = str(doc["_id"])
        # Recursively serialize children
        for key, value in doc.items():
            doc[key] = serialize_doc(value)
    elif isinstance(doc, list):
        return [serialize_doc(item) for item in doc]
    return doc


@router.get("/get_user_and_company_details")
async def get_user_and_company_details(user_data: dict = Depends(security.get_current_user)):
    try:
        user_id = user_data.get("sub")
        if not user_id:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User ID not found in token")
        pipeline = [
            {
                "$match": {"_id": ObjectId(user_id)}
            },
            {
                "$lookup": {
                    "from": "companies",
                    "localField": "company_id",
                    "foreignField": "_id",
                    "as": "company",
                }
            },
            {
                "$unwind": "$company"
            },
            {
                "$project": {
                    "_id": 1,
                    "user_name": 1,
                    "company_name": "$company.company_name",
                    "company_logo": "$company.company_logo_url",
                    "createdAt": 1,
                    "email": 1,
                    "expiry_date":1
                }
            }

        ]
        cursor = await users.aggregate(pipeline)
        result = await cursor.to_list()

        data = result[0]
        return {"data": serialize_doc(data)}


    except Exception as e:
        return {"message": str(e)}

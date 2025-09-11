from datetime import datetime

from bson import ObjectId


def serialize_doc(doc):
    # Convert _id to string for serialization
    # if "_id" in doc and isinstance(doc["_id"], ObjectId):
    #     doc["_id"] = str(doc["_id"])
    for key, value in doc.items():
        if isinstance(value, ObjectId):
            doc[key] = str(value)
    for key, value in doc.items():
        if isinstance(value, datetime):
            doc[key] = value.isoformat()

    return doc

from pymongo import MongoClient
from .config import MONGO_URI, DATABASE_NAME

client = MongoClient(MONGO_URI)
db = client[DATABASE_NAME]

def get_collection(name):
    return db[name]

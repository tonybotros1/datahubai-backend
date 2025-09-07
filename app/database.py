from pymongo import AsyncMongoClient
from .config import MONGO_URI, DATABASE_NAME

# أنشئ العميل async
client = AsyncMongoClient(MONGO_URI, maxPoolSize=100, minPoolSize=5)

# اختار قاعدة البيانات
db = client[DATABASE_NAME]


# دالة ترجع أي Collection بدك ياه
def get_collection(name: str):
    return db[name]

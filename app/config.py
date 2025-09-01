import os
from dotenv import load_dotenv

load_dotenv()  # إذا استخدمت ملف .env للتجريب

MONGO_URI = os.getenv("MONGO_URI")
DATABASE_NAME = "datahub"

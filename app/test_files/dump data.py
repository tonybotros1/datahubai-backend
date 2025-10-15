import json
import random
from datetime import datetime, timedelta
from bson import ObjectId  # pip install bson

# Number of records
num_records = 20000

# Example list of first and last names to randomize
first_names = ["Mohammed", "Ali", "Omar", "Sara", "Lina", "Khalid", "Yousef", "Maya", "Tony", "Rana"]
last_names = ["Al Sabsabi", "Haddad", "Khalil", "Nasser", "Barakat", "Salim", "Abbas", "Hassan"]


# Function to generate random date
def random_date(start, end):
    return start + timedelta(seconds=random.randint(0, int((end - start).total_seconds())))


records = []
for _ in range(num_records):
    first_name = random.choice(first_names)
    last_name = random.choice(last_names)
    name = f"{first_name} {last_name}"

    record = {
        "_id": {"$oid": str(ObjectId())},
        "name": name,
        "target": random.randint(10000, 500000),
        "company_id": {"$oid": "68bbfc4b56c35562f967422d"},
        "createdAt": {"$date": random_date(datetime(2023, 1, 1), datetime(2025, 10, 1)).isoformat() + "Z"},
        "updatedAt": {"$date": datetime.now().isoformat() + "Z"}
    }
    records.append(record)

# Save as JSON file
with open("dump_5000.json", "w") as f:
    for record in records:
        json.dump(record, f)
        f.write("\n")  # newline-delimited JSON, good for mongoimport

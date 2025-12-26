import math
from io import BytesIO

import numpy as np
from bson import ObjectId
from datetime import timedelta
from fastapi import UploadFile, HTTPException, APIRouter, File, Form, Depends
from motor.motor_asyncio import AsyncIOMotorClient
import pandas as pd
from dataclasses import dataclass
from typing import Any, Optional

from app import database
from app.core import security
from app.database import get_collection, db
import traceback
import logging

logger = logging.getLogger(__name__)

router = APIRouter()
job_cards_collection = get_collection("job_cards")
job_cards_invoice_items_collection = get_collection("job_cards_invoice_items")
job_cards_internal_notes_collection = get_collection("job_cards_internal_notes")
brands_collection = get_collection("all_brands")
models_collection = get_collection("all_brand_models")
list_collection = get_collection("all_lists")
value_collection = get_collection("all_lists_values")
countries_collection = get_collection("all_countries")
cities_collection = get_collection("all_countries_cities")
salesman_collection = get_collection("sales_man")
branches_collection = get_collection("branches")
currencies_collection = get_collection("currencies")
entity_information_collection = get_collection("entity_information")
import_errors_collection = get_collection("import_errors")


@dataclass
class ImportErrorInfo:
    row_number: int
    step: str
    field: Optional[str]
    value: Any
    error: str


def normalize_number_to_string(value):
    if value is None:
        return ""

    # إزالة الفواصل لو جاية من Excel
    if isinstance(value, str):
        value = value.replace(",", "").strip()
        if value == "":
            return ""

    try:
        num = float(value)
        # إذا الرقم صحيح (مثل 123.00)
        if num.is_integer():
            return str(int(num))
        # إذا فيه كسور (123.50)
        return str(num)
    except (ValueError, TypeError):
        # لو مش رقم أصلاً
        return str(value)


def clean_value(value, number: Optional[bool] = False):
    """
    Convert empty / NaN / float NaN to None,
    and strip & normalize strings.
    """
    if value is None:
        return None
    # check for pandas NaN or float NaN
    if isinstance(value, float) and math.isnan(value):
        return 0
    if number:
        s = value
    else:
        s = str(value).strip()
    if s == "":
        return None
    return s


def to_mongo_datetime(value):
    if pd.isna(value):
        return None
    return value.to_pydatetime()


async def create_entity_service(
        *,
        entity_name: str,
        entity_code: list[str],
        credit_limit: float,
        warranty_days: int,
        salesman_id: ObjectId | None,
        entity_status: str,
        group_name: str,
        industry_id: ObjectId | None,
        trn: str,
        entity_type_id: ObjectId | None,
        entity_address: list,
        entity_phone: list,
        entity_social: list,
        company_id: ObjectId,
        lpo_required: str
):
    if entity_address:
        for entity in entity_address:
            entity['country_id'] = ObjectId(entity['country_id']) if entity['country_id'] else None
            entity['city_id'] = ObjectId(entity['city_id']) if entity['city_id'] else None
    if entity_phone:
        for entity in entity_phone:
            entity['type_id'] = ObjectId(entity['type_id']) if entity['type_id'] else None

    if entity_social:
        for entity in entity_social:
            entity['type_id'] = ObjectId(entity['type_id']) if entity['type_id'] else None
    doc = {
        "entity_name": entity_name,
        "entity_code": entity_code,
        "credit_limit": credit_limit,
        "entity_picture": "",
        "entity_picture_public_id": "",
        "warranty_days": warranty_days,
        "salesman_id": salesman_id,
        "entity_status": entity_status,
        "group_name": group_name,
        "industry_id": industry_id,
        "trn": trn,
        "entity_type_id": entity_type_id,
        "company_id": company_id,
        "entity_address": entity_address,
        "entity_phone": entity_phone,
        "entity_social": entity_social,
        "status": True,
        "lpo_required": lpo_required,
        "createdAt": security.now_utc(),
        "updatedAt": security.now_utc(),
    }

    result = await entity_information_collection.insert_one(doc)
    return result.inserted_id


async def create_country_service(
        *,
        name: str,
        code: str = "",
        calling_code: str = "",
        currency_name: str = "",
        currency_code: str = "",
        vat: float = 0,
        flag_url: str = "",
        flag_public_id: str = "",
):
    country_dict = {
        "name": name,
        "code": code,
        "currency_name": currency_name,
        "currency_code": currency_code,
        "vat": vat,
        "flag": flag_url,
        "flag_public_id": flag_public_id,
        "calling_code": calling_code,
        "createdAt": security.now_utc(),
        "updatedAt": security.now_utc(),
        "status": True
    }

    insert_result = await countries_collection.insert_one(country_dict)
    return insert_result.inserted_id


def safe(row, field: str, *, number=False):
    try:
        return clean_value(getattr(row, field), number=number)
    except Exception as e:
        raise ValueError(f"{field}: {str(e)}")


@router.post('/get_file')
async def get_file(file: UploadFile = File(...), screen_name: str = Form(...),
                   delete_every_thing: bool = Form(...),
                   data: dict = Depends(security.get_current_user)):
    try:
        print(delete_every_thing)
        if screen_name.lower() == 'job cards':
            await dealing_with_job_cards(file, data, delete_every_thing)



    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Helper for MongoDB batch limits
def chunker(seq, size):
    for pos in range(0, len(seq), size):
        yield seq[pos:pos + size]


async def dealing_with_job_cards(file: UploadFile, data: dict, delete_every_thing: bool):
    # 1. SETUP SESSION & TRANSACTION
    async with await database.client.start_session() as session:
        async with session.start_transaction():
            try:
                company_id = ObjectId(data.get('company_id'))
                user_id = ObjectId(data.get('sub'))

                # --- STAGE 1: CLEANUP ---
                if delete_every_thing:
                    query = {'company_id': company_id}
                    await job_cards_collection.delete_many(query, session=session)
                    await job_cards_internal_notes_collection.delete_many(query, session=session)
                    await job_cards_invoice_items_collection.delete_many(query, session=session)
                    await salesman_collection.delete_many(query, session=session)
                    await entity_information_collection.delete_many(
                        {"company_id": company_id, "entity_code": "Customer"}, session=session
                    )

                # --- STAGE 2: VECTORIZED DATA CLEANING ---
                contents = await file.read()
                df = pd.read_excel(BytesIO(contents))
                df.columns = df.columns.str.strip().str.lower()

                # Convert all dates at once (Massively faster than row-by-row)
                date_cols = ["job_date", "invoice_date", "cancellation_date", "approval_date",
                             "start_date", "end_date", "delivery_date"]
                for col in date_cols:
                    if col in df.columns:
                        df[col] = pd.to_datetime(df[col], errors="coerce")

                # Fill NaNs for string operations to avoid errors
                df = df.replace({np.nan: None})

                # --- STAGE 3: MASSIVE CACHE LOAD (PRE-FETCH) ---
                # Fetching all references into memory to avoid DB calls in the loop
                brand_cache = {b["name"].upper(): b for b in await brands_collection.find().to_list(None)}
                model_cache = {m["name"].upper(): m["_id"] for m in await models_collection.find().to_list(None)}
                val_cache = {v["name"].upper(): v["_id"] for v in await value_collection.find().to_list(None)}
                city_cache = {c["name"].upper(): c["_id"] for c in await cities_collection.find().to_list(None)}
                sales_cache = {s["name"].upper(): s["_id"] for s in
                               await salesman_collection.find({"company_id": company_id}).to_list(None)}
                branch_cache = {b["name"].upper(): b["_id"] for b in
                                await branches_collection.find({"company_id": company_id}).to_list(None)}
                cust_cache = {c["entity_name"].upper(): c["_id"] for c in
                              await entity_information_collection.find({"company_id": company_id}).to_list(None)}

                # Static Reference IDs
                uae_doc = await countries_collection.find_one({"code": "UAE"})
                uae_id = uae_doc["_id"]
                curr_doc = await currencies_collection.find_one({"country_id": uae_id})

                color_list = await list_collection.find_one({"code": "COLORS"})
                color_list_id = str(color_list["_id"])

                # --- STAGE 4: RESOLVE MISSING METADATA (BULK CREATION) ---
                # Identify new brands that don't exist yet
                excel_brands = {str(b).strip().upper() for b in df['brand'].dropna().unique()}
                new_brands = excel_brands - set(brand_cache.keys())
                if new_brands:
                    new_docs = [{"name": b, "logo": None, "createdAt": security.now_utc()} for b in new_brands]
                    res = await brands_collection.insert_many(new_docs, session=session)
                    for idx, name in enumerate(new_brands):
                        brand_cache[name] = {"_id": res.inserted_ids[idx], "logo": None}

                # --- STAGE 5: THE LOOP ENGINE (ZERO AWAITS HERE) ---
                job_cards_to_insert = []
                notes_bulk_data = []  # To store (internal_note, job_index)

                for index, row in enumerate(df.itertuples(index=False)):
                    # Cache Lookups (Fastest)
                    brand_name = str(row.brand).strip().upper() if row.brand else None
                    brand_data = brand_cache.get(brand_name, {})

                    color_id = val_cache.get(str(row.color).strip().upper()) if row.color else None
                    city_id = city_cache.get(str(row.plate_city).strip().upper()) if row.plate_city else None
                    sales_id = sales_cache.get(str(row.job_sales_man).strip().upper()) if row.job_sales_man else None
                    branch_id = branch_cache.get(str(row.branch_name).strip().upper()) if row.branch_name else None
                    cust_id = cust_cache.get(str(row.customer_name).strip().upper()) if row.customer_name else None

                    # Mileage Logic
                    m_in = float(row.mileage_in) if row.mileage_in else 0
                    m_out = float(row.mileage_out) if row.mileage_out else 0

                    # Construct Job Document
                    job_doc = {
                        "company_id": company_id,
                        "car_brand": brand_data.get("_id"),
                        "car_brand_logo": brand_data.get("logo"),
                        "car_model": model_cache.get(str(row.model).strip().upper()) if row.model else None,
                        "color": color_id,
                        "plate_number": str(row.plate_number) if row.plate_number else "",
                        "country": uae_id,
                        "city": city_id,
                        "vehicle_identification_number": str(row.vin) if row.vin else "",
                        "mileage_in": m_in,
                        "mileage_out": m_out,
                        "mileage_in_out_diff": m_out - m_in,
                        "salesman": sales_id,
                        "branch": branch_id,
                        "currency": curr_doc["_id"],
                        "rate": float(curr_doc["rate"]),
                        "job_number": str(row.job_number),
                        "job_date": to_mongo_datetime(row.job_date),
                        "invoice_number": str(row.invoice_number) if row.invoice_number else "",
                        "invoice_date": to_mongo_datetime(row.invoice_date),
                        "customer": cust_id,
                        "contact_name": str(row.customer_name) if row.customer_name else "",
                        "createdAt": security.now_utc(),
                        "updatedAt": security.now_utc(),
                        "job_status_1": str(row.job_status).capitalize() if row.job_status else "Open",
                        "job_status_2": str(row.job_sub_status).capitalize() if row.job_sub_status else ""
                    }
                    job_cards_to_insert.append(job_doc)

                    # Queue Notes for later (Linked by index)
                    if row.internal_notes:
                        notes_bulk_data.append({"note": str(row.internal_notes), "job_idx": index})

                # --- STAGE 6: FINAL BULK INSERT ---
                if job_cards_to_insert:
                    # Insert Jobs
                    result = await job_cards_collection.insert_many(job_cards_to_insert, session=session)
                    inserted_ids = result.inserted_ids

                    # Insert Internal Notes (Linked to Job IDs)
                    if notes_bulk_data:
                        final_notes = []
                        for item in notes_bulk_data:
                            final_notes.append({
                                "job_card_id": inserted_ids[item["job_idx"]],
                                "company_id": company_id,
                                "notes": item["note"],
                                "user_id": user_id,
                                "type": "text",
                                "createdAt": security.now_utc()
                            })
                        await job_cards_internal_notes_collection.insert_many(final_notes, session=session)

                return {"status": "success", "processed": len(job_cards_to_insert)}

            except Exception as e:
                print(e)
                # If anything goes wrong, MongoDB rolls back all deletions and insertions
                raise HTTPException(status_code=500, detail=f"Bulk Import Failed: {str(e)}")

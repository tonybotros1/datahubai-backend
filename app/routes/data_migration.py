from datetime import timedelta
from typing import Any, Optional
from bson import ObjectId
from fastapi import APIRouter, HTTPException, Depends, UploadFile, File, Form
from app.core import security
from app.database import get_collection
import math

from app.routes.banks_and_others import BanksModel, add_new_bank
from app.routes.branches import add_new_branch
from app.routes.brands_and_models import create_brand, add_new_model
from app.routes.countries_and_cities import add_new_city
from app.routes.invoice_items import InvoiceItem, add_new_invoice_item
from app.routes.list_of_values import add_new_value
from app.routes.salesman import add_new_salesman, SaleManModel
import pandas as pd
from io import BytesIO

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
invoice_items_collection = get_collection("invoice_items")
receipts_collection = get_collection("all_receipts")
receipts_invoices_collection = get_collection("all_receipts_invoices")
banks_collection = get_collection("all_banks")


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


# =========================== main function section ===========================
@router.post('/get_file')
async def get_file(file: UploadFile = File(...), screen_name: str = Form(...),
                   delete_every_thing: bool = Form(...),
                   data: dict = Depends(security.get_current_user)):
    try:
        print(delete_every_thing)
        if screen_name.lower() == 'job cards':
            await dealing_with_job_cards(file, data, delete_every_thing)
        elif screen_name.lower() == 'job cards invoice items':
            await dealing_with_job_cards_items(file, data, delete_every_thing)
        elif screen_name.lower() == 'ar receipts':
            await dealing_with_ar_receipts(file, data, delete_every_thing)
        elif screen_name.lower() == 'ar receipts items':
            await dealing_with_ar_receipts_invoices(file, data, delete_every_thing)



    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# =========================== ar receipts section ===========================
async def dealing_with_ar_receipts(file: UploadFile, data: dict, delete_every_thing: bool):
    try:
        company_id = ObjectId(data.get('company_id'))
        user_id = ObjectId(data.get('sub'))
        if delete_every_thing:
            await receipts_collection.delete_many({'company_id': company_id})
            await receipts_invoices_collection.delete_many({'company_id': company_id})

        contents = await file.read()
        df = pd.read_excel(BytesIO(contents))
        df.columns = df.columns.str.strip().str.lower()
        # print(df.columns)
        date_cols = ["receipt_date", "cheque_date"]
        for col in date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")
        df_2025 = df[(df["receipt_date"].dt.year == 2025)]
        # print(df_2025["receipt_type"].value_counts())
        # print(df_2025.head())

        # getting data section
        bank_name_doc = await list_collection.find_one({"code": "BANKS"})
        bank_name_list_id = str(bank_name_doc["_id"])
        account_types_bank_doc = await value_collection.find_one({"name": "Bank"})
        account_types_list_id = str(account_types_bank_doc["_id"])
        existing_customers = {b['entity_name']: b for b in
                              await entity_information_collection.find({}).to_list(length=None)}
        existing_values = {b["name"].capitalize(): ObjectId(b["_id"]) for b in
                           await value_collection.find({}).to_list(length=None)}
        existing_banks = {b["account_number"]: ObjectId(b["_id"]) for b in
                          await banks_collection.find({}).to_list(length=None)}
        uae_country_doc = await countries_collection.find_one({"code": "UAE"})
        uae_country_id = str(uae_country_doc["_id"])
        uae_currency_doc = await currencies_collection.find_one({"country_id": ObjectId(uae_country_id)})
        uae_currency_id = str(uae_currency_doc["_id"])

        for i, row in enumerate(df_2025.itertuples(index=False), start=1):
            receipt_id = clean_value(row[0])
            receipt_number = normalize_number_to_string(row[1])
            receipt_date = to_mongo_datetime(row[2])
            customer_name = clean_value(row[3])
            receipt_type = clean_value(row[4])
            status = clean_value(row[5])
            bank_name = clean_value(row[10])
            cheque_date = to_mongo_datetime(row[11])
            account_number = clean_value(row[6])

            customer_id = None
            if customer_name:
                customer_data = existing_customers.get(customer_name.strip())
                if customer_data:
                    customer_id = customer_data["_id"]
                else:
                    customer_id = None

            if receipt_type:
                receipt_type_id = existing_values.get(str(receipt_type).capitalize().strip())
                if not receipt_type_id:
                    receipt_type_id = None
            else:
                receipt_type_id = None

            try:
                if bank_name:
                    bank_name_id = existing_values.get(str(bank_name).strip())
                    if not bank_name_id:
                        new_bank_name = await add_new_value(list_id=bank_name_list_id, name=str(bank_name),
                                                            mastered_by_id=None)
                        bank_name_id = ObjectId(new_bank_name['list']['_id'])
                        existing_values[str(bank_name)] = bank_name_id
                else:
                    bank_name_id = None

            except Exception as e:
                print(f"error in bank name {e}")
                raise

            try:
                if account_number:
                    account_id = existing_banks.get(str(account_number).strip())
                    if not account_id:
                        account_model = BanksModel(
                            account_name=account_number,
                            account_number=account_number,
                            currency_id=uae_currency_id,
                            account_type_id=account_types_list_id
                        )
                        new_account = await add_new_bank(bank=account_model, data=data)
                        account_id = ObjectId(new_account['account']['_id'])
                        existing_banks[str(new_account).strip()] = account_id
                else:
                    account_id = None
            except Exception as e:
                print(f"error in account {e}")
                raise

            # insert data:
            receipt_dict = {
                "company_id": company_id,
                "receipt_id": receipt_id,
                "receipt_number": receipt_number,
                "receipt_date": receipt_date,
                "customer": customer_id,
                "receipt_type": receipt_type_id,
                "status": status.capitalize(),
                "currency": 'AED',
                "rate": clean_value(row[8],number=True),
                "cheque_number": clean_value(row[9]) if clean_value(row[9]) != 0 else "",
                "cheque_date": cheque_date,
                "note": clean_value(row[12]) if clean_value(row[12]) != 0 else "",
                "bank_name": bank_name_id,
                "account": account_id,
                "createdAt": security.now_utc(),
                "updatedAt": security.now_utc(),
            }
            await receipts_collection.insert_one(receipt_dict)
            print(f"added {i}")

    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=str(e))


# =========================== ar receipts invoices section ===========================
async def dealing_with_ar_receipts_invoices(file: UploadFile, data: dict, delete_every_thing: bool):
    try:
        company_id = ObjectId(data.get("company_id"))

        if delete_every_thing:
            await receipts_invoices_collection.delete_many({"company_id": company_id})

        contents = await file.read()
        df = pd.read_excel(BytesIO(contents))
        df.columns = df.columns.str.strip().str.lower()
        existing_ar_receipts = {b.get("receipt_id", None): ObjectId(b["_id"]) for b in
                                await receipts_collection.find({"company_id": company_id}).to_list()}
        existing_job_cards = {b.get("job_id", None): ObjectId(b["_id"]) for b in
                              await job_cards_collection.find({"company_id": company_id}).to_list()}

        invoice_items_buffer = []

        for i, row in enumerate(df.itertuples(index=False), start=1):
            receipt_id = normalize_number_to_string(row[0])
            job_id = clean_value(row[1])
            amount = clean_value(row[2], number=True)

            if job_id:
                job_card_id = existing_job_cards.get(int(job_id), None)
            else:
                job_card_id = None


            if receipt_id:
                new_receipt_id = existing_ar_receipts.get(receipt_id, None)
            else:
                new_receipt_id = None
            if new_receipt_id and job_card_id:
                invoice_dict = {
                    "company_id": company_id,
                    "receipt_id" : new_receipt_id,
                    "job_id": job_card_id,
                    "amount": amount,
                    "createdAt": security.now_utc(),
                    "updatedAt": security.now_utc(),
                }
                invoice_items_buffer.append(invoice_dict)
                print(f"Row: {i}")

        if invoice_items_buffer:
            await receipts_invoices_collection.insert_many(invoice_items_buffer)


    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=str(e))


# =========================== job cards section ===========================

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
    doc["_id"] = str(result.inserted_id)
    return doc


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


async def dealing_with_job_cards(file: UploadFile, data: dict, delete_every_thing: bool):
    try:
        company_id = ObjectId(data.get('company_id'))
        user_id = ObjectId(data.get('sub'))
        if delete_every_thing:
            await job_cards_collection.delete_many({'company_id': company_id})
            await job_cards_internal_notes_collection.delete_many({'company_id': company_id})
            await job_cards_invoice_items_collection.delete_many({'company_id': company_id})
            await salesman_collection.delete_many({'company_id': company_id})
            await entity_information_collection.delete_many({
                "company_id": company_id,
                "entity_code": {"$in": ["Customer"]}
            })

        contents = await file.read()
        df = pd.read_excel(BytesIO(contents))
        df.columns = df.columns.str.strip().str.lower()
        date_cols = ["job_date", "invoice_date", "cancellation_date", "approval_date",
                     "start_date", "end_date", "delivery_date", "invoice_new_date"]
        for col in date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")
        df_2025 = df[
            (df["job_date"].dt.year == 2025) |
            (df["invoice_date"].dt.year == 2025) |
            (df["cancellation_date"].dt.year == 2025)
            ]

        color_doc = await list_collection.find_one({"code": "COLORS"})
        color_list_id = str(color_doc["_id"])
        uae_country_doc = await countries_collection.find_one({"code": "UAE"})
        uae_country_id = str(uae_country_doc["_id"])
        uae_currency_doc = await currencies_collection.find_one({"country_id": ObjectId(uae_country_id)})
        uae_currency_id = str(uae_currency_doc["_id"])
        uae_currency_rate = float(uae_currency_doc["rate"])
        phone_type_work_doc = await value_collection.find_one({"name": "Work"})
        phone_type_work_id = str(phone_type_work_doc["_id"])
        phone_type_personal_doc = await value_collection.find_one({"name": "Personal"})
        phone_type_personal_id = str(phone_type_personal_doc["_id"])
        website_www_doc = await value_collection.find_one({"name": "WWW"})
        website_www_id = str(website_www_doc["_id"])

        existing_brands = {b["name"].upper(): {"_id": ObjectId(b["_id"]), "logo": b.get("logo")} for b in
                           await brands_collection.find({}).to_list(length=None)}
        existing_models = {b["name"].upper(): ObjectId(b["_id"]) for b in
                           await models_collection.find({}).to_list(length=None)}
        existing_values = {b["name"].upper(): ObjectId(b["_id"]) for b in
                           await value_collection.find({}).to_list(length=None)}
        existing_cities = {b['name']: ObjectId(b['_id']) for b in await cities_collection.find({}).to_list(length=None)}
        existing_salesman = {b['name']: ObjectId(b['_id']) for b in
                             await salesman_collection.find({}).to_list(length=None)}
        existing_branches = {b['name']: ObjectId(b['_id']) for b in
                             await branches_collection.find({}).to_list(length=None)}
        existing_customers = {b['entity_name'].capitalize().strip() : b for b in
                              await entity_information_collection.find({}).to_list(length=None)}

        for i, row in enumerate(df_2025.itertuples(index=False), start=1):
            # for row in df[:50].itertuples(index=False):  # 20604
            brand = clean_value(row[1])
            model = clean_value(row[2])
            color = clean_value(row[4])
            city = clean_value(row[7])
            salesman = clean_value(row[13])
            branch = clean_value(row[14])
            payment_type = clean_value(row[17])
            mileage_in = clean_value(row[10], number=True)
            mileage_out = clean_value(row[11], number=True)
            job_min_test_km = clean_value(row[12], number=True)
            returned = clean_value(row[18])
            job_number = clean_value(row[19])
            job_date = row[20]
            invoice_number = normalize_number_to_string(clean_value(row[21]))
            invoice_date = row[22]
            job_cancellation_date = row[23]
            lpo_number = clean_value(row[24])
            job_approval_date = row[25]
            job_start_date = row[26]
            job_finish_date = row[27]
            job_delivery_date = row[28]
            job_warranty_days = clean_value(row[29], number=True)
            job_warranty_km = clean_value(row[30], number=True)
            reference_1 = clean_value(row[31])
            reference_2 = clean_value(row[32])
            invoice_new_date = row[33]
            job_notes = clean_value(row[34])
            job_delivery_notes = clean_value(row[35])
            job_status_1 = clean_value(row[37])
            job_status_2 = "Closed" if job_status_1.capitalize() == "Posted" else "Cancelled" if job_status_1.capitalize() == "Cancelled" else clean_value(
                row[38])
            customer = clean_value(row[39])
            customer_group_name = clean_value(row[40])
            trn = clean_value(row[41])
            entity_status = clean_value(row[42])
            customer_phone_name = clean_value(row[43])
            customer_phone_number = clean_value(row[44])
            customer_phone_work_number = clean_value(row[45])
            customer_phone_email = clean_value(row[46])
            customer_website = clean_value(row[47])
            customer_address_line = clean_value(row[48])
            customer_address_country = clean_value(row[49])
            customer_address_city = clean_value(row[50])
            customer_credit_limit = clean_value(row[51], number=True)
            customer_warranty_days = clean_value(row[52], number=True)
            customer_salesman = clean_value(row[53])
            customer_lpo_required = clean_value(row[54])
            internal_notes = clean_value(row[29])

            try:
                # brand section
                if brand:
                    # brand_id: Any = existing_brands.get(brand.upper())['_id']
                    # brand_logo = existing_brands.get(brand.upper())['logo']
                    brand_data = existing_brands.get(brand.upper())
                    if brand_data:
                        brand_id = brand_data["_id"]
                        brand_logo = brand_data.get("logo")
                    else:
                        brand_id = None
                        brand_logo = None
                    if not brand_id:
                        new_brand = await create_brand(name=str(brand).upper(), logo=None)
                        print("added new brand")
                        brand_id: Any = ObjectId(new_brand['brand']['_id'])
                        existing_brands[str(brand).upper()] = {"_id": brand_id, "logo": brand_logo}
                        # existing_brands[str(brand).upper()] = brand_id
                else:
                    brand_id = None
                    brand_logo = None
            except Exception as row_err:
                # This will show exactly which row failed
                print(f"Error Brand processing row {i}: {row_err}")
                raise

                # model section
            try:
                if model and brand:
                    model_id = existing_models.get(str(model).upper())
                    if not model_id:
                        new_model = await add_new_model(brand_id=str(brand_id), name=str(model).upper())
                        print("added new model")
                        model_id = ObjectId(new_model['model']['_id'])
                        existing_models[str(model).upper()] = model_id

                else:
                    model_id = None
            except Exception as row_err:
                # This will show exactly which row failed
                print(f"Error Model processing row {i}: {row_err}")
                raise

                # color section
            try:
                if color:
                    color_id = existing_values.get(str(color).upper())
                    if not color_id:
                        new_color = await add_new_value(list_id=color_list_id, name=str(color).upper(),
                                                        mastered_by_id=None)
                        color_id = ObjectId(new_color['list']['_id'])
                        existing_values[str(color).upper()] = color_id
                else:
                    color_id = None
            except Exception as row_err:
                # This will show exactly which row failed
                print(f"Error Color processing row {i}: {row_err}")
                raise

            # cities section
            try:
                if city:
                    city_id = existing_cities.get(city)
                    if not city_id:
                        new_city = await add_new_city(country_id=str(uae_country_id), name=city, code=str(city).upper())
                        city_id = ObjectId(new_city['City']['_id'])
                        existing_cities[city] = city_id
                else:
                    city_id = None
            except Exception as row_err:
                # This will show exactly which row failed
                print(f"Error City processing row {i}: {row_err}")
                raise

            # salesman section
            try:
                if salesman:
                    salesman_id = existing_salesman.get(str(salesman).upper())
                    if not salesman_id:
                        sale_man_model = SaleManModel(
                            name=str(salesman).upper(),
                            target=0
                        )
                        new_salesman = await add_new_salesman(sale_man=sale_man_model, data=data)
                        salesman_id = ObjectId(new_salesman['salesman']['_id'])
                        existing_salesman[str(salesman).upper()] = salesman_id
                else:
                    salesman_id = None
            except Exception as row_err:
                # This will show exactly which row failed
                print(f"Error Salesman processing row {i}: {row_err}")
                raise

            # branches section
            try:
                if branch:
                    branch_id = existing_branches.get(str(branch).upper())
                    if not branch_id:
                        new_branch = await add_new_branch(name=str(branch).upper(), code=None, line=None,
                                                          country_id=None,
                                                          city_id=None, data=data)
                        branch_id = ObjectId(new_branch['branch']['_id'])
                        existing_branches[str(branch).upper()] = branch_id
                else:
                    branch_id = None
            except Exception as row_err:
                # This will show exactly which row failed
                print(f"Error Branch processing row {i}: {row_err}")
                raise

            try:
                # customer section
                if customer:
                    # customer_id = existing_customers.get(customer.strip())['_id']
                    customer_data = existing_customers.get(customer.capitalize().strip())
                    if customer_data:
                        customer_id = customer_data["_id"]
                    else:
                        customer_id = None
                    if not customer_id:
                        try:
                            if customer_salesman:
                                customer_salesman_id_for_new_customer = existing_salesman.get(
                                    str(customer_salesman).upper())
                                if not customer_salesman_id_for_new_customer:
                                    sale_man_model = SaleManModel(
                                        name=str(customer_salesman).upper(),
                                        target=0
                                    )
                                    new_salesman = await add_new_salesman(sale_man=sale_man_model, data=data)
                                    print("added new customer salesman")
                                    customer_salesman_id_for_new_customer = ObjectId(new_salesman['salesman']['_id'])
                                    existing_salesman[str(salesman).upper()] = customer_salesman_id_for_new_customer
                            else:
                                customer_salesman_id_for_new_customer = None
                        except Exception as row_err:
                            # This will show exactly which row failed
                            print(f"Error Customer salesman processing row {i}: {row_err}")
                            raise

                        try:
                            if customer_address_city:
                                customer_address_city = existing_cities.get(city)
                                if not customer_address_city:
                                    new_city = await add_new_city(country_id=str(uae_country_id), name=city,
                                                                  code=str(city).upper())
                                    print("added new customer city country")

                                    customer_address_city = ObjectId(new_city['City']['_id'])
                                    existing_cities[city] = city_id
                            else:
                                customer_address_city = None
                        except Exception as row_err:
                            # This will show exactly which row failed
                            print(f"Error customer city country processing row {i}: {row_err}")
                            raise

                        address_list = [
                            {
                                "line": customer_address_line,
                                "isPrimary": True,
                                "country_id": uae_country_id,
                                "city_id": customer_address_city,
                            }
                        ]

                        phone_list = [
                            {
                                "number": str(customer_phone_work_number),
                                "name": str(customer_phone_name),
                                "job_title": "",
                                "email": str(customer_phone_email),
                                "isPrimary": True,
                                "type_id": ObjectId(phone_type_work_id)
                            },
                            {
                                "number": str(customer_phone_number),
                                "name": str(customer_phone_name),
                                "job_title": "",
                                "email": str(customer_phone_email),
                                "isPrimary": False,
                                "type_id": ObjectId(phone_type_personal_id)
                            },

                        ]

                        website_list = [
                            {
                                "type_id": ObjectId(website_www_id),
                                "link": str(customer_website)
                            }
                        ]
                        customer_details = await create_entity_service(entity_name=str(customer.capitalize().strip()),
                                                                       entity_code=['Customer'],
                                                                       credit_limit=float(customer_credit_limit),
                                                                       warranty_days=int(customer_warranty_days),
                                                                       salesman_id=customer_salesman_id_for_new_customer,
                                                                       entity_status=str(entity_status),
                                                                       group_name=str(customer_group_name),
                                                                       industry_id=None, trn=str(trn),
                                                                       entity_type_id=None,
                                                                       entity_address=address_list,
                                                                       entity_phone=phone_list,
                                                                       entity_social=website_list,
                                                                       company_id=company_id,
                                                                       lpo_required=str(customer_lpo_required)
                                                                       )
                        print("added new customer")
                        existing_customers[str(customer)] = customer_details
                        customer_id = customer_details['_id']

                else:
                    customer_id = None
            except Exception as row_err:
                # This will show exactly which row failed
                print(f"Error Customer processing row {i}: {row_err} : job id : {clean_value(row[0], number=True)}")
                raise

            job_dict = {
                "job_id": clean_value(row[0], number=True),
                "car_brand_logo": brand_logo,
                "company_id": company_id,
                "car_brand": brand_id,
                "car_model": model_id,
                "year": clean_value(row[3]),
                "color": color_id,
                "plate_number": clean_value(row[5]),
                "plate_code": clean_value(row[6]),
                "country": ObjectId(uae_country_id),
                "city": city_id,
                "vehicle_identification_number": clean_value(row[8]),
                "type": clean_value(row[9]),
                "mileage_in": mileage_in,
                "mileage_out": mileage_out,
                "mileage_in_out_diff": mileage_out - mileage_in,
                "fuel_amount": 0,
                "job_min_test_km": job_min_test_km,
                "salesman": salesman_id,
                "branch": branch_id,
                "currency": ObjectId(uae_currency_id),
                "rate": uae_currency_rate if uae_currency_rate else 0,
                "payment_method": payment_type.capitalize() if payment_type == 'CREDIT' else 'Cash',
                "label": returned.capitalize() if returned == 'RETURNED' else "",
                "job_number": job_number,
                "job_date": to_mongo_datetime(job_date),
                "invoice_number": invoice_number if invoice_number != "0" else "",
                "invoice_date": to_mongo_datetime(invoice_date),
                "job_cancellation_date": to_mongo_datetime(job_cancellation_date),
                "lpo_number": lpo_number,
                "job_approval_date": to_mongo_datetime(job_approval_date),
                "job_start_date": to_mongo_datetime(job_start_date),
                "job_finish_date": to_mongo_datetime(job_finish_date),
                "job_delivery_date": to_mongo_datetime(job_delivery_date) if to_mongo_datetime(
                    job_delivery_date) else to_mongo_datetime(job_date),
                "job_warranty_days": int(job_warranty_days),
                "job_warranty_km": float(job_warranty_km),
                "job_reference_1": reference_1 if reference_1 else "",
                "job_reference_2": reference_2 if reference_2 else "",
                "invoice_new_date": to_mongo_datetime(invoice_new_date),
                "job_notes": job_notes if job_notes else "",
                "job_delivery_notes": job_delivery_notes if job_delivery_notes else "",
                "job_status_1": job_status_1.capitalize(),
                "job_status_2": job_status_2.capitalize(),
                "customer": ObjectId(customer_id),
                "contact_name": customer,
                "contact_number": customer_phone_work_number,
                "contact_email": customer_phone_email,
                "credit_limit": float(customer_credit_limit),
                "engine_type": None,
                "transmission_type": "",
                "job_warranty_end_date": (
                    (dt + timedelta(days=float(job_warranty_days)))
                    if (dt := to_mongo_datetime(job_delivery_date)) else None
                ),
                "delivery_time": None,
                "createdAt": security.now_utc(),
                "updatedAt": security.now_utc(),

            }
            # print(job_dict)
            result = await job_cards_collection.insert_one(job_dict)
            job_id = result.inserted_id
            internal_note_dict = {}
            if internal_notes:
                internal_note_dict["type"] = 'text'
                internal_note_dict["job_card_id"] = job_id
                internal_note_dict['company_id'] = company_id
                internal_note_dict["notes"] = internal_notes
                internal_note_dict["user_id"] = user_id
                internal_note_dict["createdAt"] = security.now_utc()
                internal_note_dict["updatedAt"] = security.now_utc()
                internal_note_dict["file_type"] = None
                internal_note_dict["note_public_id"] = None
                await job_cards_internal_notes_collection.insert_one(internal_note_dict)
            print(str(job_id))



    except Exception as e:
        print(f"Fatal error in dealing_with_job_cards: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# =========================== job cards items section ===========================

async def dealing_with_job_cards_items(file: UploadFile, data: dict, delete_every_thing: bool):
    try:
        company_id = ObjectId(data.get('company_id'))
        if delete_every_thing:
            await job_cards_invoice_items_collection.delete_many({"company_id": company_id})
            await invoice_items_collection.delete_many({"company_id": company_id})

        contents = await file.read()
        df = pd.read_excel(BytesIO(contents))
        df.columns = df.columns.str.strip().str.lower()
        df["new"] = df["item_code"] + "-" + df["item_name"]
        print(df.head())
        existing_invoice_items = {b["name"]: ObjectId(b["_id"]) for b in
                                  await invoice_items_collection.find({"company_id": company_id}).to_list(
                                      length=None)}
        existing_job_cards = {b.get("job_id", None): ObjectId(b["_id"]) for b in
                              await job_cards_collection.find({"company_id": company_id}).to_list()}

        invoice_items_buffer = []

        for i, row in enumerate(df.itertuples(index=False), start=1):
            job_id = clean_value(row[0])
            item_code = clean_value(row[1])
            item_name = clean_value(row[2])
            item_description = clean_value(row[3])
            quantity = clean_value(row[4], number=True)
            price = clean_value(row[5], number=True)
            vat = clean_value(row[6], number=True)
            discount = clean_value(row[7], number=True)

            quantity = quantity or 0
            price = price or 0
            vat = vat or 0
            discount = discount or 0

            amount = price * quantity
            total = amount - discount
            net = total + vat

            item_id = None
            if job_id:
                job_card_id = existing_job_cards.get(int(job_id), None)
                if job_card_id:
                    if item_name:
                        item_id = existing_invoice_items.get(str(item_name))
                        if not item_id:
                            item_model = InvoiceItem(
                                name=(str(item_name)),
                                price=0,
                                description=str(item_description),
                            )
                            new_item = await add_new_invoice_item(invoice=item_model, data=data)
                            item_id = ObjectId(new_item['invoice']['_id'])
                            existing_invoice_items[str(item_code + " - " + item_name)] = item_id
                            print(f"yes added new invoice item: {item_id}")

                    else:
                        item_id = None

            else:
                job_card_id = None

            if job_card_id:
                invoice_items_dict = {
                    "company_id": company_id,
                    "job_card_id": ObjectId(job_card_id),
                    "line_number": 0,
                    "quantity": quantity,
                    "price": price,
                    "vat": vat,
                    "discount": discount,
                    "amount": amount,
                    "total": total,
                    "net": net,
                    "name": ObjectId(item_id),
                    "description": str(item_description),
                    "createdAt": security.now_utc(),
                    "updatedAt": security.now_utc(),
                }
                invoice_items_buffer.append(invoice_items_dict)
                print(i)

        if invoice_items_buffer:
            print(len(invoice_items_buffer))
            await job_cards_invoice_items_collection.insert_many(invoice_items_buffer)


    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=str(e))

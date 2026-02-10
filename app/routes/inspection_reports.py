import copy
import json
from typing import Optional, List, Any
from bson import ObjectId
from fastapi import APIRouter, HTTPException, Depends, UploadFile, Form, File
from app import database
from app.core import security
from app.database import get_collection
from datetime import datetime
from app.routes.counters import create_custom_counter
from app.routes.job_cards import get_user_branches
from app.websocket_config import manager
from app.widgets import upload_images

router = APIRouter()
job_cards_collection = get_collection("job_cards")
job_cards_inspection_reports_collection = get_collection("job_cards_inspection_reports")


def serializer(doc: dict) -> dict:
    def convert(value):
        if isinstance(value, ObjectId):
            return str(value)
        elif isinstance(value, datetime):
            return value.isoformat()
        elif isinstance(value, list):
            return [convert(v) for v in value]
        elif isinstance(value, dict):
            return {k: convert(v) for k, v in value.items()}
        return value

    return {k: convert(v) for k, v in doc.items()}


def safe_json_load(s):
    return json.loads(s) if s else None


inspection_reports_pipeline = [
    {
        '$lookup': {
            'from': 'all_lists_values',
            'let': {
                'technicianId': '$technician',
                'colorId': '$color',
                'engineTypeId': '$engine_type'
            },
            'pipeline': [
                {
                    '$match': {
                        '$expr': {
                            '$in': [
                                '$_id', [
                                    '$$technicianId', '$$colorId', '$$engineTypeId'
                                ]
                            ]
                        }
                    }
                }, {
                    '$project': {
                        'name': 1
                    }
                }
            ],
            'as': 'lists_cache'
        }
    }, {
        '$lookup': {
            'from': 'all_brands',
            'localField': 'car_brand',
            'foreignField': '_id',
            'pipeline': [
                {
                    '$project': {
                        'name': 1,
                        'logo': 1
                    }
                }
            ],
            'as': 'brand_details'
        }
    }, {
        '$lookup': {
            'from': 'all_brand_models',
            'localField': 'car_model',
            'foreignField': '_id',
            'pipeline': [
                {
                    '$project': {
                        'name': 1
                    }
                }
            ],
            'as': 'model_details'
        }
    }, {
        '$lookup': {
            'from': 'entity_information',
            'localField': 'customer',
            'foreignField': '_id',
            'pipeline': [
                {
                    '$project': {
                        'entity_name': 1
                    }
                }
            ],
            'as': 'customer_details'
        }
    }, {
        '$lookup': {
            'from': 'job_cards_inspection_reports',
            'localField': '_id',
            'foreignField': 'job_card_id',
            'pipeline': [
                {
                    '$addFields': {
                        '_id': {
                            '$toString': '$_id'
                        },
                        'company_id': {
                            '$toString': '$company_id'
                        },
                        'job_card_id': {
                            '$toString': '$job_card_id'
                        }
                    }
                }
            ],
            'as': 'inspection_report_details'
        }
    }, {
        '$unwind': {
            'path': '$inspection_report_details',
            'preserveNullAndEmptyArrays': True
        }
    }, {
        '$addFields': {
            'technician_name': {
                '$let': {
                    'vars': {
                        'match': {
                            '$arrayElemAt': [
                                {
                                    '$filter': {
                                        'input': '$lists_cache',
                                        'cond': {
                                            '$eq': [
                                                '$$this._id', '$technician'
                                            ]
                                        }
                                    }
                                }, 0
                            ]
                        }
                    },
                    'in': '$$match.name'
                }
            },
            'color_name': {
                '$let': {
                    'vars': {
                        'match': {
                            '$arrayElemAt': [
                                {
                                    '$filter': {
                                        'input': '$lists_cache',
                                        'cond': {
                                            '$eq': [
                                                '$$this._id', '$color'
                                            ]
                                        }
                                    }
                                }, 0
                            ]
                        }
                    },
                    'in': '$$match.name'
                }
            },
            'engine_type_name': {
                '$let': {
                    'vars': {
                        'match': {
                            '$arrayElemAt': [
                                {
                                    '$filter': {
                                        'input': '$lists_cache',
                                        'cond': {
                                            '$eq': [
                                                '$$this._id', '$engine_type'
                                            ]
                                        }
                                    }
                                }, 0
                            ]
                        }
                    },
                    'in': '$$match.name'
                }
            },
            'customer_name': {
                '$arrayElemAt': [
                    '$customer_details.entity_name', 0
                ]
            },
            'car_brand_name': {
                '$arrayElemAt': [
                    '$brand_details.name', 0
                ]
            },
            'car_brand_logo': {
                '$arrayElemAt': [
                    '$brand_details.logo', 0
                ]
            },
            'car_model_name': {
                '$arrayElemAt': [
                    '$model_details.name', 0
                ]
            },
            'job_warranty_end_date': {
                '$ifNull': [
                    '$job_warranty_end_date', None
                ]
            }
        }
    }, {
        '$project': {
            'lists_cache': 0,
            'customer_details': 0,
            'brand_details': 0,
            'model_details': 0,
            'invoice_items': 0
        }
    }, {
        '$addFields': {
            '_id': {
                '$toString': '$_id'
            },
            'car_brand': {
                '$toString': '$car_brand'
            },
            'car_model': {
                '$toString': '$car_model'
            },
            'country': {
                '$toString': '$country'
            },
            'customer': {
                '$toString': '$customer'
            },
            'currency': {
                '$toString': '$currency'
            },
            'company_id': {
                '$toString': '$company_id'
            },
            'color': {
                '$toString': '$color'
            },
            'city': {
                '$toString': '$city'
            },
            'branch': {
                '$toString': '$branch'
            },
            'salesman': {
                '$toString': '$salesman'
            },
            'engine_type': {
                '$toString': '$engine_type'
            },
            'technician': {
                '$toString': '$technician'
            }
        }
    }
]


@router.get("/get_current_job_card_inspection_report_details/{job_id}")
async def get_current_job_card_inspection_report_details(job_id: str, _: dict = Depends(security.get_current_user)):
    try:
        job_id = ObjectId(job_id)
        new_pipeline = copy.deepcopy(inspection_reports_pipeline)
        new_pipeline.insert(0, {
            "$match": {
                "_id": job_id
            }
        })
        cursor = await job_cards_collection.aggregate(new_pipeline)
        result = await cursor.next()
        return {"inspection_report": result}

    except HTTPException:
        raise
    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/get_new_job_cards_inspection_reports")
async def get_new_job_cards_inspection_reports(data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get('company_id'))
        user_id = ObjectId(data.get('sub'))
        user_branches = await get_user_branches(user_id)
        if user_branches:
            branch_filters = [{"branch": b, "company_id": company_id} for b in user_branches]
            match_stage: Any = {"$or": branch_filters}
        else:
            match_stage = {"company_id": company_id}
        match_stage['job_status_2'] = 'Draft'

        new_pipeline = copy.deepcopy(inspection_reports_pipeline)
        new_pipeline.insert(0, {
            "$addFields": {
                "date_field_to_filter": {
                    "$switch": {
                        "branches": [
                            {
                                "case": {"$eq": [{"$toLower": "$job_status_1"}, "new"]},
                                "then": "$job_date"
                            },
                            {
                                "case": {"$eq": [{"$toLower": "$job_status_1"}, "cancelled"]},
                                "then": "$job_cancellation_date"
                            },
                            {
                                "case": {"$eq": [{"$toLower": "$job_status_1"}, "posted"]},
                                "then": "$invoice_date"
                            }
                        ],
                        "default": "$job_date"
                    }
                }
            }
        })
        new_pipeline.insert(1,{"$match": match_stage},)
        new_pipeline.insert(2, {
            "$sort": {
                "date_field_to_filter": -1
            }
        })
        cursor = await job_cards_collection.aggregate(new_pipeline)
        results = await cursor.to_list(None)
        return {"inspection_reports": results}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/get_done_job_cards_inspection_reports")
async def get_done_job_cards_inspection_reports(data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get('company_id'))
        user_id = ObjectId(data.get('sub'))
        new_pipeline: Any = copy.deepcopy(inspection_reports_pipeline)
        user_branches = await get_user_branches(user_id)
        if user_branches:
            branch_filters = [{"branch": b, "company_id": company_id} for b in user_branches]
            match_stage: Any = {"$or": branch_filters}
        else:
            match_stage = {"company_id": company_id}
        match_stage['job_status_2'] = {"$ne": "Draft"}


        new_pipeline.insert(0, {
            "$addFields": {
                "date_field_to_filter": {
                    "$switch": {
                        "branches": [
                            {
                                "case": {"$eq": [{"$toLower": "$job_status_1"}, "new"]},
                                "then": "$job_date"
                            },
                            {
                                "case": {"$eq": [{"$toLower": "$job_status_1"}, "cancelled"]},
                                "then": "$job_cancellation_date"
                            },
                            {
                                "case": {"$eq": [{"$toLower": "$job_status_1"}, "posted"]},
                                "then": "$invoice_date"
                            }
                        ],
                        "default": "$job_date"
                    }
                }
            }
        })
        # new_pipeline.insert(1, {
        #     "$match": {
        #         "job_status_2": {"$ne": "Draft"}
        #     }
        # })
        new_pipeline.insert(1,{"$match": match_stage},)
        new_pipeline.insert(2, {
            "$sort": {
                "date_field_to_filter": -1
            }
        })
        new_pipeline.append({"$limit": 200})
        cursor = await job_cards_collection.aggregate(new_pipeline)
        results = await cursor.to_list(None)
        return {"inspection_reports": results}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/create_job_from_inspection_report")
async def create_job_from_inspection_report(job_date: Optional[datetime] = Form(None),
                                            technician: Optional[str] = Form(None),
                                            customer: Optional[str] = Form(None),
                                            customer_name: Optional[str] = Form(None),
                                            customer_email: Optional[str] = Form(None),
                                            customer_phone: Optional[str] = Form(None),
                                            credit_limit: Optional[str] = Form(None),
                                            salesman: Optional[str] = Form(None),
                                            car_brand: Optional[str] = Form(None),
                                            car_model: Optional[str] = Form(None),
                                            car_brand_logo: Optional[str] = Form(None),
                                            plate_number: Optional[str] = Form(None),
                                            code: Optional[str] = Form(None),
                                            color: Optional[str] = Form(None),
                                            mileage_in: Optional[str] = Form(None),
                                            engine_type: Optional[str] = Form(None),
                                            year: Optional[str] = Form(None),
                                            transmission_type: Optional[str] = Form(None),
                                            fuel_amount: Optional[str] = Form(None),
                                            vin: Optional[str] = Form(None),
                                            comment: Optional[str] = Form(None),
                                            left_front_wheel: Optional[str] = Form(None),
                                            right_front_wheel: Optional[str] = Form(None),
                                            left_rear_wheel: Optional[str] = Form(None),
                                            right_rear_wheel: Optional[str] = Form(None),
                                            interior_exterior: Optional[str] = Form(None),
                                            under_vehicle: Optional[str] = Form(None),
                                            under_hood: Optional[str] = Form(None),
                                            battery_performance: Optional[str] = Form(None),
                                            extra_checks: Optional[str] = Form(None),
                                            car_images: Optional[List[UploadFile]] = File(None),
                                            car_dialog: Optional[UploadFile] = File(None),
                                            customer_signature: Optional[UploadFile] = File(None),
                                            advisor_signature: Optional[UploadFile] = File(None),
                                            data: dict = Depends(security.get_current_user)):
    async with database.client.start_session() as session:
        try:
            await session.start_transaction()
            company_id = ObjectId(data.get("company_id"))
            new_job_counter = await create_custom_counter("JCN", "J", description='Inspection Reports Number',
                                                          data=data, session=session)
            print(type(left_front_wheel))

            job_card_section_dict = {
                "company_id": company_id,
                "job_date": job_date,
                "job_status_1": "Draft",
                "job_status_2": "Draft",
                "job_number": new_job_counter['final_counter'] if new_job_counter['success'] else None,
                "technician": ObjectId(technician) if technician else None,
                "customer": ObjectId(customer) if customer else None,
                "contact_name": customer_name,
                "contact_email": customer_email,
                "contact_number": customer_phone,
                "credit_limit": float(credit_limit) if credit_limit else 0,
                "salesman": ObjectId(salesman) if salesman else None,
                "car_brand": ObjectId(car_brand) if car_brand else None,
                "car_model": ObjectId(car_model) if car_model else None,
                "car_brand_logo": car_brand_logo,
                "color": ObjectId(color) if color else None,
                "plate_number": plate_number,
                "plate_code": code,
                "mileage_in": float(mileage_in) if mileage_in else 0,
                "engine_type": ObjectId(engine_type) if engine_type else None,
                "year": int(year) if year else None,
                "transmission_type": transmission_type,
                "fuel_amount": float(fuel_amount) if fuel_amount else 0,
                "vehicle_identification_number": vin,
                "createdAt": security.now_utc(),
                "updatedAt": security.now_utc(),
            }
            result = await job_cards_collection.insert_one(job_card_section_dict, session=session)
            if not result.inserted_id:
                raise HTTPException(status_code=500, detail="Failed to insert job card")
            image_urls = []
            if car_images:
                for image in car_images:
                    res = await upload_images.upload_image(image, 'job_cards_inspection_report')
                    image_urls.append({
                        "url": res["url"],
                        "image_public_id": res["public_id"],
                        "created_at": res["created_at"],
                    })

            cust_url = None
            adv_url = None
            car_dia_url = None
            if customer_signature:
                cust_url = await upload_images.upload_image(customer_signature, "inspection_report_signatures")
            if advisor_signature:
                adv_url = await upload_images.upload_image(advisor_signature, "inspection_report_signatures")
            if car_dialog:
                car_dia_url = await upload_images.upload_image(car_dialog, "inspection_report_cars_dialogs")
            customer_signature_url = cust_url['url'] if cust_url else None
            advisor_signature_url = adv_url['url'] if adv_url else None
            customer_signature_public_id = cust_url['public_id'] if cust_url else None
            advisor_signature_public_id = adv_url['public_id'] if adv_url else None
            car_dialog_url = car_dia_url['url'] if car_dia_url else None
            car_dialog_public_id = car_dia_url['public_id'] if car_dia_url else None

            inspection_report_section_dict = {
                "company_id": company_id,
                "job_card_id": result.inserted_id,
                "left_front_wheel": safe_json_load(left_front_wheel),
                "right_front_wheel": safe_json_load(right_front_wheel),
                "left_rear_wheel": safe_json_load(left_rear_wheel),
                "right_rear_wheel": safe_json_load(right_rear_wheel),
                "interior_exterior": safe_json_load(interior_exterior),
                "under_vehicle": safe_json_load(under_vehicle),
                "under_hood": safe_json_load(under_hood),
                "battery_performance": safe_json_load(battery_performance),
                "extra_checks": safe_json_load(extra_checks),
                "car_images": image_urls,
                "customer_signature": customer_signature_url,
                "customer_signature_public_id": customer_signature_public_id,
                "advisor_signature": advisor_signature_url,
                "advisor_signature_public_id": advisor_signature_public_id,
                "car_dialog": car_dialog_url,
                "car_dialog_public_id": car_dialog_public_id,
                "comment": comment,
                "createdAt": security.now_utc(),
                "updatedAt": security.now_utc(),
            }
            ins_result = await job_cards_inspection_reports_collection.insert_one(inspection_report_section_dict,
                                                                                  session=session)
            if not ins_result.inserted_id:
                raise HTTPException(status_code=500, detail="Failed to insert inspection report")
            await session.commit_transaction()

            res = await get_current_job_card_inspection_report_details(str(result.inserted_id))
            serialized = serializer(res['inspection_report'])
            await manager.broadcast({
                "type": "inspection_report_added",
                "data": serialized
            })
        except HTTPException as e:
            print(e)
            raise
        except Exception as e:
            print(e)
            await session.abort_transaction()
            raise HTTPException(status_code=500, detail=f"failed: {str(e)}")


@router.put("/update_job_from_inspection_report/{job_card_id}")
async def update_job_from_inspection_report(
        job_card_id: str,
        job_date: Optional[datetime] = Form(None),
        technician: Optional[str] = Form(None),
        customer: Optional[str] = Form(None),
        customer_name: Optional[str] = Form(None),
        customer_email: Optional[str] = Form(None),
        customer_phone: Optional[str] = Form(None),
        credit_limit: Optional[str] = Form(None),
        salesman: Optional[str] = Form(None),
        car_brand: Optional[str] = Form(None),
        car_model: Optional[str] = Form(None),
        car_brand_logo: Optional[str] = Form(None),
        plate_number: Optional[str] = Form(None),
        code: Optional[str] = Form(None),
        color: Optional[str] = Form(None),
        mileage_in: Optional[str] = Form(None),
        engine_type: Optional[str] = Form(None),
        year: Optional[str] = Form(None),
        transmission_type: Optional[str] = Form(None),
        fuel_amount: Optional[str] = Form(None),
        vin: Optional[str] = Form(None),
        comment: Optional[str] = Form(None),
        left_front_wheel: Optional[str] = Form(None),
        right_front_wheel: Optional[str] = Form(None),
        left_rear_wheel: Optional[str] = Form(None),
        right_rear_wheel: Optional[str] = Form(None),
        interior_exterior: Optional[str] = Form(None),
        under_vehicle: Optional[str] = Form(None),
        under_hood: Optional[str] = Form(None),
        battery_performance: Optional[str] = Form(None),
        extra_checks: Optional[str] = Form(None),
        new_images: Optional[List[UploadFile]] = File(None),
        kept_images: Optional[str] = Form("[]"),
        branch: Optional[str] = Form(None),
        _: dict = Depends(security.get_current_user)
):
    try:
        kept_images = json.loads(kept_images)
        job_card_id = ObjectId(job_card_id)

        # 🟦 1) جلب الـ job card الحالي
        job_card = await job_cards_collection.find_one({"_id": job_card_id})
        if not job_card:
            raise HTTPException(status_code=404, detail="Job card not found")

        # 🟦 2) جلب تقرير التفتيش الحالي (إذا وجد)
        try:
            report = await job_cards_inspection_reports_collection.find_one({"job_card_id": job_card_id})
        except Exception as e:
            # إذا حصل أي خطأ في الاتصال/سيرفر، ارفع الخطأ مباشرة ولا تنشئ تقرير جديد
            raise HTTPException(status_code=500, detail=f"Server error: {str(e)}")

        if not report:
            # لم نجد تقرير تفتيش → أنشئ واحد جديد
            new_report = {
                "company_id": job_card["company_id"],
                "job_card_id": job_card_id,
                "left_front_wheel": {},
                "right_front_wheel": {},
                "left_rear_wheel": {},
                "right_rear_wheel": {},
                "interior_exterior": {},
                "under_vehicle": {},
                "under_hood": {},
                "battery_performance": {},
                "extra_checks": {},
                "car_images": [],
                "comment": "",
                "createdAt": security.now_utc(),
                "updatedAt": security.now_utc()
            }
            ins_result = await job_cards_inspection_reports_collection.insert_one(new_report)
            inspection_id = ins_result.inserted_id
            report = new_report
        else:
            inspection_id = report["_id"]

        # 🟦 3) تحديث الـ Job Card
        job_updates = {
            "job_date": job_date if job_date else job_card.get("job_date"),
            "technician": ObjectId(technician) if technician else job_card.get("technician"),
            "customer": ObjectId(customer) if customer else job_card.get("customer"),
            "contact_name": customer_name if customer_name else job_card.get("contact_name"),
            "contact_email": customer_email if customer_email else job_card.get("contact_email"),
            "contact_number": customer_phone if customer_phone else job_card.get("contact_number"),
            "credit_limit": float(credit_limit) if credit_limit else job_card.get("credit_limit", 0),
            "salesman": ObjectId(salesman) if salesman else job_card.get("salesman"),
            "branch": ObjectId(branch) if branch else job_card.get("branch"),
            "car_brand": ObjectId(car_brand) if car_brand else job_card.get("car_brand"),
            "car_model": ObjectId(car_model) if car_model else job_card.get("car_model"),
            "car_brand_logo": car_brand_logo if car_brand_logo else job_card.get("car_brand_logo"),
            "color": ObjectId(color) if color else job_card.get("color"),
            "plate_number": plate_number if plate_number else job_card.get("plate_number"),
            "plate_code": code if code else job_card.get("plate_code"),
            "mileage_in": float(mileage_in) if mileage_in else job_card.get("mileage_in", 0),
            "engine_type": ObjectId(engine_type) if engine_type else job_card.get("engine_type"),
            "year": int(year) if year else job_card.get("year"),
            "transmission_type": transmission_type if transmission_type else job_card.get("transmission_type"),
            "fuel_amount": float(fuel_amount) if fuel_amount else job_card.get("fuel_amount", 0),
            "vehicle_identification_number": vin if vin else job_card.get("vehicle_identification_number"),
            "updatedAt": security.now_utc(),
        }
        await job_cards_collection.update_one({"_id": job_card_id}, {"$set": job_updates})

        # 🟦 4) تحديث الصور
        old_images = report.get("car_images", [])
        images_to_delete = [img for img in old_images if img["image_public_id"] not in kept_images]
        for img in images_to_delete:
            try:
                await upload_images.delete_image_from_server(img["image_public_id"])
            except Exception as e:
                print(f"Failed to delete image {img['image_public_id']}: {e}")
                pass

        new_uploaded_images = []
        if new_images:
            for img in new_images:
                res = await upload_images.upload_image(img, "job_cards_inspection_report")
                new_uploaded_images.append({
                    "url": res["url"],
                    "image_public_id": res["public_id"],
                    "created_at": res["created_at"]
                })
        updated_car_images = [img for img in old_images if img["image_public_id"] in kept_images] + new_uploaded_images

        # 🟦 5) تحديث الـ Inspection Report
        report_updates = {
            "left_front_wheel": safe_json_load(left_front_wheel) if left_front_wheel else report.get("left_front_wheel",
                                                                                                     {}),
            "right_front_wheel": safe_json_load(right_front_wheel) if right_front_wheel else report.get(
                "right_front_wheel", {}),
            "left_rear_wheel": safe_json_load(left_rear_wheel) if left_rear_wheel else report.get("left_rear_wheel",
                                                                                                  {}),
            "right_rear_wheel": safe_json_load(right_rear_wheel) if right_rear_wheel else report.get("right_rear_wheel",
                                                                                                     {}),
            "interior_exterior": safe_json_load(interior_exterior) if interior_exterior else report.get(
                "interior_exterior", {}),
            "under_vehicle": safe_json_load(under_vehicle) if under_vehicle else report.get("under_vehicle", {}),
            "under_hood": safe_json_load(under_hood) if under_hood else report.get("under_hood", {}),
            "battery_performance": safe_json_load(battery_performance) if battery_performance else report.get(
                "battery_performance", {}),
            "extra_checks": safe_json_load(extra_checks) if extra_checks else report.get("extra_checks", {}),
            "car_images": updated_car_images,
            "comment": comment if comment else report.get("comment", ""),
            "updatedAt": security.now_utc(),
        }

        await job_cards_inspection_reports_collection.update_one(
            {"_id": inspection_id},
            {"$set": report_updates}
        )
        res = await get_current_job_card_inspection_report_details(str(job_card_id))
        serialized = serializer(res['inspection_report'])
        await manager.broadcast({
            "type": "inspection_report_updated",
            "data": serialized
        })

        return {"success": True, "message": "Job & Inspection Report updated successfully"}

    except HTTPException:
        raise
    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")

import asyncio
import hashlib
import html
import secrets
from typing import Optional, List

from bson import ObjectId
from fastapi import APIRouter, HTTPException, Depends, UploadFile, File, Query
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from app.core import security
from app.core import google_mail
from app.database import get_collection
from datetime import datetime, timedelta, timezone
from app.widgets import upload_images

router = APIRouter()
companies_collection = get_collection("companies")
users_collection = get_collection("sys-users")
company_mail_settings_collection = get_collection("company_mail_settings")
company_mail_oauth_states_collection = get_collection("company_mail_oauth_states")


def _state_hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


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


class Variables(BaseModel):
    incentive_percentage: Optional[float] = None
    vat_percentage: Optional[float] = None
    tax_number: Optional[str] = None


class TermsAndConditionsBody(BaseModel):
    text: Optional[str] = None


async def _company_sender(company_id: ObjectId) -> tuple[dict, str]:
    company = await companies_collection.find_one(
        {"_id": company_id},
        {"company_name": 1, "email": 1, "owner_id": 1},
    )
    if not company:
        raise HTTPException(status_code=404, detail="Company not found")

    company_email = str(company.get("email") or "").strip().lower()
    if not company_email and company.get("owner_id"):
        owner = await users_collection.find_one(
            {"_id": company["owner_id"]},
            {"email": 1},
        )
        company_email = str((owner or {}).get("email") or "").strip().lower()
    if not company_email:
        raise HTTPException(
            status_code=400,
            detail="The company owner does not have an email address",
        )
    return company, company_email


async def _require_mail_admin(data: dict) -> None:
    company_id = ObjectId(data.get("company_id"))
    user_id = ObjectId(data.get("sub"))
    company = await companies_collection.find_one(
        {"_id": company_id},
        {"owner_id": 1},
    )
    if company and company.get("owner_id") == user_id:
        return
    user = await users_collection.find_one(
        {"_id": user_id, "company_id": company_id},
        {"is_admin": 1},
    )
    if not user or user.get("is_admin") is not True:
        raise HTTPException(
            status_code=403,
            detail="Only the company owner or an administrator can connect Google Mail",
        )


def _oauth_result_page(success: bool, message: str) -> HTMLResponse:
    color = "#16864b" if success else "#c0392b"
    title = "Google Mail connected" if success else "Google Mail connection failed"
    safe_message = html.escape(message)
    status = "connected" if success else "failed"
    content = f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{title}</title>
</head>
<body style="margin:0;background:#f5f7fa;font-family:Arial,sans-serif">
  <main style="max-width:520px;margin:80px auto;padding:32px;background:white;
               border-radius:14px;box-shadow:0 10px 35px rgba(0,0,0,.12);
               text-align:center">
    <div style="font-size:42px;color:{color}">{'✓' if success else '!'}</div>
    <h2 style="color:{color};margin:12px 0">{title}</h2>
    <p style="color:#53606d;line-height:1.5">{safe_message}</p>
    <p style="color:#8a96a3;font-size:13px">You can close this window.</p>
  </main>
  <script>
    if (window.opener) {{
      window.opener.postMessage({{
        type: 'datahub-google-mail',
        status: '{status}'
      }}, '*');
      setTimeout(function() {{ window.close(); }}, 1200);
    }}
  </script>
</body>
</html>"""
    return HTMLResponse(content=content, status_code=200 if success else 400)


@router.get("/get_company_variables_and_details")
async def get_company_variables_and_details(data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        pipeline = [
            {
                '$match': {
                    '_id': company_id
                }
            }, {
                '$lookup': {
                    'from': 'all_lists_values',
                    'let': {
                        'industry_id': '$industry'
                    },
                    'pipeline': [
                        {
                            '$match': {
                                '$expr': {
                                    '$eq': [
                                        '$_id', '$$industry_id'
                                    ]
                                }
                            }
                        }, {
                            '$project': {
                                'name': 1
                            }
                        }
                    ],
                    'as': 'industry_details'
                }
            }, {
                '$unwind': {
                    'path': '$industry_details',
                    'preserveNullAndEmptyArrays': True
                }
            }, {
                '$lookup': {
                    'from': 'sys-users',
                    'localField': 'owner_id',
                    'foreignField': '_id',
                    'as': 'owner_details'
                }
            }, {
                '$unwind': {
                    'path': '$owner_details',
                    'preserveNullAndEmptyArrays': True
                }
            }, {
                '$lookup': {
                    'from': 'all_countries',
                    'let': {
                        'country_id': '$owner_details.country'
                    },
                    'pipeline': [
                        {
                            '$match': {
                                '$expr': {
                                    '$eq': [
                                        '$_id', '$$country_id'
                                    ]
                                }
                            }
                        }, {
                            '$project': {
                                'name': 1
                            }
                        }
                    ],
                    'as': 'country_details'
                }
            }, {
                '$unwind': {
                    'path': '$country_details',
                    'preserveNullAndEmptyArrays': True
                }
            }, {
                '$lookup': {
                    'from': 'all_countries_cities',
                    'let': {
                        'city_id': '$owner_details.city'
                    },
                    'pipeline': [
                        {
                            '$match': {
                                '$expr': {
                                    '$eq': [
                                        '$_id', '$$city_id'
                                    ]
                                }
                            }
                        }, {
                            '$project': {
                                'name': 1
                            }
                        }
                    ],
                    'as': 'city_details'
                }
            }, {
                '$unwind': {
                    'path': '$city_details',
                    'preserveNullAndEmptyArrays': True
                }
            }, {
                '$lookup': {
                    'from': 'sys-roles',
                    'let': {
                        'roles_ids': '$owner_details.roles'
                    },
                    'pipeline': [
                        {
                            '$match': {
                                '$expr': {
                                    '$in': [
                                        '$_id', '$$roles_ids'
                                    ]
                                }
                            }
                        }, {
                            '$project': {
                                'role_name': 1
                            }
                        }
                    ],
                    'as': 'roles_details'
                }
            }, {
                '$addFields': {
                    'industry_name': {
                        '$ifNull': [
                            '$industry_details.name', None
                        ]
                    },
                    'country_name': {
                        '$ifNull': [
                            '$country_details.name', None
                        ]
                    },
                    'city_name': {
                        '$ifNull': [
                            '$city_details.name', None
                        ]
                    },
                    'owner_name': {
                        '$ifNull': [
                            '$owner_details.user_name', None
                        ]
                    },
                    'owner_email': {
                        '$ifNull': [
                            '$owner_details.email', None
                        ]
                    },
                    'owner_phone': {
                        '$ifNull': [
                            '$owner_details.phone_number', None
                        ]
                    },
                    'owner_address': {
                        '$ifNull': [
                            '$owner_details.address', None
                        ]
                    },
                    'incentive_percentage': {
                        '$ifNull': [
                            '$incentive_percentage', None
                        ]
                    },
                    'vat_percentage': {
                        '$ifNull': [
                            '$vat_percentage', None
                        ]
                    },
                    'tax_number': {
                        '$ifNull': [
                            '$tax_number', None
                        ]
                    }
                }
            }, {
                '$project': {
                    'industry_details': 0,
                    'country_details': 0,
                    'city_details': 0,
                    'owner_details': 0
                }
            }
        ]
        cursor = await companies_collection.aggregate(pipeline)
        result = await cursor.next()
        serialized = serializer(result)
        return {"company_variables": serialized}

    except HTTPException:
        raise
    except Exception as error:
        print(error)
        raise HTTPException(status_code=500, detail=str(error))


@router.get("/google_mail/status")
async def google_mail_status(data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        _, company_email = await _company_sender(company_id)
        connection = await company_mail_settings_collection.find_one(
            {"company_id": company_id, "provider": "google"},
            {"email": 1, "connected_at": 1, "updatedAt": 1},
        )
        return {
            "connected": bool(connection),
            "email": str((connection or {}).get("email") or ""),
            "company_email": company_email,
            "connected_at": (connection or {}).get("connected_at"),
        }
    except HTTPException:
        raise
    except Exception as error:
        raise HTTPException(status_code=500, detail=str(error))


@router.post("/google_mail/connect")
async def connect_google_mail(data: dict = Depends(security.get_current_user)):
    try:
        await _require_mail_admin(data)
        company_id = ObjectId(data.get("company_id"))
        user_id = ObjectId(data.get("sub"))
        await _company_sender(company_id)

        state = secrets.token_urlsafe(40)
        now = datetime.now(timezone.utc)
        await company_mail_oauth_states_collection.insert_one({
            "state_hash": _state_hash(state),
            "company_id": company_id,
            "user_id": user_id,
            "expiresAt": now + timedelta(minutes=10),
            "createdAt": now,
        })
        return {
            "authorization_url": google_mail.authorization_url(state),
        }
    except google_mail.GoogleMailError as error:
        raise HTTPException(status_code=error.status_code, detail=error.message)
    except HTTPException:
        raise
    except Exception as error:
        raise HTTPException(status_code=500, detail=str(error))


@router.get("/google_mail/callback", response_class=HTMLResponse)
async def google_mail_callback(
        state: str = Query(...),
        code: Optional[str] = Query(None),
        error: Optional[str] = Query(None),
):
    if error:
        return _oauth_result_page(False, f"Google authorization was cancelled: {error}")
    if not code:
        return _oauth_result_page(False, "Google did not return an authorization code.")

    now = datetime.now(timezone.utc)
    state_document = await company_mail_oauth_states_collection.find_one_and_delete({
        "state_hash": _state_hash(state),
        "expiresAt": {"$gt": now},
    })
    if not state_document:
        return _oauth_result_page(
            False,
            "This connection request is invalid or expired. Please start again.",
        )

    try:
        token_response = await asyncio.to_thread(
            google_mail.exchange_authorization_code,
            code,
        )
        access_token = str(token_response.get("access_token") or "")
        refresh_token = str(token_response.get("refresh_token") or "")
        if not access_token or not refresh_token:
            raise google_mail.GoogleMailError(
                "Google did not return offline access. Please reconnect and approve access."
            )

        connected_email = await asyncio.to_thread(
            google_mail.connected_google_email,
            access_token,
        )
        company_id = state_document["company_id"]
        _, company_email = await _company_sender(company_id)
        if connected_email.lower() != company_email.lower():
            raise google_mail.GoogleMailError(
                f"Please connect the company email {company_email}, "
                f"not {connected_email}.",
                400,
            )

        encrypted_refresh_token = google_mail.encrypt_refresh_token(refresh_token)
        current_time = security.now_utc()
        await company_mail_settings_collection.update_one(
            {"company_id": company_id, "provider": "google"},
            {
                "$set": {
                    "email": connected_email,
                    "encrypted_refresh_token": encrypted_refresh_token,
                    "scope": str(token_response.get("scope") or ""),
                    "connected_by": state_document["user_id"],
                    "connected_at": current_time,
                    "updatedAt": current_time,
                },
                "$setOnInsert": {
                    "company_id": company_id,
                    "provider": "google",
                    "createdAt": current_time,
                },
            },
            upsert=True,
        )
        return _oauth_result_page(
            True,
            f"{connected_email} is ready to send company payslips.",
        )
    except google_mail.GoogleMailError as oauth_error:
        return _oauth_result_page(False, oauth_error.message)
    except Exception:
        return _oauth_result_page(
            False,
            "Could not complete the Google Mail connection. Please try again.",
        )


@router.delete("/google_mail/disconnect")
async def disconnect_google_mail(data: dict = Depends(security.get_current_user)):
    try:
        await _require_mail_admin(data)
        company_id = ObjectId(data.get("company_id"))
        connection = await company_mail_settings_collection.find_one(
            {"company_id": company_id, "provider": "google"},
            {"encrypted_refresh_token": 1},
        )
        if not connection:
            return {"message": "Google Mail is already disconnected"}

        try:
            refresh_token = google_mail.decrypt_refresh_token(
                connection["encrypted_refresh_token"]
            )
            await asyncio.to_thread(google_mail.revoke_refresh_token, refresh_token)
        except google_mail.GoogleMailError:
            pass

        await company_mail_settings_collection.delete_one({
            "company_id": company_id,
            "provider": "google",
        })
        return {"message": "Google Mail disconnected"}
    except HTTPException:
        raise
    except Exception as error:
        raise HTTPException(status_code=500, detail=str(error))


@router.patch("/update_company_variables")
async def update_company_variables(
        var: Variables,
        data: dict = Depends(security.get_current_user)
):
    try:
        company_id = data.get("company_id")
        var = var.model_dump(exclude_unset=True)
        if not company_id:
            raise HTTPException(status_code=400, detail="Missing company_id in user data")

        result = await companies_collection.update_one(
            {"_id": ObjectId(company_id)},
            {"$set": {"vat_percentage": var['vat_percentage'], "tax_number": var['tax_number'],
                      "incentive_percentage": var['incentive_percentage']}},
        )

        if result.matched_count == 0:
            raise HTTPException(status_code=404, detail="Company not found")

        return {"status": "success", "modified_count": result.modified_count}

    except HTTPException:
        raise
    except Exception as error:
        raise HTTPException(status_code=500, detail=str(error))


@router.patch("/update_inspection_report")
async def update_inspection_report(inspection_report: Optional[List[str]] = None,
                                   data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        inspection_report = inspection_report or []
        result = await companies_collection.update_one({"_id": company_id}, {
            "$set": {"inspection_report": inspection_report, "updatedAt": security.now_utc()}})

        if result.matched_count == 0:
            raise HTTPException(status_code=404, detail="Company not found")

    except HTTPException:
        raise
    except Exception as error:
        raise HTTPException(status_code=500, detail=str(error))


@router.patch("/upload_terms_and_conditions/{language_code}")
async def upload_terms_and_conditions(language_code: str, body: TermsAndConditionsBody,
                                      data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        if language_code == "en":
            await companies_collection.update_one({"_id": company_id}, {
                "$set": {"terms_and_conditions_en": body.text, "updatedAt": security.now_utc()}
            })
        elif language_code == "ar":
            await companies_collection.update_one({"_id": company_id}, {
                "$set": {"terms_and_conditions_ar": body.text, "updatedAt": security.now_utc()}
            })

    except HTTPException:
        raise
    except Exception as error:
        raise HTTPException(status_code=500, detail=str(error))


@router.patch("/upload_header_footer/{image_type}")
async def upload_terms_and_conditions(image_type: str, image: UploadFile = File(None),
                                      data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        if image:
            result = await upload_images.upload_image(image, 'companies_header_footer')
            image_url = result["url"]
            image_public_id = result["public_id"]
        else:
            image_url = None
            image_public_id = None
        if image_type == "footer":
            await companies_collection.update_one({"_id": company_id}, {
                "$set": {"footer_url": image_url, "footer_public_id": image_public_id, "updatedAt": security.now_utc()}
            })
        elif image_type == "header":
            await companies_collection.update_one({"_id": company_id}, {
                "$set": {"header_url": image_url, "header_public_id": image_public_id, "updatedAt": security.now_utc()}
            })

    except HTTPException:
        raise
    except Exception as error:
        raise HTTPException(status_code=500, detail=str(error))

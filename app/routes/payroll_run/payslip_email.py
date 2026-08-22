import asyncio
import base64
import html
import os
import re
from email.message import EmailMessage
from email.utils import formataddr
from typing import Any, Optional

from bson import ObjectId
from bson.errors import InvalidId
from fastapi import APIRouter, Depends, File, HTTPException, UploadFile
from pydantic import EmailStr, TypeAdapter, ValidationError

from app.core import google_mail, security
from .collections import (
    companies_collection,
    company_mail_settings_collection,
    employees_collection,
    employees_email_collection,
    payroll_period_details_collection,
    payroll_runs_collection,
    payroll_runs_employees_collection,
    users_collection,
)

router = APIRouter()


MAX_PAYSLIP_PDF_SIZE = 5 * 1024 * 1024


email_address_adapter = TypeAdapter(EmailStr)


def _valid_email(value: Any) -> Optional[str]:
    if not value:
        return None
    try:
        return str(email_address_adapter.validate_python(str(value).strip()))
    except ValidationError:
        return None


def _safe_file_name(value: Any) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._ -]+", "", str(value or "")).strip(" .")
    return cleaned[:80] or "Employee"


def _send_payslip_messages(
        access_token: str,
        company_name: str,
        company_email: str,
        run_label: str,
        period_name: str,
        messages: list[dict],
) -> list[dict]:
    results = []
    safe_company_name = " ".join(company_name.splitlines()).strip() or "Company"
    safe_period_name = " ".join((period_name or run_label).splitlines()).strip()
    for item in messages:
        employee_name = str(item["employee_name"])
        message = EmailMessage()
        message["From"] = formataddr((safe_company_name, company_email))
        message["To"] = item["email"]
        message["Reply-To"] = company_email
        message["Subject"] = f"Payslip - {safe_period_name}"
        message.set_content(
            f"Dear {employee_name},\n\n"
            f"Please find attached your payslip for {safe_period_name}.\n\n"
            f"Regards,\n{safe_company_name}"
        )
        message.add_alternative(
            f"<p>Dear {html.escape(employee_name)},</p>"
            f"<p>Please find attached your payslip for "
            f"<strong>{html.escape(safe_period_name)}</strong>.</p>"
            f"<p>Regards,<br>{html.escape(safe_company_name)}</p>",
            subtype="html",
        )
        message.add_attachment(
            item["pdf"],
            maintype="application",
            subtype="pdf",
            filename=f"Payslip - {_safe_file_name(employee_name)}.pdf",
        )
        raw_message = base64.urlsafe_b64encode(
            message.as_bytes()
        ).decode("ascii").rstrip("=")
        try:
            google_mail.send_raw_message(access_token, raw_message)
            results.append({**item["result"], "status": "sent"})
        except google_mail.GoogleMailError as error:
            results.append({
                **item["result"],
                "status": "failed",
                "reason": error.message[:180],
            })

    return results


@router.post("/email_payslips/{run_id}")
async def email_payslips(
        run_id: str,
        payslips: list[UploadFile] = File(...),
        data: dict = Depends(security.get_current_user),
):
    try:
        company_id = ObjectId(data.get("company_id"))
        run_object_id = ObjectId(run_id)
    except (InvalidId, TypeError):
        raise HTTPException(status_code=400, detail="Invalid payroll run or company id")

    payroll_run = await payroll_runs_collection.find_one(
        {"_id": run_object_id, "company_id": company_id},
        {"run_number": 1, "period_id": 1},
    )
    if not payroll_run:
        raise HTTPException(status_code=404, detail="Payroll run not found")
    if not payslips:
        raise HTTPException(status_code=400, detail="No payslip PDFs were uploaded")

    uploaded_payslips = {}
    try:
        for payslip in payslips:
            file_employee_id = os.path.splitext(payslip.filename or "")[0]
            try:
                employee_id = ObjectId(file_employee_id)
            except InvalidId:
                raise HTTPException(
                    status_code=400,
                    detail="Every payslip filename must contain its employee id",
                )
            if employee_id in uploaded_payslips:
                raise HTTPException(status_code=400, detail="Duplicate employee payslip")

            pdf = await payslip.read(MAX_PAYSLIP_PDF_SIZE + 1)
            if len(pdf) > MAX_PAYSLIP_PDF_SIZE:
                raise HTTPException(
                    status_code=413,
                    detail=f"Payslip for employee {employee_id} is larger than 5 MB",
                )
            if not pdf.startswith(b"%PDF-"):
                raise HTTPException(status_code=400, detail="Invalid payslip PDF")
            uploaded_payslips[employee_id] = pdf
    finally:
        for payslip in payslips:
            await payslip.close()

    employee_ids = list(uploaded_payslips)
    run_employees = await payroll_runs_employees_collection.find(
        {
            "company_id": company_id,
            "run_id": run_object_id,
            "employee_id": {"$in": employee_ids},
        },
        {"employee_id": 1},
    ).to_list(None)
    allowed_employee_ids = {item["employee_id"] for item in run_employees}
    if allowed_employee_ids != set(employee_ids):
        raise HTTPException(
            status_code=400,
            detail="One or more employees do not belong to this payroll run",
        )

    employee_documents = await employees_collection.find(
        {
            "company_id": company_id,
            "_id": {"$in": employee_ids},
        },
        {"full_name": 1, "people_counter": 1, "email": 1},
    ).to_list(None)
    employees_by_id = {item["_id"]: item for item in employee_documents}
    if len(employees_by_id) != len(employee_ids):
        raise HTTPException(status_code=400, detail="One or more employees were not found")

    email_documents = await employees_email_collection.find(
        {
            "company_id": company_id,
            "employee_id": {"$in": employee_ids},
            "email": {"$nin": [None, ""]},
            "use_for_payslips": True,
        },
        {"employee_id": 1, "email": 1, "updatedAt": 1},
    ).sort("updatedAt", -1).to_list(None)

    selected_email_employee_ids = set()
    emails_by_employee = {}
    for email_document in email_documents:
        employee_id = email_document.get("employee_id")
        selected_email_employee_ids.add(employee_id)
        if employee_id not in emails_by_employee:
            valid_email = _valid_email(email_document.get("email"))
            if valid_email:
                emails_by_employee[employee_id] = valid_email

    period_name = ""
    if payroll_run.get("period_id"):
        period = await payroll_period_details_collection.find_one(
            {"_id": payroll_run["period_id"], "company_id": company_id},
            {"period_name": 1},
        )
        period_name = str((period or {}).get("period_name") or "").strip()

    company = await companies_collection.find_one(
        {"_id": company_id},
        {"company_name": 1, "email": 1, "owner_id": 1},
    )
    company_name = str((company or {}).get("company_name") or "Company").strip()
    company_email = _valid_email((company or {}).get("email"))
    if not company_email and (company or {}).get("owner_id"):
        company_owner = await users_collection.find_one(
            {"_id": company["owner_id"]},
            {"email": 1},
        )
        company_email = _valid_email((company_owner or {}).get("email"))

    if not company_email:
        raise HTTPException(
            status_code=400,
            detail="The current company does not have a valid sender email",
        )

    run_label = str(payroll_run.get("run_number") or "Payroll run").strip()

    messages = []
    results = []
    for employee_id, pdf in uploaded_payslips.items():
        employee = employees_by_id[employee_id]
        employee_name = str(
            employee.get("full_name")
            or employee.get("people_counter")
            or "Employee"
        ).strip()
        result = {
            "employee_id": str(employee_id),
            "employee_name": employee_name,
        }
        if employee_id not in selected_email_employee_ids:
            results.append({
                **result,
                "status": "skipped",
                "reason": "No payslip email selected",
            })
            continue
        if employee_id not in emails_by_employee:
            results.append({
                **result,
                "status": "skipped",
                "reason": "Selected payslip email is invalid",
            })
            continue
        messages.append({
            "email": emails_by_employee[employee_id],
            "employee_name": employee_name,
            "pdf": pdf,
            "result": result,
        })

    if messages:
        connection = await company_mail_settings_collection.find_one(
            {"company_id": company_id, "provider": "google"},
            {"email": 1, "encrypted_refresh_token": 1},
        )
        if not connection:
            raise HTTPException(
                status_code=409,
                detail="Connect Google Mail in Company Variables before sending payslips",
            )
        if str(connection.get("email") or "").lower() != company_email.lower():
            raise HTTPException(
                status_code=409,
                detail="The company email changed. Reconnect Google Mail in Company Variables",
            )

        try:
            refresh_token = google_mail.decrypt_refresh_token(
                connection["encrypted_refresh_token"]
            )
            access_token = await asyncio.to_thread(
                google_mail.refresh_access_token,
                refresh_token,
            )
            results.extend(await asyncio.to_thread(
                _send_payslip_messages,
                access_token,
                company_name,
                company_email,
                run_label,
                period_name,
                messages,
            ))
        except google_mail.GoogleMailError as error:
            raise HTTPException(
                status_code=error.status_code,
                detail=error.message,
            )

    sent_employee_ids = [
        ObjectId(item["employee_id"])
        for item in results
        if item["status"] == "sent"
    ]
    if sent_employee_ids:
        await payroll_runs_employees_collection.update_many(
            {
                "company_id": company_id,
                "run_id": run_object_id,
                "employee_id": {"$in": sent_employee_ids},
            },
            {"$set": {
                "payslip_emailed_at": security.now_utc(),
                "updatedAt": security.now_utc(),
            }},
        )

    sent = sum(item["status"] == "sent" for item in results)
    skipped = sum(item["status"] == "skipped" for item in results)
    failed = sum(item["status"] == "failed" for item in results)
    return {
        "message": "Payslip email merge completed",
        "sent": sent,
        "skipped": skipped,
        "failed": failed,
        "results": results,
    }

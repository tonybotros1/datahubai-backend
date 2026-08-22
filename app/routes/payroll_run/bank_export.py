from bson import ObjectId
from bson.errors import InvalidId
from fastapi import APIRouter, Depends, HTTPException

from app.core import security
from app.routes.counters import create_custom_counter
from .collections import payroll_runs_collection
from .queries import get_payroll_runs_details

router = APIRouter()


@router.patch("/prepare_bank_export/{run_id}")
async def prepare_bank_export(run_id: str, data: dict = Depends(security.get_current_user)):
    try:
        company_id = ObjectId(data.get("company_id"))
        run_object_id = ObjectId(run_id)
        payroll_run_document = await payroll_runs_collection.find_one({
            "_id": run_object_id,
            "company_id": company_id
        })

        if not payroll_run_document:
            raise HTTPException(status_code=404, detail="Payroll run not found")

        payment_number = payroll_run_document.get("payment_number") or ""
        if not payment_number:
            new_payment_counter = await create_custom_counter("PPN", "PP", description="Payroll Payment Number",
                                                              data=data)
            payment_number = new_payment_counter["final_counter"] if new_payment_counter["success"] else ""

        await payroll_runs_collection.update_one(
            {"_id": run_object_id, "company_id": company_id},
            {"$set": {
                "payment_number": payment_number,
                "bank_exported_at": security.now_utc(),
                "updatedAt": security.now_utc(),
            }}
        )

        details = await get_payroll_runs_details(run_id, data)
        return {"payroll_runs_details": details["payroll_runs_details"]}

    except InvalidId:
        raise HTTPException(status_code=400, detail="Invalid payroll run id")
    except HTTPException:
        raise
    except Exception:
        raise

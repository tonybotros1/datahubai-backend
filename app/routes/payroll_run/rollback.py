from bson import ObjectId
from bson.errors import InvalidId
from fastapi import APIRouter, Depends, HTTPException

from app import database
from app.core import security
from .collections import (
    payroll_runs_collection,
    payroll_runs_employees_collection,
    payroll_runs_employees_elements_collection,
)

router = APIRouter()


@router.delete("/rollback_payroll_run/{run_id}")
async def rollback_payroll_run(run_id: str, _: dict = Depends(security.get_current_user)):
    try:
        run_id = ObjectId(run_id)
    except InvalidId:
        raise HTTPException(status_code=400, detail="Invalid run_id")

    async with database.client.start_session() as session:
        try:
            await session.start_transaction()

            # Delete children first
            await payroll_runs_employees_elements_collection.delete_many(
                {"run_id": run_id}, session=session
            )
            await payroll_runs_employees_collection.delete_many(
                {"run_id": run_id}, session=session
            )

            # Delete main run
            result = await payroll_runs_collection.delete_one(
                {"_id": run_id}, session=session
            )

            if result.deleted_count == 0:
                await session.abort_transaction()
                raise HTTPException(status_code=404, detail="Payroll run not found")

            await session.commit_transaction()

            return {"message": "Payroll run rolled back successfully"}

        except Exception as e:
            await session.abort_transaction()
            raise HTTPException(status_code=500, detail=str(e))

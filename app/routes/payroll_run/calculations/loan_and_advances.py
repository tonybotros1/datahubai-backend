from typing import Optional

from bson import ObjectId

from ..collections import payroll_runs_employees_elements_collection


async def py_loan_and_advances_ff(loan_id: ObjectId, total_amount: float, monthly_installment: float,
                                  paid_to_date: Optional[float] = None):
    try:
        if paid_to_date is None:
            paid_cursor = await payroll_runs_employees_elements_collection.aggregate([
                {
                    "$match": {
                        "element_id": loan_id,
                    }
                },
                {
                    "$group": {
                        "_id": None,
                        "paid_to_date": {
                            "$sum": {
                                "$ifNull": [
                                    "$value", 0
                                ]
                            }
                        }
                    }
                }
            ])
            paid_result = await paid_cursor.to_list(1)
            paid_to_date = paid_result[0]["paid_to_date"] if paid_result else 0

        remaining_amount = max((total_amount or 0) - paid_to_date, 0)
        return round(min(monthly_installment or 0, remaining_amount), 2)

    except Exception as e:
        raise e

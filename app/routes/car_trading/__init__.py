from fastapi import APIRouter

from . import (
    bank_accounts,
    capitals_outstanding,
    car_trading as car_information,
    dashboard_summary,
    general_expenses,
    last_changes,
    migrations,
    sales_agreements,
    vehicle_analysis,
)
from .models import (
    CapitalModel,
    CarTradingItemsModel,
    CarTradingModel,
    CarTradingSearch,
    ExpensesSearchModel,
    GeneralExpensesModel,
    LastChangesFilter,
    PurchaseAgreementModel,
    PyObjectId,
    TransferModel,
)

router = APIRouter()
router.include_router(car_information.router)
router.include_router(dashboard_summary.router)
router.include_router(vehicle_analysis.router)
router.include_router(bank_accounts.router)
router.include_router(sales_agreements.router)
router.include_router(capitals_outstanding.router)
router.include_router(general_expenses.router)
router.include_router(last_changes.router)
router.include_router(migrations.router)

__all__ = [
    "router",
    "PyObjectId",
    "CapitalModel",
    "GeneralExpensesModel",
    "CarTradingItemsModel",
    "CarTradingSearch",
    "CarTradingModel",
    "LastChangesFilter",
    "PurchaseAgreementModel",
    "TransferModel",
    "ExpensesSearchModel",
]

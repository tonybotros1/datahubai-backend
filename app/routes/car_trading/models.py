from datetime import datetime
from typing import Optional

from bson import ObjectId
from pydantic import BaseModel
from pydantic_core import core_schema


class CapitalModel(BaseModel):
    name: Optional[str] = None
    pay: Optional[float] = None
    account_name: Optional[str] = None
    receive: Optional[float] = None
    comment: Optional[str] = None
    date: Optional[datetime] = None


class GeneralExpensesModel(BaseModel):
    item: Optional[str] = None
    pay: Optional[float] = None
    receive: Optional[float] = None
    account_name: Optional[str] = None
    comment: Optional[str] = None
    trade_id: Optional[str] = None
    date: Optional[datetime] = None


class PyObjectId(ObjectId):
    @classmethod
    def __get_pydantic_core_schema__(cls, source, handler):
        return core_schema.no_info_after_validator_function(
            cls.validate,
            core_schema.str_schema()
        )

    @classmethod
    def validate(cls, v):
        if v in (None, ""):  # allow None or empty string
            return None
        if isinstance(v, ObjectId):
            return v
        if not ObjectId.is_valid(v):
            raise ValueError("Invalid ObjectId")
        return ObjectId(v)


class CarTradingItemsModel(BaseModel):
    item: Optional[str] = None
    trade_id: Optional[str] = None
    pay: Optional[float] = None
    receive: Optional[float] = None
    account_name: Optional[str] = None
    comment: Optional[str] = None
    date: Optional[datetime] = None

    model_config = {
        "arbitrary_types_allowed": True
    }


class CarTradingSearch(BaseModel):
    trade_id: Optional[PyObjectId] = None
    car_brand: Optional[PyObjectId] = None
    car_model: Optional[PyObjectId] = None
    specification: Optional[PyObjectId] = None
    engine_size: Optional[PyObjectId] = None
    bought_from: Optional[PyObjectId] = None
    sold_to: Optional[PyObjectId] = None
    sold_by: Optional[PyObjectId] = None
    bought_by: Optional[PyObjectId] = None
    invested_by: Optional[PyObjectId] = None
    consignment_for: Optional[PyObjectId] = None
    status: Optional[str] = None
    from_date: Optional[datetime] = None
    to_date: Optional[datetime] = None
    all: Optional[bool] = False
    today: Optional[bool] = False
    this_month: Optional[bool] = False
    this_year: Optional[bool] = False


class ExpensesSearchModel(BaseModel):
    from_date: Optional[datetime] = None
    to_date: Optional[datetime] = None
    all: Optional[bool] = False
    today: Optional[bool] = False
    this_month: Optional[bool] = False
    this_year: Optional[bool] = False


class CarTradingModel(BaseModel):
    date: Optional[datetime] = None
    warranty_end_date: Optional[datetime] = None
    service_contract_end_date: Optional[datetime] = None
    mileage: Optional[float] = None
    color_out: Optional[PyObjectId] = None
    color_in: Optional[PyObjectId] = None
    car_brand: Optional[PyObjectId] = None
    car_model: Optional[PyObjectId] = None
    trim: Optional[str] = None
    specification: Optional[PyObjectId] = None
    engine_size: Optional[PyObjectId] = None
    year: Optional[PyObjectId] = None
    vin: Optional[str] = None
    bought_from: Optional[PyObjectId] = None
    sold_to: Optional[PyObjectId] = None
    note: Optional[str] = None
    status: Optional[str] = None
    bought_by: Optional[PyObjectId] = None
    sold_by: Optional[PyObjectId] = None
    invested_by: Optional[PyObjectId] = None
    consignment_for: Optional[PyObjectId] = None
    # items: Optional[List[CarTradingItemsModel]] = None

    model_config = {
        "arbitrary_types_allowed": True
    }


class LastChangesFilter(BaseModel):
    min_amount: Optional[float] = None
    max_amount: Optional[float] = None
    account_name: Optional[PyObjectId] = None
    from_date: Optional[datetime] = None
    to_date: Optional[datetime] = None
    account: Optional[str] = None


class PurchaseAgreementModel(BaseModel):
    trade_id: Optional[str] = None
    agreement_date: Optional[datetime] = None
    agreement_note: Optional[str] = None
    buyer_name: Optional[str] = None
    buyer_ID: Optional[str] = None
    buyer_phone: Optional[str] = None
    buyer_email: Optional[str] = None
    seller_name: Optional[str] = None
    seller_ID: Optional[str] = None
    seller_phone: Optional[str] = None
    seller_email: Optional[str] = None
    note: Optional[str] = None
    agreement_amount: Optional[float] = None
    agreement_down_payment: Optional[float] = None


class TransferModel(BaseModel):
    date: Optional[datetime] = None
    from_account: Optional[str] = None
    to_account: Optional[str] = None
    amount: Optional[float] = None
    comment: Optional[str] = None

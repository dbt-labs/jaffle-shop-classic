import datetime as dt

from pydantic import BaseModel


class MonthlyOrders(BaseModel):
    month: dt.date
    amount: int

    class Config:
        orm_mode = True


class CustomerMonthlyOrders(BaseModel):
    customer_id: int
    first_name: str
    last_name: str
    month: dt.date
    amount: int

    class Config:
        orm_mode = True

import datetime as dt
from typing import Optional, List, Union

import dateutil.parser
import uvicorn
from fastapi import FastAPI

from postgres.crud import get_monthly_orders
from postgres import models, schemas
from postgres.database import engine, get_db

models.Base.metadata.create_all(bind=engine)

app = FastAPI()


def get_date_from_str(value: Optional[str]) -> Optional[dt.date]:
    try:
        return dateutil.parser.parse(value)
    except (dateutil.parser.ParserError, TypeError):
        return None


@app.get("/monthly_orders", response_model=List[Union[schemas.CustomerMonthlyOrders, schemas.MonthlyOrders]])
async def monthly_orders(start_date: Optional[str] = None, end_date: Optional[str] = None, customer_id: Optional[str] = None):
    """
    API to fetch all monthly orders.

    :param start_date: If defined, only return order data after this date
    :param end_date: If defined, only return order data before this date
    :param customer_id: If defined, will filter the result on a specific customer ID
    :return: A list of JSONs representing the filtered monthly orders
    """
    start_date = get_date_from_str(start_date)
    end_date = get_date_from_str(end_date)

    with get_db() as db:
        results = get_monthly_orders(db=db, customer_id=customer_id, start_date=start_date, end_date=end_date)

    return results.all()


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=5000)

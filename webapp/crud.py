import datetime as dt
from typing import Optional

from sqlalchemy.orm import Session, Query

from postgres.models import MonthlyOrders, CustomerMonthlyOrders


def get_monthly_orders(db: Session, customer_id: Optional[int] = None, start_date: Optional[dt.datetime] = None,
                       end_date: Optional[dt.datetime] = None) -> Query:
    """Return all monthly orders, filtered by the given parameters"""
    if customer_id:
        query = db.query(CustomerMonthlyOrders).filter(CustomerMonthlyOrders.customer_id == customer_id)
        return filter_results_by_dates(query, CustomerMonthlyOrders.month, start_date, end_date)

    query = db.query(MonthlyOrders)
    return filter_results_by_dates(query, MonthlyOrders.month, start_date, end_date)


def filter_results_by_dates(query: Query, month_field, start_date: Optional[dt.datetime] = None, end_date: Optional[dt.datetime] = None):
    if start_date:
        query = query.filter(month_field >= str(start_date))
    if end_date:
        query = query.filter(month_field <= str(end_date))

    return query

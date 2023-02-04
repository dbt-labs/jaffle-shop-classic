import datetime as dt
from typing import Optional

from sqlalchemy.orm import Session, Query, InstrumentedAttribute

from postgres.models import MonthlyOrders, CustomerMonthlyOrders


def get_monthly_orders(db: Session, customer_id: Optional[int] = None, start_date: Optional[dt.date] = None,
                       end_date: Optional[dt.date] = None) -> Query:
    """Return all monthly orders, filtered by the given parameters"""
    if customer_id:
        query = db.query(CustomerMonthlyOrders).filter(CustomerMonthlyOrders.customer_id == customer_id)
        month_field = CustomerMonthlyOrders.month
    else:
        query = db.query(MonthlyOrders)
        month_field = MonthlyOrders.month

    return filter_results_by_dates(query, month_field, start_date, end_date)


def filter_results_by_dates(query: Query, month_field: InstrumentedAttribute, start_date: Optional[dt.date] = None, end_date: Optional[dt.date] = None):
    if start_date:
        query = query.filter(month_field >= str(start_date))
    if end_date:
        query = query.filter(month_field <= str(end_date))

    return query

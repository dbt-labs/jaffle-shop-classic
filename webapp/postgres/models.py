from sqlalchemy import Column, Integer, DateTime, String, Date

from postgres.database import Base


class MonthlyOrders(Base):
    __tablename__ = "monthly_orders"
    __table_args__ = {'schema': 'exercise'}

    month = Column(Date, primary_key=True)  # sqlalchemy requires some column to be primary key, randomly chose this one
    amount = Column(Integer)


class CustomerMonthlyOrders(Base):
    __tablename__ = "customer_monthly_orders"
    __table_args__ = {'schema': 'exercise'}

    customer_id = Column(Date)
    first_name = Column(String)
    last_name = Column(String)
    month = Column(DateTime, primary_key=True)  # sqlalchemy requires some column to be primary key, randomly chose this one
    amount = Column(Integer)

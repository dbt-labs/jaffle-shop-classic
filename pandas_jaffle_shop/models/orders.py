def model(dbt, session):

    payment_methods = ["credit_card", "coupon", "bank_transfer", "gift_card"]

    order_payments_renames = {
        f"{payment_method}": f"{payment_method}_amount"
        for payment_method in payment_methods
    }
    order_payments_renames = {
        key: value.upper() for key, value in order_payments_renames.items()
    }

    orders_renames = {"total_amount": "amount"}
    orders_renames = {
        key.upper(): value.upper() for key, value in orders_renames.items()
    }

    stg_orders = dbt.ref('stg_orders')
    stg_orders = stg_orders.to_pandas()

    stg_payments = dbt.ref('stg_payments')
    stg_payments = stg_payments.to_pandas()

    stg_customers = dbt.ref('stg_customers')
    stg_customers = stg_customers.to_pandas()

    order_payments_totals = stg_payments.groupby("ORDER_ID").agg(
        AMOUNT=("AMOUNT", "sum")
    )

    order_payments = (
        stg_payments.groupby(["ORDER_ID", "PAYMENT_METHOD"])
        .agg(payment_method_amount=("AMOUNT", "sum"))
        .reset_index()
        .pivot(index="ORDER_ID", columns="PAYMENT_METHOD", values="PAYMENT_METHOD_AMOUNT".lower())
        .rename(columns=order_payments_renames)
        .merge(order_payments_totals, on="ORDER_ID", how="left")
        .reset_index()
    )

    orders = stg_orders.merge(order_payments, on="ORDER_ID", how="left").rename(
        columns=orders_renames
    )
    orders = orders.fillna(0) # hacked the mainframe (fixes tests)

    return orders

def model(dbt, session):

    customers_renames = {"total_amount": "customer_lifetime_value"}
    customers_renames = {
        key.upper(): value.upper() for key, value in customers_renames.items()
    }


    stg_orders = dbt.ref('stg_orders')
    stg_orders = stg_orders.to_pandas()

    stg_payments = dbt.ref('stg_payments')
    stg_payments = stg_payments.to_pandas()

    stg_customers = dbt.ref('stg_customers')
    stg_customers = stg_customers.to_pandas()

    customer_orders = (
        stg_orders.groupby("customer_id".upper())
        .agg(
            first_order=("order_date".upper(), "min"),
            most_recent_order=("order_date".upper(), "max"),
            number_of_orders=("order_id".upper(), "count"),
        )
        .reset_index()
    )

    customer_payments = (
        stg_payments.merge(stg_orders, on="order_id".upper(), how="left")
        .groupby("customer_id".upper())
        .agg(total_amount=("amount".upper(), "sum"))
        .reset_index()
    )

    customers = (
        stg_customers.merge(customer_orders, on="customer_id".upper(), how="left")
        .merge(customer_payments, on="customer_id".upper(), how="left")
        .rename(columns=customers_renames)
    )


    return customers

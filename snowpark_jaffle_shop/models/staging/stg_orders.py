def model(dbt, session):

    orders_renames = {"id": "order_id", "user_id": "customer_id"}
    orders_renames = {key.upper(): value.upper() for key, value in orders_renames.items()}

    stg_orders = dbt.ref('raw_orders')

    for col_name in orders_renames:
        stg_orders = stg_orders.rename(stg_orders[col_name], orders_renames[col_name])

    return stg_orders

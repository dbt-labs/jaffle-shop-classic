def model(dbt, session):

    customers_renames = {"id": "customer_id"}
    customers_renames = {key.upper(): value.upper() for key, value in customers_renames.items()}

    stg_customers = dbt.ref('raw_customers')

    for col_name in customers_renames:
        stg_customers = stg_customers.rename(stg_customers[col_name], customers_renames[col_name])

    return stg_customers

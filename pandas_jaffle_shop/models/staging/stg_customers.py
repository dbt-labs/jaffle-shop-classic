def model(dbt, session):

    customers_renames = {"id": "customer_id"}
    customers_renames = {
        key.upper(): value.upper() for key, value in customers_renames.items()
    }

    stg_customers = dbt.ref('raw_customers')
    stg_customers = stg_customers.to_pandas() # see https://github.com/dbt-labs/dbt-core/issues/5646

    stg_customers = stg_customers.rename(columns=customers_renames)

    return stg_customers

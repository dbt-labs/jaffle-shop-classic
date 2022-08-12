def add_one(x):
    return x + 1

def model(dbt, session):
    dbt.config(
        materialized="table"
    )

    df = dbt.ref("customers")
    df2 = df.withColumn("degree_plus_one", add_one(df.customer_lifetime_value))

    return df2

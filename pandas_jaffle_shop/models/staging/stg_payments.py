def model(dbt, session):
    payments_renames = {"id": "payment_id"}
    payments_renames = {
        key.upper(): value.upper() for key, value in payments_renames.items()
    }

    raw_payments = dbt.ref('raw_payments')
    raw_payments = raw_payments.to_pandas() # see https://github.com/dbt-labs/dbt-core/issues/5646

    payments = raw_payments.rename(columns=payments_renames)
    # -- `amount` is currently stored in cents, so we convert it to dollars
    payments["AMOUNT"] /= 100

    return payments

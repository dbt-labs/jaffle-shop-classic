def model(dbt, session):
    print("hello world\n\n\n")

    payments_renames = {"id": "payment_id"}

    raw_payments = dbt.ref('raw_payments')
    raw_payments = raw_payments.to_pandas() # see https://github.com/dbt-labs/dbt-core/issues/5646

    payments = raw_payments.rename(columns=payments_renames)
    print("\n"*4)
    print(payments.info())
    print(payments)
    print("\n"*4)
    # -- `amount` is currently stored in cents, so we convert it to dollars
    #payments["amount"] /= 100

    return payments

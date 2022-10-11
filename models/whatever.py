def model(dbt, session):
    return session.sql("select 1 as id")

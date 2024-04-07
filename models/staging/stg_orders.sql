with raw_orders as (

    {#-
    Normally we would select from the table here, but we are using seeds to load
    our data in this project
    #}
    select * from {{ ref('raw_orders') }}

)



    select
        id as order_id,
        user_id as customer_id,
        order_date,
        status

    from raw_orders

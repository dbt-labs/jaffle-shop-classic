with source as (

    {#-
    Normally we would select from the table here, but we are using seeds to load
    our data in this project
    #}
    select * from {{ ref('raw_orders') }}

),

renamed as (

    select
        id as order_id,
        user_id as customer_id,
        order_date,
        status

    from source

),

-- Shift the order_date by the number of days since 2018-04-09 (the max order_date in the raw data)
shift_date as (
    
    select
        order_id,
        customer_id,
        (order_date + datediff('day', date '2018-04-09', CURRENT_DATE)::int) as order_date,
        status        

    from renamed
)

select * from shift_date

with source as (

    {#-
    Normally we would select from the table here, but we are using seeds to load
    our data in this project
    #}
    select * from user_pchauhan.jaffle_shop.orders

),

renamed as (

    select
        order_id,
        customer_id,
        order_date,
        status

    from source

)

select * from renamed

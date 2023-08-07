with source as (
    
    {#-
    Normally we would select from the table here, but we are using seeds to load
    our data in this project
    #}
    select * from {{ source('stripe','payment') }}

),

renamed as (

    select
        id as payment_id,
        orderid as order_id,
        paymentmethod as payment_method ,

        -- `amount` is currently stored in cents, so we convert it to dollars
        amount / 100 as amount

    from source

)

select * from renamed

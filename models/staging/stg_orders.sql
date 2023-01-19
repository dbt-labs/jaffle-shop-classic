with source as (

    {#-
    Normally we would select from the table here, but we are using seeds to load
    our data in this project
    #}
    select * from {{ ref('raw_orders') }}

),

customer_address as (
    select
        id,
        address,
    from  {{ ref('raw_customers') }}
)

renamed as (

    select
        source.id as order_id,
        source.user_id as customer_id,
        source.order_date,
        source.status
        customer_address.address as shipping_address

    from source
    join customer_address
    on source.user_id = customer_address.id

)

select * from renamed

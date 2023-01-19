with source as (
    
    {#-
    Normally we would select from the table here, but we are using seeds to load
    our data in this project
    #}
    select 
        {{ ref('raw_payments') }}.id,
        {{ ref('raw_payments') }}.order_id,
        {{ ref('raw_payments') }}.payment_method,
        {{ ref('raw_payments') }}.amount
        {{ ref('raw_orders') }}.user_id as customer_id
    from {{ ref('raw_payments') }}
    join {{ ref('raw_orders') }}
    on {{ ref('raw_payments') }}.order_id = {{ ref('raw_orders') }}.id
),

customer_address as (
    select
        id,
        address,
    from  {{ ref('raw_customers') }}
)

renamed as (

    select
        source.id as payment_id,
        source.order_id,
        source.payment_method,
        -- `amount` is currently stored in cents, so we convert it to dollars
        source.amount / 100 as amount
        customer_address.address as billing_address
    from source
    join customer_address
    on source.customer_id = customer_address.id
)

select * from renamed

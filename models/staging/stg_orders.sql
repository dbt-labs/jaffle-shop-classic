{{ config(
    materialized='incremental',
    unique_key='id',
) }}
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
    {% if is_incremental() %}
    WHERE order_date >= (SELECT MAX(order_date) FROM {{ this }})
    {% endif %}
)

select * from renamed

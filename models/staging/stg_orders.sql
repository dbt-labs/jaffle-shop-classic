{{
  config(
    table_type = 'dimension',
    primary_index = 'order_id',
    materialized = 'incremental',
    incremental_strategy='append'
    )
}}


with source as (
    {#-
    Normally we would select from the table here, but we are using seeds to load
    our data in this project
    #}
    select * from {{ ref('raw_orders') }}
    {% if is_incremental() %}
       where order_date > (select max(order_date) from {{ this }})
    {% endif %}
),
renamed as (
    select
        id as order_id,
        user_id as customer_id,
        order_date,
        status
    from source
)
select * from renamed

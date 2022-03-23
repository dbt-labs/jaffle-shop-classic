{{
  config(
    materialized = 'incremental',
    table_type = 'fact',
    incremental_strategy='append',
    primary_index='customer_id'
    )
}}

with source as (

    {#-
    Normally we would select from the table here, but we are using seeds to load
    our data in this project
    #}
    select * from {{ source('s3', 'raw_customers') }}

    {% if is_incremental() %}
      -- this filter will only be applied on an incremental run
      where id > (select max(customer_id)-3 from {{ this }})
    {% endif %}
),

renamed as (

    select
        id as customer_id,
        first_name,
        last_name

    from source

)

select * from renamed

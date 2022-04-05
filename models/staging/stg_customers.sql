{{
  config(
    materialized = 'incremental',
    table_type = 'fact',
    incremental_strategy='append',
    primary_index='customer_id'
  )
}}

WITH source AS (
    {#-
    Normally we would select from the table here, but we are using seeds to load
    our data in this project
    #}
    SELECT * FROM {{ source('s3', 'raw_customers') }}

    {%- if is_incremental() -%}
      -- this filter will only be applied on an incremental run
      WHERE id > (SELECT MAX(customer_id) FROM {{ this }})
    {%- endif -%}
),
renamed AS (
    SELECT
        id AS customer_id,
        first_name,
        last_name
    FROM source
)

SELECT * FROM renamed

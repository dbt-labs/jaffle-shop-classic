{{
  config(
    materialized='view'
  )
}}

with customers as (

    select * from {{ ref('stg_customers') }}

),


final as (

    select
        *
    from customers

)

select * from final

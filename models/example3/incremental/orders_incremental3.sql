{{
  config(
    unique_key='order_date'
  )
}}

select * from {{ref('param_orders')}}
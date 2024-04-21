--start_query
WITH orders AS (SELECT * FROM {{ ref('orders') }})

SELECT * FROM ( 
  select * from orders
) AS montara_model
--start_incremental
{% if is_incremental() %}
  WHERE order_date >= (SELECT max(order_date) FROM {{ this }})
{% endif %}
--end_incremental
--original_sql
--select * from orders
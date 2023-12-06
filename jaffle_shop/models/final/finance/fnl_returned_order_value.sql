WITH returned_orders AS (
  SELECT * 
  FROM {{ ref('wh_orders') }}
  WHERE status = 'returned'
)

customers AS (
  SELECT * 
  FROM {{ref('stg_customers') }}
)

SELECT 
  customer_id
  , SUM(COALESCE(total_amount, 0) AS total_value
FROM returned_orders
LEFT JOIN customers
  ON returned_orders.customer_id = customers.customer_id
GROUP BY customer_id

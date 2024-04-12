WITH returned_orders AS (
  SELECT 
    customer_id AS customer_id
    ,amount as total_amount
  FROM {{ ref('wh_orders') }}
  WHERE status = 'returned'
)

SELECT 
  customer_id
  ,SUM(COALESCE(total_amount, 0)) AS total_value
FROM returned_orders
GROUP BY customer_id
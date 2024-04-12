WITH customer_orders AS (
  SELECT 
    customer_id
    , DATE_FORMAT(first_order, 'MMMM') As order_month
  FROM {{ ref('wh_customers') }}
)

SELECT 
  first_order AS first_order_month
  , COUNT(*) AS customer_count
FROM customer_orders
GROUP BY first_order
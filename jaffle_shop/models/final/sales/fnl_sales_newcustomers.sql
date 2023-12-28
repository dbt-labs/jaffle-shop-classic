WITH first_orders AS (
    SELECT
      customers.customer_id
      ,  DATE_FORMAT(customers.first_order, '%y-%m') AS first_order_month
    FROM {{ ref('wh_jaffle_customers') }} AS customers
)

SELECT
  first_order_month
  , COUNT(*) AS new_customers_count
FROM first_orders
GROUP BY first_order_month

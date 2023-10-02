{{ config(
  tags=['sales', 'daily']
) }}

WITH new_customer AS (
SELECT
DATE_TRUNC('month', orders.order_date) AS order_month
, COUNT(customers.customer_id) AS customer_count
FROM {{ ref('wh_customers_pii') }} AS customers
INNER JOIN {{ ref('stg_orders') }} AS orders
ON orders.customer_id = customers.customer_id
WHERE customers.first_order = customers.most_recent_order
GROUP BY DATE_TRUNC('month', orders.order_date)
)

SELECT
  customer_count
  , order_month
FROM new_customer
WITH customer_orders AS (
SELECT
  customer_id
  , MIN(order_date) AS first_order
  , MAX(order_date) AS most_recent_order
  , COUNT(order_id) AS number_of_orders
FROM {{ ref('stg_orders') }}
GROUP BY customer_id
)

, customer_payments AS (
SELECT
  orders.customer_id
  , SUM(payments.amount_cents/100) AS total_amount_dollars
FROM {{ ref('stg_payments') }} AS payments
LEFT JOIN orders
  ON payments.order_id = orders.order_id
GROUP BY orders.customer_id
)

, final_output AS (
SELECT
  customers.customer_id
  , customers.first_name_hash
  , customers.last_name_hash
  , customer_orders.first_order
  , customer_orders.most_recent_order
  , customer_orders.number_of_orders
  , customer_payments.total_amount_dollars AS customer_lifetime_value
FROM {{ ref('stg_customers') }} AS customers
LEFT JOIN customer_orders
 ON customers.customer_id = customer_orders.customer_id
LEFT JOIN customer_payments
 ON customers.customer_id = customer_payments.customer_id
)

SELECT
  customer_id
  , first_name_hash
  , last_name_hash
  , number_of_orders
  , customer_lifetime_value
  , first_order
  , most_recent_order
FROM final_output

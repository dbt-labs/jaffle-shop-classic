SELECT
  NULLIF(id, '') AS order_id
  , NULLIF(user_id, '') AS customer_id
  , NULLIF(status, '') AS status
  , DATE(order_date) AS order_date
FROM {{ ref('raw_orders') }}

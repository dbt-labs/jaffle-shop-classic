{{ config(
  tags=['finance', 'daily']
) }}

SELECT
customer_id
, SUM(amount) AS total_value
FROM {{ ref('wh_orders') }}
WHERE status = 'returned'
GROUP BY customer_id

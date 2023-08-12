SELECT
  user_id
  , sum(amount) AS total_returned_amount
FROM {{ ref('wh_orders') }}
WHERE
  status = "returned"
GROUP BY 
  customer_id
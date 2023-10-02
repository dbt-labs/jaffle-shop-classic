SELECT
  NULLIF(id, '') AS payment_id
  , NULLIF(order_id, '') AS order_id
  , NULLIF(payment_method, '') AS payment_method
  , CAST(amount AS FLOAT) AS amount_cents
FROM {{ ref('raw_payments') }}

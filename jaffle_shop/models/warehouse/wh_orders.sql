{% set payment_methods = ['credit_card', 'coupon', 'bank_transfer', 'gift_card'] %}

WITH order_payments AS (
SELECT
  order_id
  {% for payment_method in payment_methods -%}
  , SUM(CASE WHEN payment_method = '{{ payment_method }}' THEN amount_cents ELSE 0 END) AS {{ payment_method }}_amount
  {% endfor -%}
  , SUM(amount_cents/100) AS total_amount_dollars
FROM {{ ref('stg_payments') }}
GROUP BY order_id
)

, total_amount AS (
SELECT
  orders.order_id
  , orders.customer_id
  , orders.order_date
  , orders.status
  , order_payments.credit_card_amount
  , order_payments.coupon_amount
  , order_payments.bank_transfer_amount
  , order_payments.gift_card_amount
  , order_payments.total_amount_dollars AS amount
FROM {{ ref('stg_orders') }} AS orders
LEFT JOIN order_payments
  ON orders.order_id = order_payments.order_id
)

SELECT
  order_id
  , customer_id
  , status
  , credit_card_amount
  , coupon_amount
  , bank_transfer_amount
  , gift_card_amount
  , amount
  , order_date
FROM total_amount

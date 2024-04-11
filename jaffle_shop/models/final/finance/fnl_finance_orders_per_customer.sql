SELECT 
  customers.customer_id
  , customers.customer_lifetime_value
FROM {{ ref('wh_customers') }} AS customers

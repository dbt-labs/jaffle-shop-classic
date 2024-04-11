SELECT 
  DATE_TRUNC('MONTH', customers.first_order) AS month_start_date
  , COUNT(customers.customer_id) AS new_customer_count
FROM {{ ref('wh_customers') }} AS customers
GROUP BY DATE_TRUNC('MONTH', customers.first_order)
ORDER BY DATE_TRUNC('MONTH', customers.first_order) DESC

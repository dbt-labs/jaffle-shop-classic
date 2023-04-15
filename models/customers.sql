WITH orders AS (

  SELECT * 
  
  FROM {{ ref('stg_orders')}}

),

payments AS (

  SELECT * 
  
  FROM {{ ref('stg_payments')}}

),

customer_payments AS (

  SELECT 
    orders.customer_id AS customer_id,
    amount
  
  FROM payments
  LEFT JOIN orders
     ON payments.order_id = orders.order_id

),

amount_per_customer AS (

  SELECT 
    customer_id,
    sum(amount) AS total_amount
  
  FROM customer_payments
  
  GROUP BY customer_id

),

customer_orders AS (

  SELECT 
    customer_id,
    min(order_date) AS first_order,
    max(order_date) AS most_recent_order,
    count(order_id) AS number_of_orders
  
  FROM orders
  
  GROUP BY customer_id

),

customers AS (

  SELECT * 
  
  FROM {{ ref('stg_customers')}}

),

customer_report AS (

  SELECT 
    customers.customer_id,
    customers.first_name,
    customers.last_name,
    customer_orders.first_order,
    customer_orders.most_recent_order,
    customer_orders.number_of_orders AS total_orders,
    amount_per_customer.total_amount AS customer_lifetime_value
  
  FROM customers
  LEFT JOIN customer_orders
     ON customers.customer_id = customer_orders.customer_id
  LEFT JOIN amount_per_customer
     ON customers.customer_id = amount_per_customer.customer_id

),

final_with_order AS (

  SELECT * 
  
  FROM customer_report
  
  ORDER BY total_orders DESC

)

SELECT *

FROM final_with_order

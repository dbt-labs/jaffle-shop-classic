WITH customers AS (

  SELECT * 
  
  FROM {{ ref('customers')}}

),

orders AS (

  SELECT * 
  
  FROM {{ ref('orders')}}

),

customer_order_amount AS (

  SELECT 
    customers.customer_id AS customer_id,
    customers.first_name AS first_name,
    customers.last_name AS last_name,
    orders.amount,
    CONCAT(first_name, ' ', last_name) AS full_name,
    DATEDIFF(DAY, first_order, CURRENT_DATE) AS account_length_day
  
  FROM customers
  INNER JOIN orders
     ON customers.customer_id = orders.customer_id

),

revenue_by_customer AS (

  SELECT 
    customer_id,
    first_name,
    last_name,
    sum(amount) AS revenue
  
  FROM customer_order_amount
  
  GROUP BY 
    customer_id, first_name, last_name

),

revenue_desc AS (

  SELECT * 
  
  FROM revenue_by_customer
  
  ORDER BY revenue DESC NULLS FIRST

),

top_5 AS (

  SELECT * 
  
  FROM revenue_desc
  
  LIMIT 5

)

SELECT *

FROM top_5

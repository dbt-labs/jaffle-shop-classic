WITH raw_customers AS (

  SELECT * 
  
  FROM {{ source('main.default', 'raw_customers') }}

),

raw_orders AS (

  SELECT * 
  
  FROM {{ source('main.default', 'raw_orders') }}

),

Join_1 AS (

  SELECT 
    raw_orders.user_id AS user_id,
    raw_orders.order_date AS order_date,
    raw_orders.status AS status,
    raw_customers.id AS id,
    raw_customers.first_name AS first_name,
    raw_customers.last_name AS last_name
  
  FROM raw_customers
  INNER JOIN raw_orders
     ON raw_customers.id = raw_orders.id

),

Reformat_1 AS (

  SELECT * 
  
  FROM Join_1

)

SELECT *

FROM Reformat_1

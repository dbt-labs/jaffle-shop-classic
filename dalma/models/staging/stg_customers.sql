WITH grades AS (

  {#-
  Normally we would select from the table here, but we are using seeds to load
  our data in this project
  #}
  SELECT * 
  
  FROM {{ ref('raw_customers')}}

),

rename AS (

  SELECT 
    id AS customer_id,
    first_name AS first_name,
    last_name AS last_name
  
  FROM grades

)

SELECT *

FROM rename

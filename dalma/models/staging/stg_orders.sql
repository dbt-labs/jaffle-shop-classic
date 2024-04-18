WITH source AS (

  {#-
  Normally we would select from the table here, but we are using seeds to load
  our data in this project
  #}
  SELECT * 
  
  FROM {{ ref('raw_orders')}}

),

renamed AS (

  SELECT 
    id AS order_id,
    user_id AS customer_id,
    order_date,
    status
  
  FROM source

)

SELECT *

FROM renamed

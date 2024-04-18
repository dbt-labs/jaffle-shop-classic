WITH source AS (

  {#-
  Normally we would select from the table here, but we are using seeds to load
  our data in this project
  #}
  SELECT * 
  
  FROM {{ ref('raw_payments')}}

),

renamed AS (

  SELECT 
    id AS payment_id,
    order_id,
    payment_method,
    -- `amount` is currently stored in cents, so we convert it to dollars
    amount / 100 AS amount
  
  FROM source

)

SELECT *

FROM renamed

WITH raw_orders AS (

  SELECT * 
  
  FROM {{ ref('raw_orders')}}

),

raw_payments AS (

  SELECT * 
  
  FROM {{ ref('raw_payments')}}

),

byOrder AS (

  SELECT 
    raw_payments.payment_method AS payment_method,
    raw_payments.amount AS amount,
    in1.order_date AS order_date,
    in1.status AS status,
    raw_payments.order_id AS order_id,
    in1.user_id AS user_id
  
  FROM raw_payments
  INNER JOIN raw_orders AS in1
     ON raw_payments.order_id = in1.id

),

raw_customers AS (

  SELECT * 
  
  FROM {{ ref('raw_customers')}}

),

byCustomer AS (

  SELECT 
    in0.USER_ID AS USER_ID,
    in0.AMOUNT AS AMOUNT,
    in0.ORDER_DATE AS ORDER_DATE,
    CALCULATE_FULL_NAME(in1.first_name, '', in1.last_name) AS full_name,
    {{ dollars_to_cents('in0.AMOUNT') }} AS AMOUNT_CENTS
  
  FROM byOrder AS in0
  INNER JOIN raw_customers AS in1
     ON in0.USER_ID = in1.id

),

AmountByCustomer AS (

  SELECT 
    USER_ID AS USER_ID,
    sum(AMOUNT) AS AMOUNT,
    max(ORDER_DATE) AS ORDER_DATE,
    ANY_VALUE(FULL_NAME) AS FULL_NAME,
    sum(AMOUNT_CENTS) AS AMOUNT_CENTS
  
  FROM byCustomer
  
  GROUP BY USER_ID

),

HumanReadableAmount AS (

  SELECT 
    USER_ID AS USER_ID,
    AMOUNT AS AMOUNT,
    ORDER_DATE AS ORDER_DATE,
    FULL_NAME AS FULL_NAME,
    AMOUNT_CENTS AS AMOUNT_CENTS,
    {{ human_readable_number('AMOUNT_CENTS') }} AS HUMAN_AMOUNT_CENTS,
    {{ human_readable_number('AMOUNT') }} AS HUMAN_AMOUNT
  
  FROM AmountByCustomer

)

SELECT * 

FROM HumanReadableAmount

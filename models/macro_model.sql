WITH RAW_CUSTOMERS AS (

  SELECT * 
  
  FROM {{ source('JAFFLE_SHOP.public', 'RAW_CUSTOMERS') }}

),

RAW_ORDERS AS (

  SELECT * 
  
  FROM {{ source('JAFFLE_SHOP.public', 'RAW_ORDERS') }}

),

Join_1 AS (

  SELECT 
    RAW_ORDERS.USER_ID AS USER_ID,
    RAW_ORDERS.ORDER_DATE AS ORDER_DATE,
    RAW_ORDERS.STATUS AS STATUS,
    RAW_CUSTOMERS.ID AS ID,
    RAW_CUSTOMERS.FIRST_NAME AS FIRST_NAME,
    RAW_CUSTOMERS.LAST_NAME AS LAST_NAME
  
  FROM RAW_CUSTOMERS
  INNER JOIN RAW_ORDERS
     ON RAW_CUSTOMERS.ID = RAW_ORDERS.ID

),

Reformat_1 AS (

  SELECT 
    USER_ID AS USER_ID,
    ORDER_DATE AS ORDER_DATE,
    STATUS AS STATUS,
    ID AS ID,
    FIRST_NAME AS FIRST_NAME,
    LAST_NAME AS LAST_NAME,
    {{ cents('id') }} AS my_first_cent,
    {{ cents('id', 45) }} AS my_second_cent
  
  FROM Join_1

),

Reformat_2 AS (

  SELECT * 
  
  FROM Reformat_1

),

audit_1 AS (

  {{ noForkMyGitJaffleShop.audit(table_name = 'Reformat_2') }}

)

SELECT * 

FROM audit_1

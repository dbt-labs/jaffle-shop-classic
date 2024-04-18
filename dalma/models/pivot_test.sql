WITH GRADES AS (

  SELECT * 
  
  FROM {{ source('BLACKROCK.TEST', 'GRADES') }}

),

pivot_1 AS (

  {{ HelloWorld_SQL.pivot( column = 'SUBJECT' , values = ['Mathematics', 'Science', 'Geography'] , then_value = 'MARKS' , else_value = 'NULL' , agg = 'max' ) }}

)

SELECT *

FROM pivot_1

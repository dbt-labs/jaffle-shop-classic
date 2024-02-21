WITH GRADES AS (

  SELECT * 
  
  FROM {{ source('BLACKROCK.TEST', 'GRADES') }}

)

SELECT STUDENT
{{HelloWorld_SQL.pivot(
  column='SUBJECT',
  values=['Mathematics', 'Science', 'Geography'],
  agg='max',
  then_value='MARKS',
  else_value='NULL')}}

FROM GRADES
group by STUDENT

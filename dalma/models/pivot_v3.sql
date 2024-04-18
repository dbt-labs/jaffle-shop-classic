WITH pivot_query_1 AS (

  {{ HelloWorld_SQL.pivot_query(source = 'BLACKROCK.TEST.GRADES', primary_keys = ['STUDENT'], column = 'SUBJECT', values = [
    'Mathematics', 
    'Science', 
    'Geography'
  ], agg = 'max', then_value = 'MARKS', else_value = 'NULL') }}

)

SELECT *

FROM pivot_query_1

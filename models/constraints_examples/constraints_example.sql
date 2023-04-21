{{
  config(
    materialized = "table",
  )
}}

select 
  'one' as id, 
  'blue' as color, 
  cast('2012-01-01' as date) as date_day

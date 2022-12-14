{{
  config(
    materialized = "table",
    constraints_enabled = true
  )
}}

select 
  null as id, 
  'blue' as color, 
  cast('2019-01-01' as date) as date_day

--start_query
WITH customers AS (SELECT * FROM {{ ref('customers') }})

select * from customers
--original_sql
--select * from customers
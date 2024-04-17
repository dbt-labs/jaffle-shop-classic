{% test test_something__cleansed_listings__(model) %}
--start_test_query
--start_query
WITH orders AS (SELECT * FROM {{ ref('orders') }})

select * from orders
--original_sql
--select * from orders
--end_test_query
{% endtest %}
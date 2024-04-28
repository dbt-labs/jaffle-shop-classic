--start_query
WITH cleansed_listings AS (SELECT * FROM {{ ref('cleansed_listings') }})

select * from cleansed_listings
--original_sql
--select * from cleansed_listings 
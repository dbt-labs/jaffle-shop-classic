--start_query
WITH RAW_LISTINGS AS ( SELECT * FROM {{ source('RAW_LISTINGS', 'RAW_LISTINGS') }})

select * from raw_listings
--original_sql
--select * from raw_listings 
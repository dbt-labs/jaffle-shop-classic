--start_query
WITH RAW_LISTINGS AS ( SELECT * FROM {{ source('RAW_LISTINGS', 'RAW_LISTINGS') }})

SELECT * FROM ( 
  select
*
from
raw_listings
where
id is not null
) AS montara_model
--start_incremental
{% if is_incremental() %}
  WHERE updated_at >= (SELECT max(updated_at) FROM {{ this }})
{% endif %}
--end_incremental
--original_sql
--select
--  *
--from
--  raw_listings
--where
--  id is not null
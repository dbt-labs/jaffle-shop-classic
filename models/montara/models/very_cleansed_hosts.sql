--start_query
WITH cleansed_hosts AS (SELECT * FROM {{ ref('cleansed_hosts') }})

select
*
from
cleansed_hosts
--original_sql
--select
--  *
--from
--  cleansed_hosts
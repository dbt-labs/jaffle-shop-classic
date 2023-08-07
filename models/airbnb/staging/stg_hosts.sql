

with cte as (
    select * from {{ ref('src_hosts')}}
)
select 
host_id,  NVL(host_name, 'Anonymous') as  host_name    ,
IS_SUPERHOST,CREATED_AT,UPDATED_AT, 
current_timestamp as staged_at
from cte 


with cte as (
    select * from {{ ref('src_listings')}}
)
select 
listing_id, listing_name, room_type, host_id,
case when minimum_nights=0 then 1 
when minimum_nights >1 then minimum_nights end as minimum_nights,
cast(replace(price_str,'$','') as decimal) as price ,
CREATED_AT,UPDATED_AT
,current_timestamp as staged_at from cte

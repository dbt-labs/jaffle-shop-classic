
with dim_listings_cleansed as (
    select * from {{ ref('stg_listings')}}
),
dim_hosts_cleansed  as (
    select * from {{ ref('stg_hosts')}}
)
SELECT l.listing_id, 
l.listing_name, 
l.room_type, 
l.minimum_nights, 
l.price, 
h.host_id, 
h.created_at, 
h.host_name, 
h.is_superhost 
from dim_listings_cleansed l
 LEFT JOIN
 dim_hosts_cleansed   h
  on l.host_id =h.host_id

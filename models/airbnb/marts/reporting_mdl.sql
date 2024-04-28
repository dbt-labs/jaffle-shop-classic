
with dim_listings_with_hosts as (
    select * from {{ ref('dim_listings_with_hosts')}}
),
fct_reviews  as (
    select * from {{ ref('fct_reviews')}}
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
from dim_listings_with_hosts l
 LEFT JOIN
 fct_reviews   h
  on l.host_id =h.host_id

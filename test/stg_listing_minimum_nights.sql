select * from {{ ref('stg_listings')}}
where minimum_nights<1
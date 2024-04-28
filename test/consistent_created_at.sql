select * from {{ ref('stg_listings')}} t1
inner join {{ ref('stg_reviews')}} t2
on t1.listing_id = t2.listing_id
where t2.review_date < =t1.created_at


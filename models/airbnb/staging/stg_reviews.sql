


with cte as (
    select * from {{ ref('src_reviews')}}
)
SELECT listing_id, review_date, reviewer_name, review_text, 
review_sentiment ,
  current_timestamp as staged_at from cte

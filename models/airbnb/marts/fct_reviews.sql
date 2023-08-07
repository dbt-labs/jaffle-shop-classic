{{
  config(
    materialized = 'incremental',
    on_schema_change='fail'
    )
}}
WITH stg_reviews AS (
  SELECT * FROM {{ ref('stg_reviews') }}
)
SELECT 
  *
FROM stg_reviews
WHERE review_text is not null
{% if is_incremental() %}
  AND review_date > (select max(review_date) from {{ this }})
{% endif %}
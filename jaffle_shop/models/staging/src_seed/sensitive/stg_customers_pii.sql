SELECT
  NULLIF(id, '') AS customer_id
  , NULLIF(first_name, '') AS first_name
  , NULLIF(last_name, '') AS last_name
FROM {{ ref('raw_customers') }}

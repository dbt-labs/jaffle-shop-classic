SELECT
  {{ hash_sensitive_columns('stg_customers_pii') }}
FROM {{ ref('stg_customers_pii') }}
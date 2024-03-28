{{
  config(
    materialized = 'incremental',
    )
}}

select
*
from
{{ ref('raw_payments') }}

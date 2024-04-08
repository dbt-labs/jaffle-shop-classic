with raw_hosts as (
      select * from {{ source('RAW_HOSTS', 'RAW_HOSTS') }}
)

select * from raw_hosts
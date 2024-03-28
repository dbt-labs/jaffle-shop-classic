with source as (
      select * from {{ source('raw_something', 'raw_something') }}
),
renamed as (
    select
        *
    from source
)
select * from renamed
  


with source as (

      
    select *
    from {{ ref('raw_sales') }}

),

renamed as (
    -- apply transformations

    select
        
    from source

)

select *
from renamed
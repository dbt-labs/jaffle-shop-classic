with source as (

    {#-
    Normally we would select from the table here, but we are using seeds to load
    our data in this project
    #}
    select * from {{ source('airbnb','raw_hosts') }}

),

renamed as (

    select 
    id as host_id,
    name as host_name,
    is_superhost,
    created_at,
    updated_at

    from source

)

select * from renamed

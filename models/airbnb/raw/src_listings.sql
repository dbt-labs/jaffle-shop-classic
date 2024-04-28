with source as (

    {#-
    Normally we would select from the table here, but we are using seeds to load
    our data in this project
    #}
    select * from {{ source('airbnb','raw_listings') }}

),

renamed as (

    select 
    id as listing_id,
    listing_url,
    name as listing_name,
    room_type,
    minimum_nights,
    host_id,
    price as price_str,
    CREATED_AT,
    updated_at
    from source
)

select * from renamed

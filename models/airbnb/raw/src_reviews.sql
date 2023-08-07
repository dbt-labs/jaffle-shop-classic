with source as (

    {#-
    Normally we would select from the table here, but we are using seeds to load
    our data in this project
    #}
    select * from {{ source('airbnb','raw_reviews') }}

),

renamed as (

    select 
    LISTING_ID,
    DATE as review_date,
    REVIEWER_NAME,
    COMMENTS as review_text,
    SENTIMENT as review_sentiment

    from source
)

select * from renamed

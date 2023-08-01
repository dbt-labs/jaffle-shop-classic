with source as (

    select distinct
        customername,
        contactfirstname,
        contactlastname,
        dtloaded
    from "dbtsales"."public"."raw_sales"

),

renamed as (

    select 
        md5(cast(coalesce(cast(customername as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as customer_id,
        customername as customer_name,
        contactfirstname as contact_firstname,
        contactlastname as contact_lastname,
        dtloaded
    from source

)

select  customer_id,
        customer_name,
        contact_firstname,
        contact_lastname,
        dtloaded
from renamed
UNION
select  'a',
        'b',
        NULL,
        NULL,
        now()
from renamed
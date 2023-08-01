
  
    

  create  table "dbtsales"."public"."int_sales__dbt_tmp"
  
  
    as
  
  (
    with source as (

      
    select *
    from "dbtsales"."public"."raw_sales"

),

renamed as (
    -- apply transformations

    select
        
    from source

)

select *
from renamed
  );
  
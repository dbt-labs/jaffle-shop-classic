select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select CustomerID
from "dbtsales"."public"."customers"
where CustomerID is null



      
    ) dbt_internal_test
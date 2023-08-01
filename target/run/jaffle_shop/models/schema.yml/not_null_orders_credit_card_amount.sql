select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select credit_card_amount
from "dbtsales"."public"."orders"
where credit_card_amount is null



      
    ) dbt_internal_test
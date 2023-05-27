select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    payment_id as unique_field,
    count(*) as n_records

from "sales"."public"."stg_payments"
where payment_id is not null
group by payment_id
having count(*) > 1



      
    ) dbt_internal_test
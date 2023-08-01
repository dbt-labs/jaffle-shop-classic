

with customers as (

    select * from "dbtsales"."public_stg"."stg_customers"

),


final as (

    select
        *
    from customers

)

select * from final
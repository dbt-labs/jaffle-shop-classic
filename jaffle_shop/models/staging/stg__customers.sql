with

customers as (
    select *
    from {{ ref("raw__customers") }}
),

final as (
    select
        id as customer_id,
        first_name,
        last_name
    from customers
)

select * from final

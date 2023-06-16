with

payments as (
    select *
    from {{ ref("raw__payments") }}
),

final as (
    select
        id as payment_id,
        order_id,
        payment_method,
        amount / 100 as amount
    from payments
)

select * from final

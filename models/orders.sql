
{% set payment_methods = ['credit_card', 'coupon', 'bank_transfer', 'gift_card'] %}
with orders as (

    select * from {{ ref('stg_orders') }}

),

payments as (

    select * from {{ ref('stg_payments') }}

),

order_payments as (

    select
        order_id,
        {% for payment_method in payments_dict -%}
            case when payment_method = '{{ payment_method }}' then amount else 0 end as {{ payments_dict[payment_method] }}_amount,
        {% endfor %}
        sum(amount) as total_amount

    from payments

    group by order_id

),

final as (

    select
        orders.order_id,
        orders.customer_id,
        orders.order_date,
        orders.status,
        order_payments.credit_card_amount,
        order_payments.coupon_amount,
        order_payments.bank_transfer_amount,
        order_payments.gift_card_amount,
        order_payments.total_amount as amount

    from orders


    left join order_payments
        on orders.order_id = order_payments.order_id

)

select * from final

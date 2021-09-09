with customers as (

    select * from {{ ref('stg_customers') }}

),

orders as (

    select * from {{ ref('stg_orders') }}

),

payments as (

    select * from {{ ref('stg_payments') }}

),

customer_orders as (

        select
        customer_id,

        min(order_date) as first_order,
        max(order_date) as most_recent_order,
        count(order_id) as number_of_orders
    from orders

    group by customer_id

),

customer_payments as (

    select
        orders.customer_id,
        sum(amount) as total_amount

    from payments p

    left join orders o on o.order_id=p.order_id

    group by o.customer_id

),

final as (

    select
        c.customer_id,
        c.first_name,
        c.last_name,
        co.first_order,
        co.most_recent_order,
        co.number_of_orders,
        cp.total_amount as customer_lifetime_value

    from customers c

    left join customer_orders co on c.customer_id=co.customer_id

    left join customer_payments cp on cp.customer_id=c.customer_id

)

select * from final

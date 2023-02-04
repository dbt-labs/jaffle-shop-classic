with orders as (select *
                from {{ ref('orders') }})

select date_trunc('month', order_date) ::date as month,
    sum(amount) as amount
from orders
group by month
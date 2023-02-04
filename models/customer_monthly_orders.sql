with orders as (select *
                from {{ ref('orders') }}),
    customers as (select *
                   from {{ ref('customers') }})

select orders.customer_id,
       customers.first_name,
       customers.last_name,
       date_trunc('month', order_date) ::date as month,
       sum(amount) as amount
from orders
    left join customers
on orders.customer_id = customers.customer_id

group by month, orders.customer_id, customers.first_name, customers.last_name

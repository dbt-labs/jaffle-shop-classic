{% set payment_methods = ['credit_card', 'coupon', 'bank_transfer', 'gift_card', 'direct_debit'] %}
select substr(primary_payment_method,0,length(primary_payment_method)-7) primary_payment_method, sum(sum_customer_value) sum_customer_value from (
select customer_id, max(case when ranknum = 1 then payment_method else null end) primary_payment_method, sum(payment_amount) sum_customer_value from (
select customer_id, payment_method, payment_amount, row_number() over (partition by customer_id order by payment_amount desc) ranknum from (
select customer_id, payment_method, sum(payment_amount) payment_amount from (select customers.customer_id, {% for payment_method in payment_methods -%}orders.{{payment_method}}_amount{% if not loop.last %}, {% endif %}{% endfor %}
from {{ref('customers')}} customers
left join {{ref('orders')}} orders on customers.customer_id = orders.customer_id) t1
unpivot (payment_amount for payment_method in ({% for payment_method in payment_methods -%}{{payment_method}}_amount{% if not loop.last %}, {% endif %}{% endfor -%})) as p1
group by customer_id, payment_method
)
)
group by customer_id
)
group by primary_payment_method
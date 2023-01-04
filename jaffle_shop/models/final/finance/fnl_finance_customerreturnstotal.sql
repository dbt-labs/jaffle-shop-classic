{% set return_states = ['returned', 'return_pending'] %}

select 
    custs.customer_id 

    {% for return_state in return_states -%}
    , SUM(payments.amount_dollars) FILTER (WHERE orders.status =' {{ return_state }}') AS {{ return_state }}_amount_dollars
    {% endfor -%}
    
    , SUM(payments.amount_dollars) AS sum_return_amount_dollars

from {{ ref('wh_orders') }} AS orders
    left join {{ ref('wh_customers') }} AS custs
        on custs.customer_id = orders.customer_id
    left join {{ ref('stg_payments') }} AS payments
        on payments.order_id = orders.order_id
where orders.status IN ('returned', 'return_pending')
GROUP BY customer_id
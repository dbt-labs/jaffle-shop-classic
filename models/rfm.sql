{% set cols = ['r', 'f', 'm'] %}

with rfm_cols as (

	SELECT 
        customer_id,
        most_recent_order as {{ cols[0] }},
        number_of_orders as {{ cols[1] }},
        customer_lifetime_value as {{ cols[2] }}
    FROM  {{ ref('customers') }}
),

final as (
    SELECT 

    {% for col in cols -%}
    ntile(3) over(order by {{ col }}) as {{ col }},
    {% endfor -%}


    customer_id
    FROM rfm_cols
)

SELECT * FROM final



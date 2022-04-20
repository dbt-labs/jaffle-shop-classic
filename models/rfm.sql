{% set cols = ['recency', 'frequency', 'monetary_value'] %}

with customers as (

	SELECT 
        customer_id,
        most_recent_order as recency,
        number_of_orders as frequency,
        customer_lifetime_value as monetary_value
    FROM  {{ ref('customers') }}
),

final as (
    SELECT 

    {% for col in cols -%}
    ntile(3) over(order by {{ col }}) as {{ col }},
    {% endfor -%}
    

    customer_id
    FROM customers
)

SELECT * FROM final



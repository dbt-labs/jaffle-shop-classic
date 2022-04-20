with customers as (

	SELECT 
        customer_id,
        ntile(3) over(order by most_recent_order) as recency,
        ntile(3) over(order by number_of_orders) as frequency,
        ntile(3) over(order by customer_lifetime_value) as monetary_value
    FROM  {{ ref('customers') }}
),

final as (
    SELECT * FROM customers
)

SELECT * FROM final
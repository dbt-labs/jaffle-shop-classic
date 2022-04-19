with orders as (

	SELECT * FROM  {{ ref('orders') }}
),

final as (

	SELECT 
		order_date, 
		sum(amount) as amount,
		avg(amount) as aov
	from orders
    group by 
        order_date
)

SELECT * FROM final
select 
    date_trunc('month', first_order) AS first_order_month
    , count(*) AS number_customers
from {{ ref('wh_customers') }}
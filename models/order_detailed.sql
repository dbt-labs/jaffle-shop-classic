select *, null as cluster_label from {{ ref('stg_order_attributes') }}  left join {{ ref('orders') }} using (order_id)


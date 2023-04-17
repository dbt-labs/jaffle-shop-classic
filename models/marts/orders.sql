with orders as (

    select * from {{ ref('int_order_payments_pivoted') }}

)
,
customers as (

    select * from {{ ref('int_customer_order_history_joined') }}

)
,
statuses as (
    select * from {{ ref('stg_statuses') }}
),
final as (

    select 
        *
    from orders 
    left join customers using (customer_id)
    inner join statuses using (status)
)

select * from final


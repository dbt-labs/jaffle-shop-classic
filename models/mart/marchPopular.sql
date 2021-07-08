with customers as (
    select * from {{ ref('stg_customers') }}
),

orders as (
    select * from {{ ref('stg_orders') }}
),

payments as (
    select * from {{ ref('stg_payments') }}
),



get_dist_payments as (

    select
        distinct payment_method

    from payments

)

select * from get_dist_payments
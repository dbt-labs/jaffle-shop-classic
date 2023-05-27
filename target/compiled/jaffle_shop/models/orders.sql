--Set the payment methods in a variable to use later in a for loop


with orders as (

    select * from "sales"."public"."stg_orders"

),

payments as (

    select * from "sales"."public"."stg_payments"

),

order_payments as (

    select
        order_id,

        --use the variable specified at the top of this model
        sum(case when payment_method = 'bank_transfer' then amount else 0 end) as bank_transfer_amount,
        
        --remove trailling , that may cause double comma errors,--end for loop
        sum(case when payment_method = 'credit_card' then amount else 0 end) as credit_card_amount,
        
        --remove trailling , that may cause double comma errors,--end for loop
        sum(case when payment_method = 'gift_card' then amount else 0 end) as gift_card_amount,
        
        --remove trailling , that may cause double comma errors,--end for loop
        sum(case when payment_method = 'coupon' then amount else 0 end) as coupon_amount,
        
        --remove trailling , that may cause double comma errors--end for loop
        sum(amount) as total_amount

    from payments

    group by order_id

),

final as (

    select
        orders.order_id,
        orders.customer_id,
        orders.order_date,
        orders.status,

        order_payments.bank_transfer_amount,

        order_payments.credit_card_amount,

        order_payments.gift_card_amount,

        order_payments.coupon_amount,

        order_payments.total_amount as amount

    from orders


    left join order_payments
        on orders.order_id = order_payments.order_id

)

select * from final
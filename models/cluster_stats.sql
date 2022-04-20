select
    cluster_label,
    sum(credit_card_amount) as sum_credit_card_amount,
    sum(coupon_amount) as sum_coupon_amount,
    sum(bank_transfer_amount) as sum_bank_transfer_amount,
    sum(gift_card_amount) as sum_gift_card_amount,
    sum(amount) as sum_amount
from {{ ref('order_detailed') }} 
group by cluster_label

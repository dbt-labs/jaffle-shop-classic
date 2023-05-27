{% macro get_payment_methods() %}
{{ return(["bank_transfer", "credit_card", "gift_card", "coupon"]) }}
{% endmacro %}
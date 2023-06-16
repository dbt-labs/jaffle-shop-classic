{{ config(tags=["unit-test"]) }}

{% set payment_methods = ["credit_card", "coupon", "bank_transfer", "gift_card"] %}


{% call dbt_unit_testing.test(
    "pl__orders",
    "Payments are aggregated by order correctly",
    {"cte_name": "order_payments"}
) %}
  {% call dbt_unit_testing.mock_ref("stg__payments", {"input_format": "csv"}) %}
    payment_id,order_id,payment_method,amount
    1,1,null,10
    2,1,null,20
    3,2,null,30
    4,3,null,40
  {% endcall %}

  {# Currently just testing the non-dynamic columns #}
  {% call dbt_unit_testing.expect({"input_format": "csv"}) %}
    order_id,total_amount
    1,30
    2,30
    3,40
  {% endcall %}
{% endcall %}

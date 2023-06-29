{{ config(tags=["unit-test"]) }}

{% set payment_methods = ["credit_card", "coupon", "bank_transfer", "gift_card"] %}

{# Using the `final` CTE here to prove that it's the same as no `cte_name` usage #}
{% call dbt_unit_testing.test(
    "pl__orders",
    "Order metrics are aggregated correctly",
    {"cte_name": "final"}
) %}
  {% call dbt_unit_testing.mock_ref("stg__orders", {"input_format": "csv"}) %}
    order_id,customer_id,order_date,status
    1,1,'2020-01-01',null
    2,1,'2020-01-02',null
    3,2,'2020-01-03',null
  {% endcall %}

  {% call dbt_unit_testing.mock_ref("stg__payments", {"input_format": "csv"}) %}
    payment_id,order_id,payment_method,amount
    1,1,null,10
    2,1,null,20
    3,2,null,30
    4,3,null,40
  {% endcall %}

  {# Currently just testing the non-dynamic columns #}
  {% call dbt_unit_testing.expect({"input_format": "csv"}) %}
    order_id,customer_id,order_date,status,amount
    1,1,'2020-01-01',null,30
    2,1,'2020-01-02',null,30
    3,2,'2020-01-03',null,40
  {% endcall %}
{% endcall %}

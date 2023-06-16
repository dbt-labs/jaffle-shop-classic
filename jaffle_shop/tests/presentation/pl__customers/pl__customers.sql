{{ config(tags=["unit-test"]) }}


{% call dbt_unit_testing.test(
    "pl__customers",
    "Customer metrics are aggregated correctly"
) %}
  {% call dbt_unit_testing.mock_ref("stg__customers", {"input_format": "csv"}) %}
    customer_id,first_name,last_name
    1,'Alpha',  'X'
    2,'Beta',   'Y'
    3,'Charlie','Z'
  {% endcall %}

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

  {% call dbt_unit_testing.expect({"input_format": "csv"}) %}
    customer_id,first_name,last_name,first_order,most_recent_order,number_of_orders,customer_lifetime_value
    1,'Alpha',  'X','2020-01-01','2020-01-02',2,60
    2,'Beta',   'Y','2020-01-03','2020-01-03',1,40
    3,'Charlie','Z',null,null,null,null
  {% endcall %}
{% endcall %}

"""
Test the ``code_coverage/dbt/unit_tests.py`` module.
"""
import textwrap

import pytest

import code_coverage.dbt.unit_tests as unit_tests


@pytest.fixture
def dbt_unit_test() -> str:
    return textwrap.dedent(
        """
        {{ config(tags=["unit-test"]) }}


        {% call dbt_unit_testing.test(
            "pl__customers",
            "Customer orders are aggregated correctly",
            {"cte_name": "customer_orders"}
        ) %}
          {% call dbt_unit_testing.mock_ref("stg__orders", {"input_format": "csv"}) %}
            order_id,customer_id,order_date,status
            1,1,'2020-01-01',null
            2,1,'2020-01-02',null
            3,2,'2020-01-03',null
          {% endcall %}

          {% call dbt_unit_testing.expect({"input_format": "csv"}) %}
            customer_id,first_order,most_recent_order,number_of_orders
            1,'2020-01-01','2020-01-02',2
            2,'2020-01-03','2020-01-03',1
          {% endcall %}
        {% endcall %}
        """
    )

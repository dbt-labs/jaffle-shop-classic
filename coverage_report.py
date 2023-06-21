from dataclasses import dataclass

import typing as t

from jinja2 import Environment, nodes

TEST_STRING = """
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


@dataclass
class TestCase:
    filename: str
    lineno: int
    dbt_model: str
    cte_name: str = None


def has_unit_test_tag(kwargs: t.Iterable[nodes.Keyword]) -> bool:
    for arg in kwargs:
        if arg.key == "tags":
            for item in arg.value.items:
                if item.value == "unit-test":
                    return True

    return False


def is_test_file(ast: nodes.Template) -> bool:
    for node in ast.body[0].nodes:
        if (
            isinstance(node, nodes.Call) and
            node.node.name == "config" and
            has_unit_test_tag(node.kwargs)
        ):
            return True

    return False

def get_cte_name(args: t.Iterable[nodes.Const]) -> t.Optional[str]:
    if len(args) < 3:
        return None

    if not isinstance(args[2], nodes.Dict):
        raise Exception("Unknown 3rd argument provided to test() function!")

    for dict_item in args[2].items:
        if dict_item.key.value == "cte_name":
            return dict_item.value.value

    return None


def main():
    env = Environment()
    ast = env.parse(TEST_STRING)

    calls = [
        callblock.call for callblock in ast.body
        if (
            isinstance(callblock, nodes.CallBlock) and
            callblock.call.node.node.name == "dbt_unit_testing" and
            callblock.call.node.attr == "test"
        )
    ]

    cases = [
        TestCase(
            filename="somefile",
            lineno=call.lineno,
            dbt_model=call.args[0].value,
            cte_name=get_cte_name(call.args)
        )
        for call in calls
    ]

    print("Is test file?", is_test_file(ast))

    pass



if __name__ == "__main__":
    main()
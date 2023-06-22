from dataclasses import dataclass
from pathlib import Path

import typing as t

from jinja2 import Environment, nodes
from jinja2.exceptions import TemplateSyntaxError
import typer

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
    if not ast.body:
        return False

    if not hasattr(ast.body[0], "nodes"):
        return False

    for node in ast.body[0].nodes:
        if (
            isinstance(node, nodes.Call) and
            hasattr(node, "node") and
            hasattr(node.node, "name") and
            node.node.name == "config" and
            has_unit_test_tag(node.kwargs)
        ):
            return True

    return False


def get_cte_name(args: t.List[nodes.Const]) -> t.Optional[str]:
    if len(args) < 3:
        return None

    if not isinstance(args[2], nodes.Dict):
        raise Exception("Unknown 3rd argument provided to test() function!")

    for dict_item in args[2].items:
        if dict_item.key.value == "cte_name":
            return dict_item.value.value

    return None


def get_test_files(env: Environment, src: Path) -> t.List[Path]:
    files = []
    for sql in src.glob("**/*.sql"):
        with sql.open() as f:
            try:
                ast = env.parse(f.read())
            except TemplateSyntaxError:
                continue
            if is_test_file(ast):
                files.append(sql)

    return files


def get_test_cases(env: Environment, file: Path) -> t.List[TestCase]:
    with file.open() as f:
        ast = env.parse(f.read())

    calls = [
        callblock.call for callblock in ast.body
        if (
                isinstance(callblock, nodes.CallBlock) and
                hasattr(callblock.call.node, "node") and
                callblock.call.node.node.name == "dbt_unit_testing" and
                callblock.call.node.attr == "test"
        )
    ]

    return [
        TestCase(
            filename=str(file),
            lineno=call.lineno,
            dbt_model=call.args[0].value,
            cte_name=get_cte_name(call.args)
        )
        for call in calls
    ]


def get_all_test_cases(env: Environment, files: t.List[Path]) -> t.List[TestCase]:
    return [
        case
        for file in files
        for case in get_test_cases(env, file)
    ]


def main(directory: t.Optional[Path] = "."):
    env = Environment()
    files = get_test_files(env, directory)
    cases = get_all_test_cases(env, files)

    from pprint import pprint
    pprint(cases)


if __name__ == "__main__":
    typer.run(main)
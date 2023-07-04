"""
Parse the dbt unit tests to find the CTEs that are tested.
"""
from __future__ import annotations

import dataclasses
import pathlib
from collections.abc import Iterable

from jinja2 import Environment, nodes
from jinja2.exceptions import TemplateSyntaxError


@dataclasses.dataclass
class TestCase:
    filename: str
    lineno: int
    model_name: str
    cte_name: str = None


def has_unit_test_tag(kwargs: Iterable[nodes.Keyword]) -> bool:
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
            isinstance(node, nodes.Call)
            and hasattr(node, "node")
            and hasattr(node.node, "name")
            and node.node.name == "config"
            and has_unit_test_tag(node.kwargs)
        ):
            return True

    return False


def get_cte_name(args: list[nodes.Const]) -> str | None:
    if len(args) < 3:
        return None

    if not isinstance(args[2], nodes.Dict):
        raise Exception("Unknown 3rd argument provided to test() function!")

    for dict_item in args[2].items:
        if dict_item.key.value == "cte_name":
            return dict_item.value.value

    return None


def get_test_files(env: Environment, src: pathlib.Path) -> list[pathlib.Path]:
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


def get_test_cases(env: Environment, file: pathlib.Path) -> list[TestCase]:
    with file.open() as f:
        ast = env.parse(f.read())

    calls = [
        callblock.call
        for callblock in ast.body
        if (
            isinstance(callblock, nodes.CallBlock)
            and hasattr(callblock.call.node, "node")
            and callblock.call.node.node.name == "dbt_unit_testing"
            and callblock.call.node.attr == "test"
        )
    ]

    return [
        TestCase(
            filename=str(file),
            lineno=call.lineno,
            model_name=call.args[0].value,
            cte_name=get_cte_name(call.args),
        )
        for call in calls
    ]


def get_all_test_cases(env: Environment, files: list[pathlib.Path]) -> list[TestCase]:
    return [case for file in files for case in get_test_cases(env, file)]


def parse_dbt_unit_tests(test_paths: list[pathlib.Path]) -> list[TestCase]:
    """
    Return all the dbt unit tests parsed for key information.

    :return: The list of parsed dbt unit tests.
    """
    env = Environment()
    files = [file for dir_ in test_paths for file in get_test_files(env, dir_)]

    return get_all_test_cases(env, files)

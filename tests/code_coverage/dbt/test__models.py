"""
Test the ``code_coverage/dbt/models.py`` module.
"""
import pytest

import sqlglot  # parse_one
from sqlglot import Expression

import code_coverage.dbt.models as models


@pytest.mark.parametrize(
    "name, sql, cte_type",
    [
        ("a", r"SELECT * FROM TABLE", "import"),
        ("b", r"SELECT A, B, C FROM TABLE", "import"),
        ("c", r"SELECT A AS B FROM TABLE", "logical"),
        ("c", r"SELECT SUM(A) FROM TABLE", "logical"),
        ("c", r"SELECT A FROM TABLE GROUP BY A", "logical"),
    ],
)
def test__cte(name: str, sql: str, cte_type: models.CteType):
    """
    Test the ``CTE`` class.
    """
    expression = sqlglot.parse_one(sql)
    cte = models.CTE(name, expression)

    assert cte.name == name
    assert cte.expression == expression
    assert cte.cte_type == cte_type

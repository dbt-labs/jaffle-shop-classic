"""
Parse the compiled model SQL files to find the CTEs.
"""
import dataclasses
import pathlib
from typing import Literal

import sqlglot
from sqlglot import Expression
from typing_extensions import Self

from code_coverage.dbt.config import DbtConfig

CteType = Literal["import", "logical", "final"]

# Clauses which indicate that a CTE is a "logical" CTE, not an "import" CTE
LOGICAL_CTE_ARGS = [
    "with",
    "distinct",
    "limit",
    "joins",
    "laterals",
    "where",
    "pivots",
    "group",
    "qualify",
    "windows",
    "order",
]


@dataclasses.dataclass
class CTE:
    name: str
    expression: Expression
    type: CteType = dataclasses.field(init=False)

    def __post_init__(self):
        self.type = self._determine_cte_type()

    def _determine_cte_type(self) -> CteType:
        """
        The type of CTE this is.

        Can be "import", "logical", or "final".
        """
        if self.name.lower() == "final":
            return "final"
        elif _is_import_cte(self.expression):
            return "import"
        else:
            return "logical"

    def __str__(self) -> str:
        return f"{self.name} ({self.type})"


@dataclasses.dataclass
class Model:
    path: pathlib.Path
    name: str = dataclasses.field(init=False)
    ctes: list[CTE] = dataclasses.field(init=False)

    def __post_init__(self):
        if not self.path.is_file():
            raise TypeError(f"'{self.path}' is not a file.")

        self.name = self.path.stem

    def parse_ctes(self, model_root: pathlib.Path, compiled_root: pathlib.Path) -> Self:
        """
        Parse the CTEs out of the model.

        :param model_root: The root directory for the raw dbt models.
        :param compiled_root: The root directory for the compiled dbt models.

        :return: Self, for chaining.
        """
        compiled_path = compiled_root / self.path.relative_to(model_root)
        with compiled_path.open() as f:
            self.ctes = _get_model_common_table_expressions(f.read())

        return self


def _is_import_cte(common_table_expression: Expression) -> bool:
    """
    Determine if a CTE is an "import" CTE.

    An "import" CTE is one that is a simple `SELECT *` or `SELECT <list of
    columns>` from an object. In particular, it doesn't have any filtering,
    renames, joins, calculations, etc.

    :param common_table_expression: The expression to check.

    :return: ``True`` if the CTE is an "import" CTE, ``False`` otherwise.
    """
    return (
        # The `FROM` isn't a subquery
        getattr(common_table_expression.args["from"], "alias_or_name", "") != ""
        # The column list is a star, or doesn't have any calculations
        and (
            common_table_expression.is_star
            or all(col.key == "column" for col in common_table_expression.args["expressions"])
        )
        # It doesn't have any logical CTE clauses
        and all(common_table_expression.args.get(arg, None) is None for arg in LOGICAL_CTE_ARGS)
    )


def _get_common_table_expressions(sql: str) -> dict[str, Expression]:
    """
    Get the top-level CTEs from a single SQL statement.

    :param sql: The SQL statement to parse.

    :return: A dictionary of CTE name to CTE expression.
    """
    parsed = sqlglot.parse(sql)
    if len(parsed) != 1:
        raise ValueError(f"The SQL text should have a single statement, found {len(parsed)} statements.")

    common_table_expressions: Expression = parsed[0].args.get("with", None)

    if common_table_expressions is None:
        return {}
    return {
        expression.alias: expression.this
        for _, expression in common_table_expressions.iter_expressions()
        if expression.key == "cte"
    }


def _get_model_common_table_expressions(sql: str) -> list[CTE]:
    """
    Get CTEs from a single SQL statement.

    :param sql: The SQL statement to parse.

    :return: A list of CTEs.
    """
    return [
        CTE(name=cte_name, expression=cte_expr) for cte_name, cte_expr in _get_common_table_expressions(sql).items()
    ]


def parse_models_and_ctes(config: DbtConfig) -> list[Model]:
    """
    Walk the models directory and print out the CTEs in each model.

    :param config: The dbt project configuration.
    """
    if not (compiled := config.compiled_paths[0]).exists():
        raise FileNotFoundError(f"The compiled directory '{compiled}' does not exist." f" Try running `dbt compile`.")

    models = config.model_paths[0]
    return [Model(path=path).parse_ctes(model_root=models, compiled_root=compiled) for path in models.glob("**/*.sql")]

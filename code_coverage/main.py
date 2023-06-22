"""
https://jinja.palletsprojects.com/en/3.1.x/extensions/#ast
https://github.com/tobymao/sqlglot
"""
import dataclasses
from pathlib import Path
from typing import Literal

import sqlglot
from sqlglot import Expression


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

    @property
    def cte_type(self) -> Literal["import", "logical", "final"]:
        """
        The type of CTE this is.

        Can be "import", "logical", or "final".
        """
        if self.name.lower() == "final":
            return "final"
        elif is_import_cte(self.expression):
            return "import"
        else:
            return "logical"

    def __str__(self) -> str:
        return f"{self.name} ({self.cte_type})"


def get_common_table_expressions(sql: str) -> dict[str, Expression]:
    """
    Get CTEs from a single SQL statement.
    """
    parsed = sqlglot.parse(sql)
    if len(parsed) != 1:
        raise ValueError(f"The SQL text should have a single statement, found {len(parsed)}.")

    common_table_expressions: Expression = parsed[0].args.get("with", None)

    if common_table_expressions is None:
        return {}
    return {
        expression.alias: expression.this
        for _, expression in common_table_expressions.iter_expressions()
        if expression.key == "cte"
    }


def is_import_cte(common_table_expression: Expression) -> bool:
    """
    Determine if a CTE is an "import" CTE.
    """
    return (
        # The `FROM` isn't a subquery
        getattr(common_table_expression.args["from"], "alias_or_name", "") != ""
        # The column list is a star, or doesn't have any calculations
        and (common_table_expression.is_star or all(
            col.key == "column"
            for col in common_table_expression.args["expressions"]
        ))
        # It doesn't have any logical CTE clauses
        and all(
            common_table_expression.args.get(arg, None) is None
            for arg in LOGICAL_CTE_ARGS
        )
    )


def get_model_common_table_expressions(sql: str) -> list[CTE]:
    """
    Get CTEs from a single SQL statement.
    """
    return [
        CTE(name=cte_name, expression=cte_expr)
        for cte_name, cte_expr in get_common_table_expressions(sql).items()
    ]


def walk_the_models(model_directory: Path) -> None:
    """
    Walk the models directory and print out the CTEs in each model.

    :param model_directory: The directory containing the models.
    """
    assert model_directory.is_dir()
    assert model_directory.name == "models"  # Required by dbt

    compiled = Path("../target/compiled/jaffle_shop/jaffle_shop/models/")
    for path in model_directory.glob("**/*.sql"):
        compiled_path = compiled / path.relative_to("../jaffle_shop/models")
        with open(compiled_path, "r") as f:
            sql = f.read()

        print(path.stem)
        for cte in get_model_common_table_expressions(sql):
            print(f"\t{cte}")


def main() -> None:
    """Parse some SQL."""
    walk_the_models(Path("../jaffle_shop/models"))


if __name__ == "__main__":
    main()

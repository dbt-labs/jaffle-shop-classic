"""
Test the ``code_coverage/dbt/models.py`` module.
"""
import pathlib
import textwrap

import pytest
import sqlglot

import code_coverage.dbt.models as models
from code_coverage.dbt import DbtConfig


@pytest.fixture
def compiled_sql() -> str:
    return textwrap.dedent(
        """
        -- noinspection SqlResolveForFile
        WITH
            a AS (SELECT * FROM TABLE_1),
            b AS (SELECT * FROM TABLE_2),
            c AS (SELECT SUM(A) FROM a),
            d AS (SELECT SUM(B) FROM b),
            final AS (SELECT * FROM c UNION SELECT * FROM d)

        SELECT * FROM final
        """
    )


@pytest.fixture
def dbt_config(tmp_path: pathlib.Path) -> DbtConfig:
    return DbtConfig(
        name="test",
        model_paths=[tmp_path / "models"],
        test_paths=[tmp_path / "tests"],
        target_path=tmp_path / "target",
    )


@pytest.mark.parametrize(
    "name, sql, cte_type",
    [
        ("a", r"SELECT * FROM TABLE", "import"),
        ("b", r"SELECT A, B, C FROM TABLE", "import"),
        ("c", r"SELECT A AS B FROM TABLE", "logical"),
        ("d", r"SELECT SUM(A) FROM TABLE", "logical"),
        ("e", r"SELECT A FROM TABLE GROUP BY A", "logical"),
        ("final", r"SELECT * FROM TABLE", "final"),
    ],
)
def test__cte(name: str, sql: str, cte_type: models.CteType):
    """
    Test the ``CTE`` class.
    """
    expression = sqlglot.parse_one(sql)
    cte = models.CTE(name, expression)
    cte.type = cte_type  # This assignment is tested in ``test__is_import_cte``

    assert cte.name == name
    assert cte.expression == expression
    assert str(cte) == f"{name} ({cte_type})"


def test__model__not_file(tmp_path: pathlib.Path):
    """
    Test that the ``Model`` class raises a ``TypeError`` when the path is
    not a file.
    """
    with pytest.raises(TypeError):
        models.Model(tmp_path)


def test__model(tmp_path: pathlib.Path):
    temp_file = tmp_path / "tmp.txt"
    temp_file.touch()
    model = models.Model(temp_file)

    assert model.path == temp_file
    assert model.name == "tmp"


def test__model__parse_ctes(tmp_path: pathlib.Path, compiled_sql: str):
    """
    Test that ``Model.parse_ctes`` parses the CTEs out of the model.
    """
    model_root = tmp_path / "models"
    compiled_root = tmp_path / "target"
    model_path = model_root / "model.sql"
    compiled_path = compiled_root / "model.sql"

    model_root.mkdir(exist_ok=True), compiled_root.mkdir(exist_ok=True)
    model_path.touch(exist_ok=True), compiled_path.touch(exist_ok=True)

    compiled_path.write_text(compiled_sql)

    model_pre_parse = models.Model(model_path)
    model = model_pre_parse.parse_ctes(model_root, compiled_root)

    assert model_pre_parse == model
    assert len(model.ctes) == 5

    assert model.ctes[0].name == "a"
    assert model.ctes[0].type == "import"

    assert model.ctes[1].name == "b"
    assert model.ctes[1].type == "import"

    assert model.ctes[2].name == "c"
    assert model.ctes[2].type == "logical"

    assert model.ctes[3].name == "d"
    assert model.ctes[3].type == "logical"

    assert model.ctes[4].name == "final"
    assert model.ctes[4].type == "final"


@pytest.mark.parametrize(
    "expression, is_import",
    [
        (r"SELECT * FROM TABLE", True),
        (r"SELECT A, B, C FROM TABLE", True),
        (r"SELECT A AS B FROM TABLE", False),
        (r"SELECT SUM(A) FROM TABLE", False),
        (r"SELECT A FROM TABLE GROUP BY A", False),
    ],
)
def test__is_import_cte(expression: str, is_import: bool):
    """
    Test that ``_is_import_cte`` correctly identifies import CTEs.
    """
    assert models._is_import_cte(sqlglot.parse_one(expression)) == is_import


def test__get_common_table_expressions__len_ne_1():
    """
    Test that ``_get_common_table_expressions`` parses the CTEs out of the
    model.
    """
    with pytest.raises(ValueError):
        models._get_common_table_expressions(r"SELECT * FROM TABLE_1; SELECT * FROM TABLE_2")


def test__get_common_table_expressions__no_ctes():
    """
    Test that ``_get_common_table_expressions`` returns an empty dictionary
    when there are no CTEs.
    """
    sql = r"SELECT * FROM TABLE"
    assert models._get_common_table_expressions(sql) == {}


def test__get_common_table_expressions(compiled_sql: str):
    """
    Test that ``_get_common_table_expressions`` parses the CTEs out of the
    model.
    """
    sql = compiled_sql
    expected = {
        "a": sqlglot.parse_one("SELECT * FROM TABLE_1"),
        "b": sqlglot.parse_one("SELECT * FROM TABLE_2"),
        "c": sqlglot.parse_one("SELECT SUM(A) FROM a"),
        "d": sqlglot.parse_one("SELECT SUM(B) FROM b"),
        "final": sqlglot.parse_one("SELECT * FROM c UNION SELECT * FROM d"),
    }

    assert models._get_common_table_expressions(sql) == expected


def test__parse_models_and_ctes__file_not_found(dbt_config: DbtConfig):
    """
    Test that ``parse_models_and_ctes`` raises a ``FileNotFoundError`` when
    the project directory does not exist.
    """
    with pytest.raises(FileNotFoundError):
        models.parse_models_and_ctes(dbt_config)


def test__parse_models_and_ctes(dbt_config: DbtConfig, compiled_sql: str):
    """
    Test that ``parse_models_and_ctes`` parses the models and CTEs out of
    the project.
    """
    model_root = dbt_config.model_paths[0]
    compiled_root = dbt_config.compiled_paths[0]
    model_root.mkdir(exist_ok=True), compiled_root.mkdir(exist_ok=True)

    (model_root / "model_1.sql").touch(exist_ok=True)
    (compiled_model := compiled_root / "model_1.sql").touch(exist_ok=True)
    compiled_model.write_text(compiled_sql)

    expected = [
        models.Model(model_root / "model_1.sql").parse_ctes(model_root=model_root, compiled_root=compiled_root),
    ]

    assert models.parse_models_and_ctes(dbt_config) == expected

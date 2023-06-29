"""
Generate the coverage metric for the dbt unit tests.
"""
import pathlib
import sqlite3

from code_coverage.dbt import DbtConfig, parse_dbt_unit_tests, parse_models_and_ctes


def compute_test_coverage(project_dir: pathlib.Path) -> float:
    """
    Compute the code coverage for the dbt unit tests.

    This returns the metric as a percentage: that is, the percentage 10%
    would be returned as ``10.0`` rather than ``0.1``.

    :return: The code coverage for the dbt unit tests as a percentage.
    """
    dbt_config = DbtConfig.from_root(project_dir)

    models = parse_models_and_ctes(dbt_config)
    tests = parse_dbt_unit_tests(dbt_config.test_paths)

    if not models or not tests:
        return 0.0

    model_rows = [(model.name, cte.name, cte.type) for model in models for cte in model.ctes]
    test_rows = [(test.model_name, test.cte_name) for test in tests]

    with sqlite3.connect(":memory:") as conn:
        conn.executescript(pathlib.Path("code_coverage/coverage.sql").read_text())

        conn.executemany("""INSERT INTO models VALUES (?, ?, ?)""", model_rows)
        conn.executemany("""INSERT INTO tests VALUES (?, ?)""", test_rows)

        coverage = conn.execute("""SELECT 100.0 * SUM(coverage_flag) / COUNT(*) FROM coverage_flags""")

    return coverage.fetchone()[0]

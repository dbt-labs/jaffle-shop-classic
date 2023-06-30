"""
Generate the coverage metric for the dbt unit tests.
"""
from __future__ import annotations

import dataclasses
import pathlib
import sqlite3

from code_coverage.dbt import DbtConfig, parse_dbt_unit_tests, parse_models_and_ctes


@dataclasses.dataclass
class CoverageRow:
    """
    Quick and dirty dataclass to facilitate printing the coverage report.
    """

    model_name: str
    ctes: int
    miss: int
    cover: float
    missing: str

    @classmethod
    def from_tuple(cls, row: tuple[str, int, int, float, str]) -> CoverageRow:
        """
        Create a ``CoverageRow`` from a tuple.
        """
        return cls(*row)

    def __str__(self) -> str:
        """
        Return a string representation of the coverage row.
        """
        return f"{self.model_name:<25} {self.ctes:>6} {self.miss:>6} {self.cover:>8.1f}%   {self.missing or ''}"


def _print_coverage_report(coverage_rows: list[CoverageRow]) -> None:
    """
    Print the coverage report.

    This is quick and dirty as it's just MVP.
    """
    print("Model Name                  CTEs   Miss     Cover   Missing")
    print("-----------------------------------------------------------")
    for row in coverage_rows[:-1]:
        print(row)
    print("-----------------------------------------------------------")
    print(coverage_rows[-1])


def _compute(
    model_rows: list[tuple[str, str, str]],
    test_rows: list[tuple[str, str]],
    cov_report: bool,
) -> float:
    """
    Compute the code coverage for the dbt unit tests.
    """
    with sqlite3.connect(":memory:") as conn:
        conn.executescript(pathlib.Path("code_coverage/coverage.sql").read_text())

        conn.executemany("""INSERT INTO models VALUES (?, ?, ?)""", model_rows)
        conn.executemany("""INSERT INTO tests VALUES (?, ?)""", test_rows)

        coverage_result = conn.execute("""SELECT 100.0 * SUM(coverage_flag) / COUNT(*) FROM coverage_flags""")
        coverage = coverage_result.fetchone()[0]

        if cov_report:
            coverage_report = conn.execute("""SELECT model_name, ctes, miss, cover, missing FROM coverage_report""")
            _print_coverage_report([CoverageRow.from_tuple(row) for row in coverage_report.fetchall()])

    return coverage


def compute_test_coverage(
    project_dir: pathlib.Path,
    cov_report: bool,
    dbt_config: DbtConfig | None = None,
) -> float:
    """
    Compute the code coverage for the dbt unit tests.

    This returns the metric as a percentage: that is, the percentage 10%
    would be returned as ``10.0`` rather than ``0.1``.

    :return: The code coverage for the dbt unit tests as a percentage.
    """
    dbt_config = dbt_config or DbtConfig.from_root(project_dir)

    models = parse_models_and_ctes(dbt_config)
    tests = parse_dbt_unit_tests(dbt_config.test_paths)

    if not models or not tests:
        return 0.0

    model_rows = [(model.name, cte.name, cte.type) for model in models for cte in model.ctes]
    test_rows = [(test.model_name, test.cte_name) for test in tests]

    return _compute(
        model_rows=model_rows,
        test_rows=test_rows,
        cov_report=cov_report,
    )

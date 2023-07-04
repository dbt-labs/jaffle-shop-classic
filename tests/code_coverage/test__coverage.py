"""
Test the ``code_coverage/coverage.py`` module.
"""
import pathlib

import pytest

import code_coverage.coverage as coverage
from code_coverage.dbt import DbtConfig

CoverageRecord = tuple[str, int, int, float, str]


@pytest.fixture
def coverage_record() -> CoverageRecord:
    """
    Return a coverage record from a "database".
    """
    # model_name, ctes, miss, cover, missing
    return "model_name", 1, 2, 3.0, "missing"


def test__coverage_row__from_tuple(coverage_record: CoverageRecord):
    """
    Test that ``CoverageRow.from_tuple`` returns a ``CoverageRow`` instance.
    """
    coverage_row = coverage.CoverageRow.from_tuple(coverage_record)

    assert isinstance(coverage_row, coverage.CoverageRow)
    assert coverage_row.model_name == "model_name"
    assert coverage_row.ctes == 1
    assert coverage_row.miss == 2
    assert coverage_row.cover == 3.0
    assert coverage_row.missing == "missing"


def test__coverage_row__str(coverage_record: CoverageRecord):
    """
    Test the string representation of a ``CoverageRow``.
    """
    coverage_row = coverage.CoverageRow.from_tuple(coverage_record)

    assert str(coverage_row) == "model_name                     1      2      3.0%   missing"


def test__print_coverage_report(coverage_record: CoverageRecord, capsys):
    """
    Test that ``_print_coverage_report`` prints the coverage report.
    """
    coverage_row = coverage.CoverageRow.from_tuple(coverage_record)
    coverage_total = coverage.CoverageRow.from_tuple(("TOTAL", 1, 2, 3.0, ""))
    coverage._print_coverage_report([coverage_row, coverage_total])

    captured = capsys.readouterr()
    assert captured.out == (
        "Model Name                  CTEs   Miss     Cover   Missing\n"
        "-----------------------------------------------------------\n"
        "model_name                     1      2      3.0%   missing\n"
        "-----------------------------------------------------------\n"
        "TOTAL                          1      2      3.0%   \n"
    )


def test__compute_test_coverage__no_models(tmp_path: pathlib.Path, dbt_config: DbtConfig):
    """
    Test that ``compute_test_coverage`` returns ``0.0`` if there are no
    models.
    """
    dbt_config.model_paths[0].mkdir(exist_ok=True, parents=True)
    dbt_config.compiled_paths[0].mkdir(exist_ok=True, parents=True)

    coverage_metric = coverage.compute_test_coverage(
        project_dir=tmp_path,
        dbt_config=dbt_config,
        cov_report=False,
    )

    assert coverage_metric == 0.0

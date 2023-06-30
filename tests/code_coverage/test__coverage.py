"""
Test the ``code_coverage/coverage.py`` module.
"""
import pathlib

import code_coverage.coverage as coverage
from code_coverage.dbt import DbtConfig


def test__compute_test_coverage__no_models(tmp_path: pathlib.Path, dbt_config: DbtConfig) -> None:
    """
    Test that ``compute_test_coverage`` returns ``0.0`` if there are no
    models.
    """
    dbt_config.model_paths[0].mkdir(exist_ok=True, parents=True)
    dbt_config.compiled_paths[0].mkdir(exist_ok=True, parents=True)

    coverage_metric = coverage.compute_test_coverage(
        project_dir=tmp_path,
        dbt_config=dbt_config,
    )

    assert coverage_metric == 0.0

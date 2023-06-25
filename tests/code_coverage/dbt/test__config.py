"""
Test the ``code_coverage/dbt/config.py`` module.
"""
import pathlib
import textwrap

import pytest

import code_coverage.dbt.config as config


@pytest.fixture
def dbt_config_root(tmp_path) -> pathlib.Path:
    """
    Create a mock ``dbt_project.yml`` file in a temp directory.
    """
    dbt_project = tmp_path / "dbt_project.yml"
    dbt_project.write_text(textwrap.dedent(
        """
        name: project_name
        model-paths: ["models"]
        test-paths: ["tests"]
        target-path: "target"
        """
    ))
    return tmp_path


def test__dbt_config__compiled_paths(dbt_config_root: pathlib.Path):
    """
    Test the ``DbtConfig`` dataclass.
    """
    dbt_config = config.DbtConfig.from_root(dbt_config_root)

    assert dbt_config.compiled_paths == [
        pathlib.Path("target/compiled/project_name/models")
    ]


def test__dbt_config__from_root(dbt_config_root: pathlib.Path):
    """
    Test the ``DbtConfig`` dataclass.
    """
    dbt_config = config.DbtConfig.from_root(dbt_config_root)

    assert dbt_config.name == "project_name"
    assert dbt_config.model_paths == [pathlib.Path("models")]
    assert dbt_config.test_paths == [pathlib.Path("tests")]
    assert dbt_config.target_path == pathlib.Path("target")

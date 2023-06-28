"""
The ``dbt_project.yml`` file exposed as a dataclass.
"""
from __future__ import annotations

import dataclasses
import pathlib

import yaml


@dataclasses.dataclass
class DbtConfig:
    name: str
    model_paths: list[pathlib.Path]
    test_paths: list[pathlib.Path]
    target_path: pathlib.Path
    compiled_paths: list[pathlib.Path] = dataclasses.field(init=False)

    def __post_init__(self):
        self.compiled_paths = [
            self.target_path / "compiled" / self.name / model_path
            for model_path in self.model_paths
        ]

    @classmethod
    def from_root(cls, dbt_project_root: pathlib.Path) -> DbtConfig:
        """
        Construct a DbtConfig from the dbt project root.

        :param dbt_project_root: The directory where the ``dbt_project.yml``
            file lives.

        :return: A DbtConfig.
        """
        with (dbt_project_root / "dbt_project.yml").open() as config_yml:
            config = yaml.safe_load(config_yml)

        return DbtConfig(
            name=config["name"],
            model_paths=[pathlib.Path(p) for p in config["model-paths"]],
            test_paths=[pathlib.Path(p) for p in config["test-paths"]],
            target_path=pathlib.Path(config["target-path"]),
        )

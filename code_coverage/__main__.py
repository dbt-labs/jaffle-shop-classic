"""
Generate the code coverage for the dbt project.
"""
from __future__ import annotations

import dataclasses
import pathlib
import pprint
import yaml

import jinja2
import typer

from code_coverage.badge import generate_badge
from code_coverage.parser.dbt_model_parser import walk_the_models
from code_coverage.parser.unit_test_parser import get_test_files, get_all_test_cases


@dataclasses.dataclass
class DbtConfig:
    name: str
    model_paths: list[pathlib.Path]
    test_paths: list[pathlib.Path]
    target_path: pathlib.Path
    compiled_path: pathlib.Path = dataclasses.field(init=False)

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

    def __post_init__(self):
        self.compiled_path = self.target_path / "compiled" / self.name / self.model_paths[0]


def main(
    project_dir: str = ".",
    badge_path: str = "dbt-coverage.svg",
) -> None:
    """
    Generate the code coverage for the dbt project.

    :param project_dir: The directory containing the ``dbt_project.yml``
        file.
    :param badge_path: The file path to generate the badge to.
    """
    dbt_project_root = pathlib.Path(project_dir)
    dbt_config = DbtConfig.from_root(dbt_project_root)

    print(dbt_config)

    walk_the_models(dbt_config.model_paths[0])

    env = jinja2.Environment()
    files = [
        file for dir_ in dbt_config.test_paths for file in get_test_files(env, dir_)
    ]
    cases = get_all_test_cases(env, files)

    pprint.pprint(cases)

    if badge_path:
        generate_badge(pathlib.Path(badge_path).resolve(), 75.12)


if __name__ == "__main__":
    typer.run(main)

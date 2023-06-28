"""
Generate the code coverage for the dbt project.
"""
from __future__ import annotations

import pathlib
import pprint
import subprocess

import jinja2
import typer

from code_coverage.badge import generate_badge
from code_coverage.dbt import (
    DbtConfig,
    get_all_test_cases,
    get_test_files,
    parse_models_and_ctes,
)


def main(
    project_dir: str = ".",
    badge_path: str = "dbt-coverage.svg",
    compile_dbt: bool = False,
) -> None:
    """
    Generate the code coverage for the dbt project.

    :param project_dir: The directory containing the ``dbt_project.yml``
        file.
    :param badge_path: The file path to generate the badge to.
    :param compile_dbt: Whether to compile the dbt project before generating
        the code coverage.
    """
    dbt_project_root = pathlib.Path(project_dir)
    dbt_config = DbtConfig.from_root(dbt_project_root)

    print(dbt_config)

    if compile_dbt:
        subprocess.run(["dbt", "compile", f"--project-dir={project_dir}"])

    models = parse_models_and_ctes(dbt_config)
    for model in models:
        print(model.name)
        [print(f"\t{cte}") for cte in model.ctes]

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

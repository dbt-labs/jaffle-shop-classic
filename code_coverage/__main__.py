"""
Generate the code coverage for the dbt project.
"""
from __future__ import annotations

import pathlib
import subprocess

import typer

from code_coverage.badge import generate_badge
from code_coverage.coverage import compute_test_coverage


def main(
    project_dir: str = ".",
    badge_path: str = "coverage-dbt.svg",
    compile_dbt: bool = False,
    cov_report: bool = False,
) -> None:
    """
    Generate the code coverage for the dbt project and write it (as an SVG)
    to ``badge_path``.

    :param project_dir: The directory containing the ``dbt_project.yml``
        file.
    :param badge_path: The file path to generate the badge to.
    :param compile_dbt: Whether to compile the dbt project before generating
        the code coverage, defaults to ``False``.
    :param cov_report: Whether to print the coverage report to stdout,
        defaults to ``False``.
    """
    if compile_dbt:
        subprocess.run(["dbt", "compile", f"--project-dir={project_dir}"])

    coverage_metric = compute_test_coverage(
        project_dir=pathlib.Path(project_dir),
        cov_report=cov_report,
    )
    generate_badge(
        badge_path=pathlib.Path(badge_path).resolve(),
        coverage=coverage_metric,
    )


if __name__ == "__main__":
    typer.run(main)

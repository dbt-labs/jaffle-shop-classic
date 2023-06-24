from bisect import bisect
from dataclasses import dataclass
from math import floor
from pathlib import Path
import typing as t
import yaml

from jinja2 import Environment
import pybadges
import typer

from .dbt_model_parser import walk_the_models
from .unit_test_parser import get_test_files, get_all_test_cases

# Borrowed from https://docs.gitlab.com/ee/user/project/badges.html
badge_colors = {
    0: "#9f9f9f",
    75: "#e05d44",
    90: "#dfb317",
    95: "#a3c51c",
    100: "#4c1",
    1000: "#4c1"
}


def generate_badge(badge_path: Path, coverage: float) -> None:
    bounds = list(badge_colors.keys())
    key = bounds[bisect(bounds, floor(coverage)) - 1]
    color = badge_colors[key]
    svg = pybadges.badge(left_text="coverage", right_text=f"{round(coverage, 2)}%", right_color=color)

    with badge_path.open("w") as f:
        f.write(svg)


@dataclass
class DbtConfig:
    name: str
    model_paths: t.List[Path]
    test_paths: t.List[Path]


def get_dbt_config(dbt_project_root: Path) -> DbtConfig:
    with (dbt_project_root / "dbt_project.yml").open() as config_yml:
        config = yaml.safe_load(config_yml)

    return DbtConfig(
        name=config['name'],
        model_paths=[Path(p) for p in config['model-paths']],
        test_paths=[Path(p) for p in config['test-paths']],
    )


def main(dbt_project_root: t.Optional[Path] = Path("."), badge: t.Optional[Path] = None):
    dbt_config = get_dbt_config(dbt_project_root)

    env = Environment()
    files = [
        file
        for dir in dbt_config.test_paths
        for file in get_test_files(env, dir)
    ]
    cases = get_all_test_cases(env, files)

    from pprint import pprint
    pprint(cases)

    walk_the_models(dbt_config.model_paths[0])

    if badge:
        generate_badge(badge, 75.12)


if __name__ == "__main__":
    typer.run(main)

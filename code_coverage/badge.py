"""
Generate a code coverage badge.

Borrowed from:

- https://docs.gitlab.com/ee/user/project/badges.html
"""
import bisect
import math
import pathlib

import pybadges


BADGE_COLOURS: dict[int, str] = {
    0: "#9f9f9f",
    75: "#e05d44",
    90: "#dfb317",
    95: "#a3c51c",
    100: "#4c1",
    1000: "#4c1",
}


def generate_badge(badge_path: pathlib.Path, coverage: float) -> None:
    """
    Generate a code coverage badge.

    :param badge_path: The path to the badge.
    :param coverage: The code coverage percentage.
    """
    bounds = list(BADGE_COLOURS.keys())
    key = bounds[-1 + bisect.bisect(bounds, math.floor(coverage))]
    svg = pybadges.badge(
        left_text="dbt-coverage",
        right_text=f"{round(coverage, 2)}%",
        right_color=BADGE_COLOURS[key],
    )

    with badge_path.open("w") as f:
        f.write(svg)

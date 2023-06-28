"""
Test the ``code_coverage/badge.py`` module.
"""
import pathlib

import pytest

import code_coverage.badge as badge


def test__generate_badge(tmp_path: pathlib.Path) -> None:
    """
    Test that the badge is generated correctly.

    This is just a simple test to ensure that the badge is created, but does
    not test the contents of the badge.
    """
    badge_path = tmp_path / pathlib.Path("badge.svg")
    badge.generate_badge(
        badge_path=badge_path,
        coverage=0,
    )

    assert badge_path.exists()


def test__generate_badge__not_a_file() -> None:
    """
    Test that ``TypeError`` is raised if the badge path is not a file.
    """
    with pytest.raises(TypeError):
        badge.generate_badge(
            badge_path=pathlib.Path("."),
            coverage=100,
        )

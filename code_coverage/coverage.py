"""
Generate the coverage metric for the dbt unit tests.
"""
import pathlib
import pprint

from code_coverage.dbt import DbtConfig, parse_dbt_unit_tests, parse_models_and_ctes


def compute_test_coverage(project_dir: pathlib.Path) -> float:
    """
    Compute the code coverage for the dbt unit tests.
    :return:
    """
    dbt_config = DbtConfig.from_root(project_dir)
    print(dbt_config)

    models = parse_models_and_ctes(dbt_config)
    for model in models:
        print(model.name)
        [print(f"\t{cte}") for cte in model.ctes]

    cases = parse_dbt_unit_tests(dbt_config.test_paths)
    pprint.pprint(cases)

    # Now join them!

import pytest
from .lint_models import (
    dbt_ls_modified_paths_output,
    get_modified_lintable_paths,
    build_sqlfluff_command,
    run_sqlfluff_on_modified_paths
)

@pytest.fixture
def subprocess(mocker):
    return mocker.patch("testing.lint_models.subprocess")

@pytest.fixture
def args_lint():
    return {}

@pytest.fixture
def args_fix():
    return {'fix': 'True'}

@pytest.fixture
def args_ci():
    return {"ci_profile": "testing", "ci_config_path": ".sqlfluffci"}

@pytest.fixture
def output_lines():
    return [
        'blah.yml',
        'core/models/consumer/staging/src_consumer/stg_data_accountapplication.sql',
        'core/models/consumer/staging/src_consumer/stg_django_content_type.sql'
    ]

@pytest.fixture
def modified_lintable_paths():
    return ["core/models/consumer/staging/src_consumer/stg_data_accountapplication.sql",
            "core/models/consumer/staging/src_consumer/stg_django_content_type.sql"
            ]

def test_dbt_ls_modified_paths_output_lint(args_lint, subprocess, output_lines):
    subprocess.run.return_value.stdout = b'blah.yml\ncore/models/consumer/staging/src_consumer/stg_data_accountapplication.sql\ncore/models/consumer/staging/src_consumer/stg_django_content_type.sql'
    output = dbt_ls_modified_paths_output(args_lint)
    assert output == output_lines

def test_dbt_ls_modified_paths_output_ci(args_ci, subprocess, output_lines):
    subprocess.run.return_value.stdout = b'blah.yml\ncore/models/consumer/staging/src_consumer/stg_data_accountapplication.sql\ncore/models/consumer/staging/src_consumer/stg_django_content_type.sql'
    output = dbt_ls_modified_paths_output(args_ci)
    call_args = subprocess.run.call_args[0][0]
    assert '--profile' in call_args
    assert output == output_lines

def test_get_modified_lintable_paths(output_lines, modified_lintable_paths):
    output = get_modified_lintable_paths(output_lines)
    assert output == modified_lintable_paths

def test_build_sqlfluff_command_lint(args_lint, modified_lintable_paths):
    output = build_sqlfluff_command(args_lint, modified_lintable_paths)
    assert output == "sqlfluff lint core/models/consumer/staging/src_consumer/stg_data_accountapplication.sql core/models/consumer/staging/src_consumer/stg_django_content_type.sql"

def test_build_sqlfluff_command_fix(args_fix, modified_lintable_paths):
    output = build_sqlfluff_command(args_fix, modified_lintable_paths)
    assert output == "sqlfluff fix core/models/consumer/staging/src_consumer/stg_data_accountapplication.sql core/models/consumer/staging/src_consumer/stg_django_content_type.sql"

def test_build_sqlfluff_command_ci(args_ci, modified_lintable_paths):
    output = build_sqlfluff_command(args_ci, modified_lintable_paths)
    assert output == "sqlfluff lint --config .sqlfluffci core/models/consumer/staging/src_consumer/stg_data_accountapplication.sql core/models/consumer/staging/src_consumer/stg_django_content_type.sql"

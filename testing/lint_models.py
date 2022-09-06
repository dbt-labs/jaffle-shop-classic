import logging
import subprocess
from typing import Callable, List
import sys
import json
import re


logging.basicConfig(format="%(levelname)s:%(message)s", level=logging.INFO)
logging = logging.getLogger(__name__)

def get_args():
    # Get args passed to the script from the make file
    # so we can differentiate from local dev and CI.
    if len(sys.argv) > 1:
        args = json.loads(sys.argv[1])
    else:
        args = {}
    return args

def dbt_ls_modified_paths_output(
        args: dict
) -> List[str]:
    if 'docker' in args:
        ls_command = ["./dbt_local"]
    else:
        ls_command = []

    ls_command.extend(["dbt",
                  "ls",
                  "--select",
                  "state:modified",
                  "--state",
                  "production_target/",
                  "--output",
                  "path",
                  ])
    # Add a profile argument for when the script is being run in CI
    if 'ci_profile' in args:
        ls_command.extend(["--profile", f"{args['ci_profile']}"])
    print(ls_command)
    ls_output = subprocess.run(ls_command, capture_output=True)
    ls_output_list = ls_output.stdout.decode("utf-8").split("\n")
    print(ls_output_list)
    return ls_output_list

def get_modified_lintable_paths(output_lines: List
) -> List[str]:
    changed_model_paths = re.findall(r'[\/\w-]+.sql', ' '.join(output_lines))
    if len(changed_model_paths) > 0:
        logging.info("Found modified paths...proceeding.")
        return changed_model_paths
    else:
        logging.info("No modified paths found. Exiting.")

def build_sqlfluff_command(
        args: dict,
        lintable_paths: List
):
    # Specify lint or fix command
    if 'docker' in args:
        sqlfluff_command = "./dbt_local "
    else:
        sqlfluff_command = ''
    if 'fix' in args:
        sqlfluff_command = sqlfluff_command + "sqlfluff fix"
    else:
        sqlfluff_command = sqlfluff_command + "sqlfluff lint"
    # Add additional ci config when run in CI
    if 'ci_config_path' in args:
        sqlfluff_command = sqlfluff_command + ' --config ' + args['ci_config_path']
    sqlfluff_command = sqlfluff_command + ' ' + ' '.join(lintable_paths)
    return sqlfluff_command

def run_sqlfluff_on_modified_paths():
    args = get_args()
    output_lines = dbt_ls_modified_paths_output(args)
    lintable_paths = get_modified_lintable_paths(output_lines)
    logging.info(lintable_paths)
    if lintable_paths:
        sqlfluff_command = build_sqlfluff_command(args, lintable_paths)
        logging.info(f'Executing: "{sqlfluff_command}"')
        res = subprocess.run(
            sqlfluff_command, shell=True
        )
        if res.returncode != 0:
            raise Exception(f"Linting failed.")
    else:
        logging.info('No lintable paths found.')



if __name__ == "__main__":
    run_sqlfluff_on_modified_paths()

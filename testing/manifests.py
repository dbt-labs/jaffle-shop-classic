import os
import base64
import json
import logging
import requests
import argparse

logging.basicConfig(format="%(levelname)s:%(message)s", level=logging.INFO)
logging = logging.getLogger(__name__)


def delete_previous_merged_build_number(project, circle_ci_token_b64):
    previous_merged_build_number = get_merged_build_number(project, circle_ci_token_b64)
    url = f"https://circleci.com/api/v2/project/gh/octoenergy/datalake-models/envvar/{project}_CURRENT_MERGED_BUILD_NUMBER_{previous_merged_build_number}"
    headers = {"content-type": "application/json",
               "authorization": f"Basic '{circle_ci_token_b64}'"}
    response = requests.request("DELETE", url, headers=headers)
    if response.status_code == 200:
        logging.info(f"Deleted previous build number: {previous_merged_build_number}")
        logging.info(response.json())
        return response.json()
    else:
        logging.info("Failed to find previous build number. Continuing.")
        logging.info(response.json())
        return response.json()


def post_merged_build_number(project, circle_ci_token_b64, circle_build_number):
    url = "https://circleci.com/api/v2/project/gh/octoenergy/datalake-models/envvar"
    headers = {"content-type": "application/json",
               "authorization": f"Basic '{circle_ci_token_b64}'"}
    data = {"name": f"{project}_CURRENT_MERGED_BUILD_NUMBER_{circle_build_number}",
            "value": "a_random_value_of_no_importance"}
    response = requests.post(url, headers=headers, json=data)
    if response.status_code == 201:
        logging.info("Posted new merged build number.")
        logging.info(response.json())
        delete_previous_merged_build_number_result = delete_previous_merged_build_number(project, circle_ci_token_b64)
        return response.json()
    else:
        raise Exception(response, f"Failed to post merged build number as env var.")


def get_merged_build_number(project, circle_ci_token_b64):
    url = "https://circleci.com/api/v2/project/gh/octoenergy/datalake-models/envvar"
    headers = {"content-type": "application/json",
               "authorization": f"Basic '{circle_ci_token_b64}'"}
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        env_vars = response.json()['items']
        project_build_number_string = [env_var['name'] for env_var in env_vars if
                                       f'{project}_CURRENT_MERGED_BUILD_NUMBER_' in env_var[
                                           'name']][0]
        project_build_number = int(
            project_build_number_string.replace(f'{project}_CURRENT_MERGED_BUILD_NUMBER_', ''))
        logging.info(f'Found current merged build number: {project_build_number}')
        return project_build_number
    else:
        raise Exception(
            response.status_code,
            response.json(),
            f"Failed to find merged build number."
        )


def get_production_manifest_url(project, circle_ci_token_b64):
    project_build_number = get_merged_build_number(project, circle_ci_token_b64)
    url = f"https://circleci.com/api/v2/project/github/octoenergy/datalake-models/{project_build_number}/artifacts"
    headers = {"content-type": "application/json",
               "authorization": f"Basic '{circle_ci_token_b64}'"}
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        logging.info(f'{response.status_code}: Found production manifest url.')
        manifest_url = response.json()['items'][0]['url']
        return manifest_url
    else:
        raise Exception(response, f"Failed to retrieve production manifest URL.")


def get_production_manifest(project, circle_ci_token_b64):
    artifact_url = get_production_manifest_url(project, circle_ci_token_b64)
    circle_ci_token = base64.b64decode(circle_ci_token_b64).decode('utf-8')
    headers = {
        "content-type": "application/json",
        "Circle-Token": f"{circle_ci_token}"
    }
    response = requests.get(artifact_url, headers=headers)
    if response.status_code == 200:
        production_manifest = response.json()
        logging.info(f'{response.status_code}: Retrieved production manifest.')
        return production_manifest
    else:
        raise Exception(response.status_code, response.json(), f"Failed to retrieve manifest.")


def save_production_manifest_to_state_dir(project, circle_ci_token_b64, state_directory):
    production_manifest = get_production_manifest(project, circle_ci_token_b64)
    if not os.path.isdir(state_directory):
        os.mkdir(state_directory)
    with open(f'{state_directory}/manifest.json', 'w') as f:
        json.dump(production_manifest, f)
    logging.info(f'Manifest saved to {state_directory}/manifest.json')


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        'command',
        help='get_production_manifest or post_build_number or save_production_manifest_to_state_dir',
        nargs='?',
        choices=(
            'post_merged_build_number',
            'get_merged_build_number',
            'save_production_manifest_to_state_dir'
        )
    )
    parser.add_argument("project", type=str, help="The project to get the manifest for")
    parser.add_argument("token", type=str, help="Base 64 encoded Circle Ci Token")
    parser.add_argument(
        "--circle_build_number",
        type=int,
        help="Circle build number to encode and post as an environment variable in CI"
    )
    parser.add_argument(
        "--state_directory",
        type=str,
        default='production_target',
        help="Directory in which to store the production manifest for comparison."
    )
    args = parser.parse_args()

    if not args.project:
        raise Exception(f"Must specify a project.")
    elif not args.token:
        raise Exception(f"Could not find Circle Ci Token environment variable.")
    else:
        pass

    if args.command == 'post_merged_build_number' and args.circle_build_number:
        post_merged_build_number(args.project, args.token, args.circle_build_number)
    elif args.command == 'get_merged_build_number':
        merged_build_number = get_merged_build_number(args.project, args.token)
        print(merged_build_number)
    elif args.command == 'save_production_manifest_to_state_dir':
        save_production_manifest_to_state_dir(args.project, args.token, args.state_directory)
    else:
        raise Exception('''
            Must specify one of:
            - post_merged_build_number and circle_build_number
            - get_merged_build_number
            - save_production_manifest_to_state_dir
            '''
                        )

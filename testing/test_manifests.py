import pytest
from .manifests import (
    delete_previous_merged_build_number,
    post_merged_build_number,
    get_merged_build_number,
    get_production_manifest_url,
    get_production_manifest,
    save_production_manifest_to_state_dir
)

@pytest.fixture()
def delete_previous_merged_build_number_response():
    return {'message': 'Environment variable deleted.'}


@pytest.fixture()
def post_merged_build_number_request_response():
    return {
        'name': 'oegb_CURRENT_MERGED_BUILD_NUMBER_45451',
        'value': 'xxxxance'
    }


@pytest.fixture()
def get_merged_build_number_get_request_response():
    return {
        'next_page_token': None,
        'items': [
            {'name': 'oegb_CURRENT_MERGED_BUILD_NUMBER_45451',
             'value': 'xxxxance'},
        ]
    }


@pytest.fixture()
def get_merged_build_number_expected():
    return 45451


@pytest.fixture()
def get_production_manifest_url_get_request_response():
    return {
        'next_page_token': None,
        'items': [
            {
                'path': 'production_target/manifest.json',
                'node_index': 0,
                'url': 'https://output.circle-artifacts.com/output/job/8ad304ab-b1f9-47e8-b57d-9f2073bad057/artifacts/0/production_target/manifest.json'
            }
        ]
    }


@pytest.fixture()
def get_production_manifest_url_expected():
    return 'https://output.circle-artifacts.com/output/job/8ad304ab-b1f9-47e8-b57d-9f2073bad057/artifacts/0/production_target/manifest.json'


@pytest.fixture()
def get_production_manifest_get_request_response():
    return {
        "metadata":
            {
                "dbt_schema_version": "some_data",
            }
    }

def test_delete_previous_merged_build_number(mocker, delete_previous_merged_build_number_response):
    mock_get_merged_build_number = mocker.patch("testing.manifests.get_merged_build_number",
                                                return_value=get_merged_build_number_expected)

    mock_requests = mocker.patch("requests.request")
    mock_requests.return_value.status_code = 200
    mock_requests.return_value.json.return_value = post_merged_build_number_request_response

    actual = delete_previous_merged_build_number('oegb', 'token')
    mock_requests.assert_called_once_with('DELETE',
        f"https://circleci.com/api/v2/project/gh/octoenergy/datalake-models/envvar/oegb_CURRENT_MERGED_BUILD_NUMBER_{get_merged_build_number_expected}",
        headers={
            'content-type': 'application/json',
            'authorization': "Basic 'token'"
        },

    )
    assert actual == post_merged_build_number_request_response

def test_post_merged_build_number(mocker, post_merged_build_number_request_response):
    mock_delete_previous_merged_build_number = mocker.patch("testing.manifests.delete_previous_merged_build_number",
                                                return_value=delete_previous_merged_build_number_response)

    mock_requests = mocker.patch("requests.post")
    mock_requests.return_value.status_code = 201
    mock_requests.return_value.json.return_value = post_merged_build_number_request_response

    actual = post_merged_build_number('oegb', 'token', 45451)
    mock_requests.assert_called_once_with(
        "https://circleci.com/api/v2/project/gh/octoenergy/datalake-models/envvar",
        headers={
            'content-type': 'application/json',
            'authorization': "Basic 'token'"
        },
        json={
            'name': 'oegb_CURRENT_MERGED_BUILD_NUMBER_45451',
            'value': 'a_random_value_of_no_importance'
        }
    )
    assert actual == post_merged_build_number_request_response


def test_get_merged_build_number(mocker, get_merged_build_number_get_request_response,
                                 get_merged_build_number_expected):
    mock_requests = mocker.patch("requests.get")
    mock_requests.return_value.status_code = 200
    mock_requests.return_value.json.return_value = get_merged_build_number_get_request_response

    actual = get_merged_build_number('oegb', 'token')
    mock_requests.assert_called_once_with(
        "https://circleci.com/api/v2/project/gh/octoenergy/datalake-models/envvar",
        headers={
            'content-type': 'application/json',
            'authorization': "Basic 'token'"
        }
    )
    assert actual == get_merged_build_number_expected


def test_get_production_manifest_url(mocker, get_production_manifest_url_get_request_response,
                                     get_production_manifest_url_expected,
                                     get_merged_build_number_expected):
    mock_get_merged_build_number = mocker.patch("testing.manifests.get_merged_build_number",
                                                return_value=get_merged_build_number_expected)

    mock_requests = mocker.patch("requests.get")
    mock_requests.return_value.status_code = 200
    mock_requests.return_value.json.return_value = get_production_manifest_url_get_request_response

    actual = get_production_manifest_url('oegb', 'token')
    mock_requests.assert_called_once_with(
        "https://circleci.com/api/v2/project/github/octoenergy/datalake-models/45451/artifacts",
        headers={
            'content-type': 'application/json',
            'authorization': "Basic 'token'"
        }
    )
    assert actual == get_production_manifest_url_expected


def test_get_production_manifest(mocker, get_production_manifest_get_request_response,
                                 get_production_manifest_url_expected):
    mock_get_production_manifest_url = mocker.patch(
        "testing.manifests.get_production_manifest_url",
        return_value=get_production_manifest_url_expected
    )

    mock_requests = mocker.patch("requests.get")
    mock_requests.return_value.status_code = 200
    mock_requests.return_value.json.return_value = get_production_manifest_get_request_response

    actual = get_production_manifest('oegb', b'dG9rZW4=')
    mock_requests.assert_called_once_with(
        get_production_manifest_url_expected,
        headers={
            'content-type': 'application/json',
            "Circle-Token": "token"
        }
    )
    assert actual == get_production_manifest_get_request_response

# def test_save_production_manifest_to_state_dir():

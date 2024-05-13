import pytest
from copy import deepcopy
from sources.rest_api import rest_api_source
from .source_configs import VALID_CONFIGS, INVALID_CONFIGS


@pytest.mark.parametrize("expected_message, exception, invalid_config", INVALID_CONFIGS)
def test_invalid_configurations(expected_message, exception, invalid_config):
    with pytest.raises(exception, match=expected_message):
        rest_api_source(invalid_config)


@pytest.mark.parametrize("valid_config", VALID_CONFIGS)
def test_valid_configurations(valid_config):
    rest_api_source(valid_config)


@pytest.mark.parametrize(
    "config",
    [
        {
            "client": {"base_url": "https://api.example.com"},
            "resources": [
                "posts",
                {
                    "name": "post_comments",
                    "endpoint": {
                        "path": "posts/{post_id}/comments",
                        "params": {
                            "post_id": {
                                "type": "resolve",
                                "resource": "posts",
                                "field": "id",
                            },
                        },
                    },
                },
            ],
        },
        {
            "client": {"base_url": "https://api.example.com"},
            "resources": [
                {
                    "name": "posts",
                    "endpoint": {
                        "path": "posts",
                        "params": {
                            "limit": 100,
                            "since": {
                                "type": "incremental",
                                "cursor_path": "updated_at",
                                "initial_value": "2024-01-25T11:21:28Z",
                            },
                        },
                        "paginator": "json_links",
                    },
                },
            ],
        },
        {
            "client": {"base_url": "https://api.example.com"},
            "resources": [
                {
                    "name": "posts",
                    "endpoint": {
                        "path": "posts",
                        "params": {
                            "limit": 100,
                        },
                        "paginator": "json_links",
                        "incremental": {
                            "start_param": "since",
                            "end_param": "until",
                            "cursor_path": "updated_at",
                            "initial_value": "2024-01-25T11:21:28Z",
                        },
                    },
                },
            ],
        },
    ],
)
def test_configurations_dict_is_not_modified_in_place(config):
    config_copy = deepcopy(config)
    rest_api_source(config)
    assert config_copy == config

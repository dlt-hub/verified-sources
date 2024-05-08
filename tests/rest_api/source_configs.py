from collections import namedtuple
from dlt.common.exceptions import DictValidationException
from dlt.sources.helpers.rest_client.paginators import SinglePagePaginator
from dlt.sources.helpers.rest_client.auth import HttpBasicAuth


ConfigTest = namedtuple("ConfigTest", ["expected_message", "exception", "config"])

INVALID_CONFIGS = [
    ConfigTest(
        expected_message="following required fields are missing {'resources'}",
        exception=DictValidationException,
        config={"client": {"base_url": ""}},
    ),
    ConfigTest(
        expected_message="following required fields are missing {'client'}",
        exception=DictValidationException,
        config={"resources": []},
    ),
    ConfigTest(
        expected_message="In path ./client: following fields are unexpected {'invalid_key'}",
        exception=DictValidationException,
        config={
            "client": {
                "base_url": "https://api.example.com",
                "invalid_key": "value",
            },
            "resources": ["posts"],
        },
    ),
    ConfigTest(
        expected_message="Invalid paginator: invalid_paginator. Available options: json_links, header_links, auto, single_page",
        exception=ValueError,
        config={
            "client": {
                "base_url": "https://api.example.com",
                "paginator": "invalid_paginator",
            },
            "resources": ["posts"],
        },
    ),
]


VALID_CONFIGS = [
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
                        "limit": 1,
                    },
                    "paginator": SinglePagePaginator(),
                },
            },
        ],
    },
    {
        "client": {
            "base_url": "https://example.com",
            "paginator": "header_links",
            "auth": HttpBasicAuth("my-secret", ""),
        },
        "resources": ["users"],
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
    {
        "client": {
            "base_url": "https://api.example.com",
            "headers": {
                "X-Test-Header": "test42",
            },
        },
        "resources": ["users"],
    },
]

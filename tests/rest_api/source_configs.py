from collections import namedtuple
from typing import List
from dlt.common.exceptions import DictValidationException
from dlt.sources.helpers.rest_client.paginators import SinglePagePaginator
from dlt.sources.helpers.rest_client.auth import HttpBasicAuth

from sources.rest_api.typing import PaginatorTypeConfig


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
        expected_message="field 'paginator' with value invalid_paginator is not one of:",
        exception=DictValidationException,
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
                    "paginator": "json_response",
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
            "paginator": "header_link",
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
                    "paginator": "json_response",
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
                    "paginator": "json_response",
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
]


# NOTE: leaves some parameters as defaults to test them
PAGINATOR_TYPE_CONFIGS: List[PaginatorTypeConfig] = [
    {"type": "auto"},
    {"type": "single_page"},
    {"type": "page_number", "initial_page": 10, "total_path": "response.pages"},
    {"type": "offset", "limit": 100, "maximum_offset": 1000},
    {"type": "header_link", "links_next_key": "next_page"},
    {"type": "json_response", "next_url_path": "response.nex_page_link"},
    {"type": "cursor", "cursor_param": "cursor"},
]

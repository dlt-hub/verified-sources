from collections import namedtuple
from dlt.common.exceptions import DictValidationException
from sources.rest_api.paginators import SinglePagePaginator


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
        expected_message="In ./client: following fields are unexpected {'invalid_key'}",
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
]

from typing import Callable, List

import dlt
import pytest
from dlt.sources.helpers.rest_client.paginators import SinglePagePaginator
from dlt.extract.source import DltResource
from sources.rest_api import RESTAPIConfig, rest_api_source
from tests.utils import ALL_DESTINATIONS, assert_load_info, load_table_counts


def _make_pipeline(destination_name: str):
    return dlt.pipeline(
        pipeline_name="rest_api",
        destination=destination_name,
        dataset_name="rest_api_data",
        full_refresh=True,
    )


def test_rest_api_source_filtered(mock_api_server) -> None:
    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://api.example.com",
        },
        "resources": [
            {
                "name": "posts",
                "endpoint": "posts",
                "processing_steps": [
                    {"filter": lambda x: x["id"] == 1},
                ],
            },
        ],
    }
    mock_source = rest_api_source(config)

    data = list(mock_source.with_resources("posts"))
    assert len(data) == 1
    assert data[0]["title"] == "Post 1"


def test_rest_api_source_exclude_columns(mock_api_server) -> None:

    def exclude_columns(columns: List[str]) -> Callable:
        def pop_columns(resource: DltResource) -> DltResource:
            for col in columns:
                resource.pop(col)
            return resource

        return pop_columns

    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://api.example.com",
        },
        "resources": [
            {
                "name": "posts",
                "endpoint": "posts",
                "processing_steps": [
                    {
                        "map": exclude_columns(["title"]),
                    },
                ],
            },
        ],
    }
    mock_source = rest_api_source(config)

    data = list(mock_source.with_resources("posts"))

    assert all("title" not in record for record in data)


def test_rest_api_source_anonymize_columns(mock_api_server) -> None:

    def anonymize_columns(columns: List[str]) -> Callable:
        def empty_columns(resource: DltResource) -> DltResource:
            for col in columns:
                resource[col] = "dummy"
            return resource

        return empty_columns

    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://api.example.com",
        },
        "resources": [
            {
                "name": "posts",
                "endpoint": "posts",
                "processing_steps": [
                    {
                        "map": anonymize_columns(["title"]),
                    },
                ],
            },
        ],
    }
    mock_source = rest_api_source(config)

    data = list(mock_source.with_resources("posts"))

    assert all(record["title"] == "dummy" for record in data)


def test_rest_api_source_map(mock_api_server) -> None:

    def lower_title(row):
        row["title"] = row["title"].lower()
        return row

    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://api.example.com",
        },
        "resources": [
            {
                "name": "posts",
                "endpoint": "posts",
                "processing_steps": [
                    {"map": lower_title},
                ],
            },
        ],
    }
    mock_source = rest_api_source(config)

    data = list(mock_source.with_resources("posts"))

    assert all(record["title"].startswith("post ") for record in data)


def test_rest_api_source_filter_and_map(mock_api_server) -> None:

    def id_by_10(row):
        row["id"] = row["id"] * 10
        return row

    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://api.example.com",
        },
        "resources": [
            {
                "name": "posts",
                "endpoint": "posts",
                "processing_steps": [
                    {"map": id_by_10},
                    {"filter": lambda x: x["id"] == 10},
                ],
            },
            {
                "name": "posts_2",
                "endpoint": "posts",
                "processing_steps": [
                    {"filter": lambda x: x["id"] == 10},
                    {"map": id_by_10},
                ],
            },
        ],
    }
    mock_source = rest_api_source(config)

    data = list(mock_source.with_resources("posts"))
    assert len(data) == 1
    assert data[0]["title"] == "Post 1"

    data = list(mock_source.with_resources("posts_2"))
    assert len(data) == 1
    assert data[0]["id"] == 100
    assert data[0]["title"] == "Post 10"


def test_rest_api_source_filtered_child(mock_api_server) -> None:
    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://api.example.com",
        },
        "resources": [
            {
                "name": "posts",
                "endpoint": "posts",
                "processing_steps": [
                    {"filter": lambda x: x["id"] in (1, 2)},
                ],
            },
            {
                "name": "comments",
                "endpoint": {
                    "path": "/posts/{post_id}/comments",
                    "params": {
                        "post_id": {
                            "type": "resolve",
                            "resource": "posts",
                            "field": "id",
                        }
                    },
                },
                "processing_steps": [
                    {"filter": lambda x: x["id"] == 1},
                ],
            },
        ],
    }
    mock_source = rest_api_source(config)

    data = list(mock_source.with_resources("comments"))
    assert len(data) == 2
    # assert data[0]["title"] == "Post 1"


def test_rest_api_source_filtered_and_map_child(mock_api_server) -> None:

    def extend_body(row):
        row["body"] = f"{row['_posts_title']} - {row['body']}"
        return row

    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://api.example.com",
        },
        "resources": [
            {
                "name": "posts",
                "endpoint": "posts",
                "processing_steps": [
                    {"filter": lambda x: x["id"] in (1, 2)},
                ],
            },
            {
                "name": "comments",
                "endpoint": {
                    "path": "/posts/{post_id}/comments",
                    "params": {
                        "post_id": {
                            "type": "resolve",
                            "resource": "posts",
                            "field": "id",
                        }
                    },
                },
                "include_from_parent": ["title"],
                "processing_steps": [
                    {"map": extend_body},
                    {"filter": lambda x: x["body"].startswith("Post 2")},
                ],
            },
        ],
    }
    mock_source = rest_api_source(config)

    data = list(mock_source.with_resources("comments"))
    # assert len(data) == 1
    assert data[0]["body"] == "Post 2 - Comment 0 for post 2"

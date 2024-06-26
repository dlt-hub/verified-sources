import pytest

import dlt
from dlt.pipeline.exceptions import PipelineStepFailed
from dlt.sources.helpers.rest_client.paginators import BaseReferencePaginator

from tests.utils import assert_load_info, load_table_counts, assert_query_data

from sources.rest_api import rest_api_source
from sources.rest_api import (
    RESTAPIConfig,
    ClientConfig,
    EndpointResource,
    Endpoint,
)


def test_load_mock_api(mock_api_server):
    pipeline = dlt.pipeline(
        pipeline_name="rest_api_mock",
        destination="duckdb",
        dataset_name="rest_api_mock",
        full_refresh=True,
    )

    mock_source = rest_api_source(
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
                            }
                        },
                    },
                },
                {
                    "name": "post_details",
                    "endpoint": {
                        "path": "posts/{post_id}",
                        "params": {
                            "post_id": {
                                "type": "resolve",
                                "resource": "posts",
                                "field": "id",
                            }
                        },
                    },
                },
            ],
        }
    )

    load_info = pipeline.run(mock_source)
    print(load_info)
    assert_load_info(load_info)
    table_names = [t["name"] for t in pipeline.default_schema.data_tables()]
    table_counts = load_table_counts(pipeline, *table_names)

    assert table_counts.keys() == {"posts", "post_comments", "post_details"}

    assert table_counts["posts"] == 100
    assert table_counts["post_details"] == 100
    assert table_counts["post_comments"] == 5000

    with pipeline.sql_client() as client:
        posts_table = client.make_qualified_table_name("posts")
        posts_details_table = client.make_qualified_table_name("post_details")
        post_comments_table = client.make_qualified_table_name("post_comments")

    assert_query_data(
        pipeline,
        f"SELECT title FROM {posts_table} limit 5",
        [f"Post {i}" for i in range(5)],
    )

    assert_query_data(
        pipeline,
        f"SELECT body FROM {posts_details_table} limit 5",
        [f"Post body {i}" for i in range(5)],
    )

    assert_query_data(
        pipeline,
        f"SELECT body FROM {post_comments_table} limit 5",
        [f"Comment {i} for post 0" for i in range(5)],
    )


def test_ignoring_endpoint_returning_404(mock_api_server):
    mock_source = rest_api_source(
        {
            "client": {"base_url": "https://api.example.com"},
            "resources": [
                "posts",
                {
                    "name": "post_details",
                    "endpoint": {
                        "path": "posts/{post_id}/some_details_404",
                        "params": {
                            "post_id": {
                                "type": "resolve",
                                "resource": "posts",
                                "field": "id",
                            }
                        },
                        "response_actions": [
                            {
                                "status_code": 404,
                                "action": "ignore",
                            },
                        ],
                    },
                },
            ],
        }
    )

    res = list(mock_source.with_resources("posts", "post_details").add_limit(1))

    assert res[:5] == [
        {"id": 0, "body": "Post body 0"},
        {"id": 0, "title": "Post 0"},
        {"id": 1, "title": "Post 1"},
        {"id": 2, "title": "Post 2"},
        {"id": 3, "title": "Post 3"},
    ]


def test_source_with_post_request(mock_api_server):
    class JSONBodyPageCursorPaginator(BaseReferencePaginator):
        def update_state(self, response):
            self._next_reference = response.json().get("next_page")

        def update_request(self, request):
            if request.json is None:
                request.json = {}

            request.json["page"] = self._next_reference

    mock_source = rest_api_source(
        {
            "client": {"base_url": "https://api.example.com"},
            "resources": [
                {
                    "name": "search_posts",
                    "endpoint": {
                        "path": "/posts/search",
                        "method": "POST",
                        "json": {"ids_greater_than": 50},
                        "paginator": JSONBodyPageCursorPaginator(),
                    },
                }
            ],
        }
    )

    res = list(mock_source.with_resources("search_posts"))

    for i in range(49):
        assert res[i] == {"id": 51 + i, "title": f"Post {51 + i}"}


def test_unauthorized_access_to_protected_endpoint(mock_api_server):
    pipeline = dlt.pipeline(
        pipeline_name="rest_api_mock",
        destination="duckdb",
        dataset_name="rest_api_mock",
        full_refresh=True,
    )

    mock_source = rest_api_source(
        {
            "client": {"base_url": "https://api.example.com"},
            "resources": [
                "/protected/posts/bearer-token-plain-text-error",
            ],
        }
    )

    # TODO: Check if it's specically a 401 error
    with pytest.raises(PipelineStepFailed):
        pipeline.run(mock_source)


def test_posts_under_results_key(mock_api_server):
    mock_source = rest_api_source(
        {
            "client": {"base_url": "https://api.example.com"},
            "resources": [
                {
                    "name": "posts",
                    "endpoint": {
                        "path": "posts_under_a_different_key",
                        "data_selector": "many-results",
                        "paginator": "json_response",
                    },
                },
            ],
        }
    )

    res = list(mock_source.with_resources("posts").add_limit(1))

    assert res[:5] == [
        {"id": 0, "title": "Post 0"},
        {"id": 1, "title": "Post 1"},
        {"id": 2, "title": "Post 2"},
        {"id": 3, "title": "Post 3"},
        {"id": 4, "title": "Post 4"},
    ]


def test_posts_without_key(mock_api_server):
    mock_source = rest_api_source(
        {
            "client": {
                "base_url": "https://api.example.com",
                "paginator": "header_link",
            },
            "resources": [
                {
                    "name": "posts_no_key",
                    "endpoint": {
                        "path": "posts_no_key",
                    },
                },
            ],
        }
    )

    res = list(mock_source.with_resources("posts_no_key").add_limit(1))

    assert res[:5] == [
        {"id": 0, "title": "Post 0"},
        {"id": 1, "title": "Post 1"},
        {"id": 2, "title": "Post 2"},
        {"id": 3, "title": "Post 3"},
        {"id": 4, "title": "Post 4"},
    ]


@pytest.mark.skip
def test_load_mock_api_typeddict_config(mock_api_server):
    pipeline = dlt.pipeline(
        pipeline_name="rest_api_mock",
        destination="duckdb",
        dataset_name="rest_api_mock",
        full_refresh=True,
    )

    mock_source = rest_api_source(
        RESTAPIConfig(
            client=ClientConfig(base_url="https://api.example.com"),
            resources=[
                "posts",
                EndpointResource(
                    name="post_comments",
                    endpoint=Endpoint(
                        path="posts/{post_id}/comments",
                        params={
                            "post_id": {
                                "type": "resolve",
                                "resource": "posts",
                                "field": "id",
                            }
                        },
                    ),
                ),
            ],
        )
    )

    load_info = pipeline.run(mock_source)
    print(load_info)
    assert_load_info(load_info)
    table_names = [t["name"] for t in pipeline.default_schema.data_tables()]
    table_counts = load_table_counts(pipeline, *table_names)

    assert table_counts.keys() == {"posts", "post_comments"}

    assert table_counts["posts"] == 100
    assert table_counts["post_comments"] == 5000


def test_response_action_on_status_code(mock_api_server, mocker):
    mock_response_hook = mocker.Mock()
    mock_source = rest_api_source(
        {
            "client": {"base_url": "https://api.example.com"},
            "resources": [
                {
                    "name": "post_details",
                    "endpoint": {
                        "path": "posts/1/some_details_404",
                        "response_actions": [
                            {
                                "status_code": 404,
                                "action": mock_response_hook,
                            },
                        ],
                    },
                },
            ],
        }
    )

    list(mock_source.with_resources("post_details").add_limit(1))

    mock_response_hook.assert_called_once()


def test_response_action_on_every_response(mock_api_server, mocker):
    def custom_hook(request, *args, **kwargs):
        return request

    mock_response_hook = mocker.Mock(side_effect=custom_hook)
    mock_source = rest_api_source(
        {
            "client": {"base_url": "https://api.example.com"},
            "resources": [
                {
                    "name": "posts",
                    "endpoint": {
                        "response_actions": [
                            mock_response_hook,
                        ],
                    },
                },
            ],
        }
    )

    list(mock_source.with_resources("posts").add_limit(1))

    mock_response_hook.assert_called_once()

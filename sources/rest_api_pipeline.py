import dlt
from rest_api import (
    rest_api_source,
    ResolveConfig as resolve_from,
    rest_api_resources_v2,
)
from rest_api import RESTClient, EndpointResource, Endpoint
from rest_api.auth import BearerTokenAuth


def load_github():
    pipeline = dlt.pipeline(
        pipeline_name="rest_api_github_v3",
        destination="duckdb",
        dataset_name="rest_api_data",
    )

    github_source = rest_api_source(
        {
            "client": {
                "base_url": "https://api.github.com/repos/dlt-hub/dlt/",
                "auth": {
                    "token": dlt.secrets["github_token"],
                },
            },
            # Default params for all resouces and their endpoints
            "resource_defaults": {
                "primary_key": "id",
                "write_disposition": "merge",
                "endpoint": {
                    "params": {
                        "per_page": 100,
                    },
                },
            },
            "resources": [
                # "pulls", <- This is both name and endpoint path
                # {
                #     "name": "pulls",
                #     "endpoint": "pulls",  # <- This is the endpoint path
                # }
                {
                    "name": "issues",
                    "endpoint": {
                        "path": "issues",
                        "params": {
                            "sort": "updated",
                            "direction": "desc",
                            "state": "open",
                            "since": {
                                "type": "incremental",
                                "cursor_path": "updated_at",
                                "initial_value": "2024-01-25T11:21:28Z",
                            },
                        },
                    },
                },
                {
                    "name": "issue_comments",
                    "endpoint": {
                        "path": "issues/{issue_number}/comments",
                        "params": {
                            "issue_number": {
                                "type": "resolve",
                                "resource": "issues",
                                "field": "number",
                            }
                        },
                    },
                    "include_from_parent": ["id"],
                },
            ],
        }
    )

    load_info = pipeline.run(github_source)
    print(load_info)


def load_github_legacy():
    pipeline = dlt.pipeline(
        pipeline_name="rest_api_github_v1",
        destination="duckdb",
        dataset_name="rest_api_data",
    )

    github_source = rest_api_source(
        {
            "client": {
                "base_url": "https://api.github.com/repos/dlt-hub/dlt/",
                # If you leave out the default_paginator, it will be inferred from the API:
                # "default_paginator": "header_links",
                "auth": {
                    "token": dlt.secrets["github_token"],
                },
            },
            "endpoints": {
                "issues/{issue_number}/comments": {
                    "params": {
                        "per_page": 100,
                        "issue_number": resolve_from("issues", "number"),
                    },
                    "resource": {
                        "primary_key": "id",
                    },
                },
                "issues": {
                    "params": {
                        "per_page": 100,
                        "sort": "updated",
                        "direction": "desc",
                        "state": "open",
                        "since": dlt.sources.incremental(
                            "updated_at", initial_value="2024-01-25T11:21:28Z"
                        ),
                    },
                    "resource": {
                        "primary_key": "id",
                        "write_disposition": "merge",
                    },
                },
            },
        }
    )

    load_info = pipeline.run(github_source)
    print(load_info)


def load_github_v2():
    pipeline = dlt.pipeline(
        pipeline_name="rest_api_github_v2",
        destination="duckdb",
        dataset_name="rest_api_data",
    )

    github_source = rest_api_resources_v2(
        RESTClient(
            base_url="https://api.github.com/repos/dlt-hub/dlt/",
            auth=BearerTokenAuth(dlt.secrets["github_token"]),
        ),
        EndpointResource(
            Endpoint(
                "issues",
                params={
                    "per_page": 100,
                    "sort": "updated",
                    "direction": "desc",
                    "state": "open",
                },
            ),
            name="issues",
            primary_key="id",
            write_disposition="merge",
        ),
        EndpointResource(
            Endpoint(
                "issues/{issue_number}/comments",
                params={
                    "per_page": 100,
                    "issue_number": resolve_from("issues", "number"),
                },
            ),
            primary_key="id",
        ),
    )

    load_info = pipeline.run(github_source)
    print(load_info)


def load_pokemon():
    pipeline = dlt.pipeline(
        pipeline_name="rest_api_pokemon",
        destination="duckdb",
        dataset_name="rest_api_data",
    )

    pokemon_source = rest_api_source(
        {
            "client": {
                "base_url": "https://pokeapi.co/api/v2/",
                # If you leave out the default_paginator, it will be inferred from the API:
                # default_paginator: "json_links",
            },
            "resource_defaults": {
                "endpoint": {
                    "params": {
                        "limit": 1000,
                    },
                },
            },
            "resources": [
                "pokemon",
                "berry",
                "location",
            ],
        }
    )

    load_info = pipeline.run(pokemon_source)
    print(load_info)


if __name__ == "__main__":
    load_github()
    load_pokemon()

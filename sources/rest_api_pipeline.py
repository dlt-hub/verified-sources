import dlt
from dlt.sources.helpers import requests
from rest_api import rest_api_source

#
# dlt Requests:
#


@dlt.resource(
    table_name="issues",
    write_disposition="merge",
    primary_key="id",
)
def get_issues(
    updated_at=dlt.sources.incremental(
        "updated_at", initial_value="1970-01-01T00:00:00Z"
    )
):
    url = (
        f"https://api.github.com/repos/dlt-hub/dlt/issues"
        f"?since={updated_at.last_value}&per_page=100"
        f"&sort=updated&directions=desc&state=open"
    )

    while True:
        response = requests.get(url)
        response.raise_for_status()
        yield response.json()

        # Get next page
        if "next" not in response.links:
            break
        url = response.links["next"]["url"]


#
# REST Source:
#


def load_github():
    pipeline = dlt.pipeline(
        pipeline_name="rest_api_github",
        destination="duckdb",
        dataset_name="rest_api_data",
    )

    github_source = rest_api_source(
        {
            "client": {
                "base_url": "https://api.github.com/repos/dlt-hub/dlt/",
                # If you leave out the default_paginator, it will be inferred from the API:
                # "default_paginator": "header_links",

                # "auth": {
                #     "token": dlt.secrets['token'],
                # }
            },
            "endpoints": {
                "issues/comments": {
                    "params": {
                        "per_page": 100,
                        "since": dlt.sources.incremental(
                            "updated_at", initial_value="2024-01-25T11:21:28Z"
                        ),
                    },
                    "resource": {
                        "primary_key": "id",
                        "write_disposition": "merge",
                    },
                },
                "issues": {
                    "params": {
                        "per_page": 100,
                        "sort": "updated",
                        "direction": "desc",
                        "state": "open",
                    },
                    "resource": {
                        "primary_key": "id",
                        "write_disposition": "merge",
                    },
                    "incremental": {
                        "cursor_path": "updated_at",
                        "initial_value": "2024-01-25T11:21:28Z",
                        "param": "since",
                        # also, todo: "transform": to_iso8601,
                    },
                },
            },
        }
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
            "endpoints": {
                "pokemon": {
                    "params": {
                        "limit": 1000,  # Default page size is 20
                    },
                },
                "berry": {
                    "params": {
                        "limit": 1000,
                    },
                },
                "location": {
                    "params": {
                        "limit": 1000,
                    },
                },
            },
        }
    )

    load_info = pipeline.run(pokemon_source)
    print(load_info)


if __name__ == "__main__":
    load_pokemon()
    load_github()

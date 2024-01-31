"""Generic API Source"""

import dlt

from api_client import APIClient, JSONResponsePaginator, HeaderLinkPaginator, BearerTokenAuth


PAGINATOR_MAP = {
    "json_links": JSONResponsePaginator,
    "header_links": HeaderLinkPaginator,
}


def create_paginator(paginator_config):
    return PAGINATOR_MAP.get(paginator_config, lambda: None)()


def make_client_config(config):
    client_config = config.get("client", {})
    return {
        "base_url": client_config.get("base_url"),
        "auth": client_config.get("auth"),
        "paginator": create_paginator(client_config.get("default_paginator")),
    }


def setup_incremental_object(config):
    return (
        dlt.sources.incremental(
            config.get("cursor_path"),
            initial_value=config.get("initial_value")
        ),
        config.get("param")
    ) if config else (None, None)


@dlt.source
def rest_api_source(config):
    """
    Creates and configures a REST API source for data extraction.

    Example:
        pokemon_source = rest_api_source({
            "client": {
                "base_url": "https://pokeapi.co/api/v2/",
                "default_paginator": "json_links",
            },
            "endpoints": {
                "pokemon": {
                    "params": {
                        "limit": 100, # Default page size is 20
                    },
                    "resource": {
                        "primary_key": "id",
                    },
                },
            },
        })
    """

    client = APIClient(**make_client_config(config))

    for endpoint, endpoint_config in config["endpoints"].items():
        request_params = endpoint_config.get("params", {})
        resource_config = endpoint_config.get("resource", {})

        incremental_object, incremental_param = setup_incremental_object(endpoint_config.get("incremental"))

        def paginate_resource(method, path, params, paginator,
                            incremental_object=incremental_object):
            if incremental_object:
                params[incremental_param] = incremental_object.last_value

            yield from client.paginate(
                method=method,
                path=path,
                params=params,
                paginator=paginator,
            )

        yield dlt.resource(paginate_resource, name=endpoint, **resource_config)(
            method=endpoint_config.get("method", "get"),
            path=endpoint,
            params=request_params,
            paginator=create_paginator(endpoint_config.get("paginator")),
        )

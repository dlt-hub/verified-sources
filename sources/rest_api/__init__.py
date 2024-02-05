"""Generic API Source"""
from typing import TypedDict, Optional, Dict, Any, Union

import dlt

from .client import RESTClient
from .paginators import (
    BasePaginator,
    JSONResponsePaginator,
    HeaderLinkPaginator,
    UnspecifiedPaginator,
)
from .auth import BearerTokenAuth


PAGINATOR_MAP = {
    "json_links": JSONResponsePaginator,
    "header_links": HeaderLinkPaginator,
    "auto": UnspecifiedPaginator,
}


PaginatorType = Union[str, BasePaginator]


class AuthConfig(TypedDict, total=False):
    token: str


class ClientConfig(TypedDict, total=False):
    base_url: str
    auth: Optional[AuthConfig]
    default_paginator: Optional[PaginatorType]


class ResourceConfig(TypedDict, total=False):
    primary_key: str
    write_disposition: str


class IncrementalConfig(TypedDict, total=False):
    cursor_path: str
    initial_value: str
    param: str


class EndpointConfig(TypedDict):
    params: Dict[str, Any]
    resource: ResourceConfig
    incremental: Optional[IncrementalConfig]
    method: str
    paginator: Optional[PaginatorType]


class RESTAPIConfig(TypedDict):
    client: ClientConfig
    endpoints: Dict[str, EndpointConfig]


def create_paginator(paginator_config):
    if isinstance(paginator_config, BasePaginator):
        return paginator_config
    return PAGINATOR_MAP.get(paginator_config, lambda: None)()


def create_auth(auth_config):
    return BearerTokenAuth(auth_config.get("token")) if auth_config else None


def make_client_config(config):
    client_config = config.get("client", {})
    return {
        "base_url": client_config.get("base_url"),
        "auth": client_config.get("auth"),
        "paginator": create_paginator(client_config.get("default_paginator")),
    }


def setup_incremental_object(request_params, incremental_config):
    for key, value in request_params.items():
        if isinstance(value, dlt.sources.incremental):
            return value, key

    return setup_incremental_object_from_config(incremental_config)


def setup_incremental_object_from_config(config):
    return (
        (
            dlt.sources.incremental(
                config.get("cursor_path"), initial_value=config.get("initial_value")
            ),
            config.get("param"),
        )
        if config
        else (None, None)
    )


@dlt.source
def rest_api_source(config: RESTAPIConfig):
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

    client = RESTClient(**make_client_config(config))

    for endpoint, endpoint_config in config["endpoints"].items():
        request_params = endpoint_config.get("params", {})
        resource_config = endpoint_config.get("resource", {})

        incremental_object, incremental_param = setup_incremental_object(
            request_params, endpoint_config.get("incremental")
        )

        def paginate_resource(
            method, path, params, paginator, incremental_object=incremental_object
        ):
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

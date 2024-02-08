"""Generic API Source"""

from typing import TypedDict, Optional, Dict, Any, Union, NamedTuple

import graphlib

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


class ResolveConfig(NamedTuple):
    resource_name: str
    field_path: str


class ResolvedParam(NamedTuple):
    param_name: str
    resolve_config: ResolveConfig


def create_paginator(paginator_config):
    if isinstance(paginator_config, BasePaginator):
        return paginator_config
    return PAGINATOR_MAP.get(paginator_config, lambda: None)()


def create_auth(auth_config):
    if isinstance(auth_config, BearerTokenAuth):
        return auth_config
    return BearerTokenAuth(auth_config.get("token")) if auth_config else None


def make_client_config(config):
    client_config = config.get("client", {})
    return {
        "base_url": client_config.get("base_url"),
        "auth": create_auth(client_config.get("auth")),
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
                    }
                },
            },
        })
    """
    return rest_api_resources(config)


def rest_api_resources(config: RESTAPIConfig):
    client = RESTClient(**make_client_config(config))
    dependency_graph = graphlib.TopologicalSorter()
    endpoint_config_map = {}
    resources = {}

    # Create the dependency graph
    for endpoint, endpoint_config in config["endpoints"].items():
        resource_name = endpoint_config.get("resource", {}).get("name", endpoint)
        path = endpoint_config.get("path", endpoint)

        resolved_params = [
            ResolvedParam(key, value)
            for key, value in endpoint_config.get("params", {}).items()
            if isinstance(value, ResolveConfig)
        ]

        if len(resolved_params) > 1:
            raise ValueError(
                f"Multiple resolved params for resource {resource_name}: {resolved_params}"
            )

        predecessors = set(x.resolve_config.resource_name for x in resolved_params)

        dependency_graph.add(resource_name, *predecessors)
        endpoint_config["_resolved_param"] = (
            resolved_params[0] if resolved_params else None
        )
        endpoint_config["path"] = path
        endpoint_config_map[resource_name] = endpoint_config

    # Create the resources
    for resource_name in dependency_graph.static_order():
        endpoint_config = endpoint_config_map[resource_name]
        request_params = endpoint_config.get("params", {})
        resource_config = endpoint_config.get("resource", {})

        incremental_object, incremental_param = setup_incremental_object(
            request_params, endpoint_config.get("incremental")
        )

        if endpoint_config.get("_resolved_param") is None:
            def paginate_resource(
                method,
                path,
                params,
                paginator,
                incremental_object=incremental_object,
                incremental_param=incremental_param,
            ):
                if incremental_object:
                    params[incremental_param] = incremental_object.last_value

                yield from client.paginate(
                    method=method,
                    path=path,
                    params=params,
                    paginator=paginator,
                )

            resources[resource_name] = dlt.resource(
                paginate_resource, name=resource_name, **resource_config
            )(
                method=endpoint_config.get("method", "get"),
                path=endpoint_config.get("path"),
                params=request_params,
                paginator=create_paginator(endpoint_config.get("paginator")),
            )

        else:
            resolved_param: ResolvedParam = endpoint_config["_resolved_param"]

            predecessor = resources[resolved_param.resolve_config.resource_name]

            param_name = resolved_param.param_name
            request_params.pop(param_name, None)

            def paginate_dependent_resource(
                items,
                method,
                path,
                params,
                paginator,
                param_name=param_name,
                field_path=resolved_param.resolve_config.field_path,
            ):
                items = items or []
                for item in items:
                    formatted_path = path.format(**{param_name: item[field_path]})

                    yield from client.paginate(
                        method=method, path=formatted_path, params=params, paginator=paginator
                    )

            resources[resource_name] = dlt.resource(
                paginate_dependent_resource,
                name=resource_name,
                data_from=predecessor,
                **resource_config,
            )(
                method=endpoint_config.get("method", "get"),
                path=endpoint_config.get("path"),
                params=request_params,
                paginator=create_paginator(endpoint_config.get("paginator")),
            )

    return list(resources.values())

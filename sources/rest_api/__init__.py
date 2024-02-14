"""Generic API Source"""

from dataclasses import dataclass
import copy
from typing import Any, Dict, NamedTuple, Optional, TypedDict, Union

import dlt
import graphlib
from dlt.common.schema.typing import (
    TColumnNames,
    # TSchemaContract,
    TTableFormat,
    TTableSchemaColumns,
    TWriteDisposition,
)
from dlt.extract.incremental import Incremental
from dlt.extract.source import DltResource
from dlt.extract.typing import TTableHintTemplate

from .auth import BearerTokenAuth, AuthBase
from .client import RESTClient
from .paginators import (
    BasePaginator,
    HeaderLinkPaginator,
    JSONResponsePaginator,
    UnspecifiedPaginator,
)
from .utils import remove_key, deep_merge


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


class RESTAPIConfigLegacy(TypedDict):
    client: ClientConfig
    endpoints: Dict[str, EndpointConfig]


class ResolveConfig(NamedTuple):
    resource_name: str
    field_path: str


class ResolvedParam(NamedTuple):
    param_name: str
    resolve_config: ResolveConfig


class Endpoint(TypedDict, total=False):
    path: str
    method: str
    params: Optional[Dict[str, Any]]
    json: Optional[Dict[str, Any]]
    paginator: Optional[PaginatorType]


class EndpointResource(TypedDict, total=False):
    name: TTableHintTemplate[str]
    endpoint: Endpoint
    write_disposition: TTableHintTemplate[TWriteDisposition]
    parent: TTableHintTemplate[str]
    columns: TTableHintTemplate[TTableSchemaColumns]
    primary_key: TTableHintTemplate[TColumnNames]
    merge_key: TTableHintTemplate[TColumnNames]
    incremental: Incremental[Any]
    # schema_contract: TTableHintTemplate[TSchemaContract]
    table_format: TTableHintTemplate[TTableFormat]


class RESTAPIConfig(TypedDict):
    client: ClientConfig
    resource_defaults: EndpointResource
    resources: Dict[str, EndpointResource]


def create_paginator(paginator_config):
    if isinstance(paginator_config, BasePaginator):
        return paginator_config
    return PAGINATOR_MAP.get(paginator_config, lambda: None)()


def create_auth(auth_config):
    if isinstance(auth_config, AuthBase):
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
        if isinstance(value, dict) and value.get("type") == "incremental":
            return (
                dlt.sources.incremental(
                    value.get("cursor_path"), initial_value=value.get("initial_value")
                ),
                key,
            )

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
    """
    Creates and configures a REST API source for data extraction.

    Example:
        github_source = rest_api_resources_v3({
            "client": {
                "base_url": "https://api.github.com/repos/dlt-hub/dlt/",
                "auth": {
                    "token": dlt.secrets["token"],
                },
            },
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
                },
            ],
        })
    """
    client = RESTClient(**make_client_config(config))
    dependency_graph = graphlib.TopologicalSorter()
    endpoint_resource_map = {}
    resources = {}

    default_resource_config = config.get("resource_defaults", {})

    resource_list = config.get("resources")

    if resource_list is None:
        raise ValueError("No resources defined")

    # Create the dependency graph
    for resource_kwargs in resource_list:
        endpoint_resource = make_endpoint_resource(
            resource_kwargs, default_resource_config
        )

        resource_name = endpoint_resource["name"]

        resolved_params = find_resolved_params(endpoint_resource["endpoint"])

        if len(resolved_params) > 1:
            raise ValueError(
                f"Multiple resolved params for resource {resource_name}: {resolved_params}"
            )

        predecessors = set(x.resolve_config.resource_name for x in resolved_params)

        dependency_graph.add(resource_name, *predecessors)
        endpoint_resource["_resolved_param"] = (
            resolved_params[0] if resolved_params else None
        )

        if resource_name in endpoint_resource_map:
            raise ValueError(f"Resource {resource_name} has already been defined")

        endpoint_resource_map[resource_name] = endpoint_resource

    # Create the resources
    for resource_name in dependency_graph.static_order():
        endpoint_resource = endpoint_resource_map[resource_name]
        endpoint_config = endpoint_resource["endpoint"]
        request_params = endpoint_config.get("params", {})

        # TODO: Remove _resolved_param from endpoint_resource
        resolved_param: ResolvedParam = endpoint_resource.pop("_resolved_param", None)
        resource_kwargs = remove_key(endpoint_resource, "endpoint")

        incremental_object, incremental_param = setup_incremental_object(
            request_params, endpoint_config.get("incremental")
        )

        if resolved_param is None:

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
                paginate_resource, **resource_kwargs
            )(
                method=endpoint_config.get("method", "get"),
                path=endpoint_config.get("path"),
                params=request_params,
                paginator=create_paginator(endpoint_config.get("paginator")),
            )

        else:
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
                        method=method,
                        path=formatted_path,
                        params=params,
                        paginator=paginator,
                    )

            resources[resource_name] = dlt.resource(
                paginate_dependent_resource,
                data_from=predecessor,
                **resource_kwargs,
            )(
                method=endpoint_config.get("method", "get"),
                path=endpoint_config.get("path"),
                params=request_params,
                paginator=create_paginator(endpoint_config.get("paginator")),
            )

    return list(resources.values())


def make_endpoint_resource(
    resource: Union[str, EndpointResource], default_config: EndpointResource
):
    """
    Creates an EndpointResource object based on the provided resource
    definition and merges it with the default configuration.

    This function supports defining a resource in multiple formats:
    - As a string: The string is interpreted as both the resource name
        and its endpoint path.
    - As a dictionary: The dictionary must include 'name' and 'endpoint'
        keys. The 'endpoint' can be a string representing the path,
        or a dictionary for more complex configurations.
    """
    if isinstance(resource, str):
        resource = {"name": resource, "endpoint": {"path": resource}}
        return deep_merge(copy.deepcopy(default_config), resource)

    if "endpoint" in resource and isinstance(resource["endpoint"], str):
        resource["endpoint"] = {"path": resource["endpoint"]}

    if "name" not in resource:
        raise ValueError("Resource must have a name")

    if "path" not in resource["endpoint"]:
        raise ValueError("Resource endpoint must have a path")

    return deep_merge(copy.deepcopy(default_config), resource)


def make_resolved_param(key, value):
    if isinstance(value, ResolveConfig):
        return ResolvedParam(key, value)
    if isinstance(value, dict) and value.get("type") == "resolve":
        return ResolvedParam(
            key,
            ResolveConfig(resource_name=value["resource"], field_path=value["field"]),
        )
    return None


def find_resolved_params(endpoint_config):
    """
    Find all resolved params in the endpoint configuration and return
    a list of ResolvedParam objects.

    Resolved params are either of type ResolveConfig or are dictionaries
    with a key "type" set to "resolve".
    """
    return [
        make_resolved_param(key, value)
        for key, value in endpoint_config.get("params", {}).items()
        if isinstance(value, ResolveConfig)
        or (isinstance(value, dict) and value.get("type") == "resolve")
    ]


def rest_api_resources_legacy(config: RESTAPIConfigLegacy):
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

        include_from_parent: list[str] = resource_config.pop("include_from_parent", [])

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

                    child_results = client.paginate(
                        method=method,
                        path=formatted_path,
                        params=params,
                        paginator=paginator,
                    )

                    parent_resource_name = resolved_param.resolve_config.resource_name
                    for r in child_results:
                        if r:
                            yield _add_from_parent(
                                r,
                                item,
                                include_from_parent,
                                parent_resource_name,
                            )
                        else:
                            yield r

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


def _add_from_parent(
    child_records,
    parent_record,
    include_from_parent,
    parent_resource_name,
):
    """allows dependent resource to include parent resource values
    which are not in the response of the child resource"""
    for child in child_records:
        for parent_field in include_from_parent:
            field_from_parent = f"_{parent_resource_name}_{parent_field}"
            if field_from_parent not in child:
                child[field_from_parent] = parent_record[parent_field]
    return child_records


#
# Alternative implementation
#


@dataclass
class Endpoint:
    path: str
    method: str = "get"
    params: Optional[Dict[str, Any]] = None
    json: Optional[Dict[str, Any]] = None
    paginator: Optional[PaginatorType] = None
    incremental: Optional[IncrementalConfig] = None


class EndpointResource:
    def __init__(
        self, endpoint: Endpoint, name: Optional[str] = None, **resource_kwargs
    ):
        self.endpoint = endpoint
        self.name = name or endpoint.path
        self.resource_kwargs = resource_kwargs


@dlt.source
def rest_api_resources_v2(client: RESTClient, *resources: EndpointResource):
    """
    Alternative implementation of the rest_api_source function that uses
    classes to represent the resources and their dependencies:

    Example:
        github_source = rest_api_resources_v2(
            Client(
                base_url="https://api.github.com/repos/dlt-hub/dlt/",
                default_paginator="header_links",
                auth=BearerTokenAuth(dlt.secrets["token"]),
            ),
            Resource(
                Endpoint(
                    "issues/{issue_id}/comments",
                    params={
                        "per_page": 100,
                        "since": dlt.sources.incremental(
                            "updated_at", initial_value="2024-01-25T11:21:28Z"
                        ),
                        "issue_id": resolve_from("issues", "id"),
                    },
                ),
                primary_key="id",
                write_disposition="merge",
            ),
            Resource(
                Endpoint(
                    "issues",
                    params={
                        "per_page": 100,
                        "sort": "updated",
                        "direction": "desc",
                        "state": "open",
                    },
                ),
                primary_key="id",
                write_disposition="merge",
                name="issues",
            )
        )
    """
    dependency_graph = graphlib.TopologicalSorter()
    resource_config_map: Dict[str, EndpointResource] = {}
    dlt_resources: Dict[str, DltResource] = {}

    # Create the dependency graph
    for resource in resources:
        resource_name = resource.name
        resolved_params = [
            ResolvedParam(key, value)
            for key, value in resource.endpoint.params.items()
            if isinstance(value, ResolveConfig)
        ]

        if len(resolved_params) > 1:
            raise ValueError(
                f"Multiple resolved params for resource {resource_name}: {resolved_params}"
            )

        predecessors = set(x.resolve_config.resource_name for x in resolved_params)

        dependency_graph.add(resource_name, *predecessors)

        # Store resolved param
        resource.endpoint._resolved_param = (
            resolved_params[0] if resolved_params else None
        )
        resource_config_map[resource_name] = resource

    # Create the resources
    for resource_name in dependency_graph.static_order():
        resource_config = resource_config_map[resource_name]
        endpoint = resource_config.endpoint
        request_params = endpoint.params or {}

        incremental_object, incremental_param = setup_incremental_object(
            request_params, endpoint.incremental
        )

        if endpoint._resolved_param is None:

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

            dlt_resources[resource_name] = dlt.resource(
                paginate_resource, name=resource_name, **resource.resource_kwargs
            )(
                method=endpoint.method,
                path=endpoint.path,
                params=request_params,
                paginator=create_paginator(endpoint.paginator),
            )

        else:
            resolved_param: ResolvedParam = endpoint._resolved_param

            predecessor = dlt_resources[resolved_param.resolve_config.resource_name]

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
                        method=method,
                        path=formatted_path,
                        params=params,
                        paginator=paginator,
                    )

            dlt_resources[resource_name] = dlt.resource(
                paginate_dependent_resource,
                name=resource_name,
                data_from=predecessor,
                **resource.resource_kwargs,
            )(
                method=endpoint.method,
                path=endpoint.path,
                params=request_params,
                paginator=create_paginator(endpoint.paginator),
            )

    return list(dlt_resources.values())

"""Generic API Source"""

import copy
from typing import (
    Type,
    Any,
    Dict,
    Tuple,
    List,
    NamedTuple,
    Optional,
    TypedDict,
    Union,
)
import graphlib

import dlt
from dlt.common.validation import validate_dict
from dlt.common.schema.typing import (
    TColumnNames,
    # TSchemaContract,
    TTableFormat,
    TTableSchemaColumns,
    TWriteDisposition,
)
from dlt.extract.incremental import Incremental
from dlt.extract.source import DltResource, DltSource
from dlt.extract.typing import TTableHintTemplate
from dlt.common import logger
from dlt.sources.helpers.requests.retry import Client

from .auth import BearerTokenAuth, AuthBase
from .client import RESTClient
from .paginators import (
    BasePaginator,
    HeaderLinkPaginator,
    JSONResponsePaginator,
    UnspecifiedPaginator,
    SinglePagePaginator,
)
from .utils import remove_key, deep_merge


PAGINATOR_MAP = {
    "json_links": JSONResponsePaginator,
    "header_links": HeaderLinkPaginator,
    "auto": UnspecifiedPaginator,
    "single_page": SinglePagePaginator,
}


PaginatorConfigDict = Dict[str, Any]
PaginatorType = Union[Any, BasePaginator, str, PaginatorConfigDict]


class AuthConfig(TypedDict, total=False):
    token: str


class ClientConfig(TypedDict, total=False):
    base_url: str
    auth: Optional[AuthConfig]
    paginator: Optional[PaginatorType]
    request_client: Optional[Client]
    ignore_http_status_codes: Optional[List[int]]


class IncrementalConfig(TypedDict, total=False):
    cursor_path: str
    initial_value: str
    param: str


class ResolveConfig(NamedTuple):
    resource_name: str
    field_path: str


class ResolvedParam(NamedTuple):
    param_name: str
    resolve_config: ResolveConfig


class Endpoint(TypedDict, total=False):
    path: Optional[str]
    method: Optional[str]
    params: Optional[Dict[str, Any]]
    json: Optional[Dict[str, Any]]
    paginator: Optional[PaginatorType]


# TODO: check why validate_dict does not respect total=False
class EndpointResource(TypedDict, total=False):
    name: TTableHintTemplate[str]
    endpoint: Optional[Union[str, Endpoint]]
    write_disposition: Optional[TTableHintTemplate[TWriteDisposition]]
    parent: Optional[TTableHintTemplate[str]]
    columns: Optional[TTableHintTemplate[TTableSchemaColumns]]
    primary_key: Optional[TTableHintTemplate[TColumnNames]]
    merge_key: Optional[TTableHintTemplate[TColumnNames]]
    incremental: Optional[Incremental[Any]]
    table_format: Optional[TTableHintTemplate[TTableFormat]]
    include_from_parent: Optional[List[str]]


class FlexibleEndpointResource(EndpointResource, total=False):
    name: Optional[TTableHintTemplate[str]]


class RESTAPIConfig(TypedDict):
    client: ClientConfig
    resource_defaults: Optional[FlexibleEndpointResource]
    resources: List[Union[str, EndpointResource]]


def get_paginator_class(paginator_type: str) -> Type[BasePaginator]:
    try:
        return PAGINATOR_MAP[paginator_type]
    except KeyError:
        available_options = ", ".join(PAGINATOR_MAP.keys())
        raise ValueError(
            f"Invalid paginator: {paginator_type}. "
            f"Available options: {available_options}"
        )


def create_paginator(paginator_config: PaginatorType) -> Optional[BasePaginator]:
    if isinstance(paginator_config, BasePaginator):
        return paginator_config

    if isinstance(paginator_config, str):
        paginator_class = get_paginator_class(paginator_config)
        return paginator_class()

    if isinstance(paginator_config, dict):
        paginator_type = paginator_config.get("type", "auto")
        paginator_class = get_paginator_class(paginator_type)
        return paginator_class(**remove_key(paginator_config, "type"))

    return None


def create_auth(auth_config: Optional[AuthConfig]) -> Optional[AuthBase]:
    if isinstance(auth_config, AuthBase):
        return auth_config
    return BearerTokenAuth(auth_config.get("token")) if auth_config else None


def make_client_config(config: Dict[str, Any]) -> ClientConfig:
    client_config = config.get("client", {})
    return {
        "base_url": client_config.get("base_url"),
        "auth": create_auth(client_config.get("auth")),
        "paginator": create_paginator(client_config.get("paginator")),
        "request_client": client_config.get("request_client"),
        "ignore_http_status_codes": client_config.get("ignore_http_status_codes"),
    }


def setup_incremental_object(
    request_params: Dict[str, Any], incremental_config: Optional[IncrementalConfig]
) -> Tuple[Optional[Incremental[Any]], Optional[str]]:
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


def setup_incremental_object_from_config(
    config: Optional[IncrementalConfig],
) -> Tuple[Optional[Incremental[Any]], Optional[str]]:
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


def make_parent_key_name(resource_name: str, field_name: str) -> str:
    return f"_{resource_name}_{field_name}"


@dlt.source
def rest_api_source(config: RESTAPIConfig) -> List[DltResource]:
    """
    Creates and configures a REST API source for data extraction.

    Example:
        pokemon_source = rest_api_source({
            "client": {
                "base_url": "https://pokeapi.co/api/v2/",
                "paginator": "json_links",
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


def rest_api_resources(config: RESTAPIConfig) -> List[DltResource]:
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

    validate_dict(RESTAPIConfig, config, path=".")

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
        paginator = create_paginator(endpoint_config.get("paginator"))

        # TODO: Remove _resolved_param from endpoint_resource
        resolved_param: ResolvedParam = endpoint_resource.pop("_resolved_param", None)
        include_from_parent: list[str] = endpoint_resource.pop(
            "include_from_parent", []
        )
        if not resolved_param and include_from_parent:
            raise ValueError(
                f"Resource {resource_name} has include_from_parent but is not "
                "dependent on another resource"
            )

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
                paginator=paginator,
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
                    parent_resource_name = resolved_param.resolve_config.resource_name

                    parent_record = (
                        {
                            make_parent_key_name(parent_resource_name, key): item[key]
                            for key in include_from_parent
                        }
                        if include_from_parent
                        else None
                    )

                    for child_page in client.paginate(
                        method=method,
                        path=formatted_path,
                        params=params,
                        paginator=paginator,
                    ):
                        if parent_record:
                            for child_record in child_page:
                                child_record.update(parent_record)
                        yield child_page

            resources[resource_name] = dlt.resource(
                paginate_dependent_resource,
                data_from=predecessor,
                **resource_kwargs,
            )(
                method=endpoint_config.get("method", "get"),
                path=endpoint_config.get("path"),
                params=request_params,
                paginator=paginator,
            )

    return list(resources.values())


def make_endpoint_resource(
    resource: Union[str, EndpointResource], default_config: EndpointResource
) -> EndpointResource:
    """
    Creates an EndpointResource object based on the provided resource
    definition and merges it with the default configuration.

    This function supports defining a resource in multiple formats:
    - As a string: The string is interpreted as both the resource name
        and its endpoint path.
    - As a dictionary: The dictionary must include `name` and `endpoint`
        keys. The `endpoint` can be a string representing the path,
        or a dictionary for more complex configurations. If the `endpoint`
        is missing the `path` key, the resource name is used as the `path`.
    """
    if isinstance(resource, str):
        resource = {"name": resource, "endpoint": {"path": resource}}
        return deep_merge(copy.deepcopy(default_config), resource)

    if "endpoint" in resource and isinstance(resource["endpoint"], str):
        resource["endpoint"] = {"path": resource["endpoint"]}

    if "name" not in resource:
        raise ValueError("Resource must have a name")

    if "path" not in resource["endpoint"]:
        resource["endpoint"]["path"] = resource["name"]

    return deep_merge(copy.deepcopy(default_config), resource)


def make_resolved_param(
    key: str, value: Union[ResolveConfig, Dict[str, Any]]
) -> Optional[ResolvedParam]:
    if isinstance(value, ResolveConfig):
        return ResolvedParam(key, value)
    if isinstance(value, dict) and value.get("type") == "resolve":
        return ResolvedParam(
            key,
            ResolveConfig(resource_name=value["resource"], field_path=value["field"]),
        )
    return None


def find_resolved_params(endpoint_config: Endpoint) -> List[ResolvedParam]:
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


def check_connection(
    source: DltSource,
    *resource_names: list[str],
) -> tuple[bool, str]:
    try:
        list(source.with_resources(*resource_names).add_limit(1))
        return (True, "")
    except Exception as e:
        logger.error(f"Error checking connection: {e}")
        return (False, str(e))

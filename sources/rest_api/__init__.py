"""Generic API Source"""

from typing import (
    Type,
    Any,
    Dict,
    Tuple,
    List,
    Optional,
    Generator,
    Callable,
    cast,
)
import graphlib  # type: ignore[import,unused-ignore]

import dlt
from dlt.common.validation import validate_dict
from dlt.extract.incremental import Incremental
from dlt.extract.source import DltResource, DltSource
from dlt.common import logger, jsonpath
from dlt.common.schema.schema import Schema
from dlt.common.schema.typing import TSchemaContract
from dlt.common.configuration.specs import BaseConfiguration

from .client import RESTClient
from .detector import single_entity_path
from .paginators import BasePaginator
from .typing import (
    ClientConfig,
    ResolvedParam,
    Endpoint,
    EndpointResource,
    RESTAPIConfig,
    HTTPMethodBasic,
)
from .config_setup import (
    create_auth,
    create_paginator,
    build_resource_dependency_graph,
    make_parent_key_name,
    setup_incremental_object,
    create_response_hooks,
)


def rest_api_source(
    config: RESTAPIConfig,
    name: str = None,
    section: str = None,
    max_table_nesting: int = None,
    root_key: bool = False,
    schema: Schema = None,
    schema_contract: TSchemaContract = None,
    spec: Type[BaseConfiguration] = None,
) -> DltSource:
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
    decorated = dlt.source(
        rest_api_resources,
        name,
        section,
        max_table_nesting,
        root_key,
        schema,
        schema_contract,
        spec,
    )

    return decorated(config)


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

    client_config = config["client"]
    client = RESTClient(
        base_url=client_config["base_url"],
        auth=create_auth(client_config.get("auth")),
        paginator=create_paginator(client_config.get("paginator")),
    )

    resource_defaults = config.get("resource_defaults", {})

    resource_list = config.get("resources")

    if not resource_list:
        raise ValueError("No resources defined")

    (
        dependency_graph,
        endpoint_resource_map,
        resolved_param_map,
    ) = build_resource_dependency_graph(
        resource_defaults,
        resource_list,
    )

    resources = create_resources(
        client,
        dependency_graph,
        endpoint_resource_map,
        resolved_param_map,
    )

    return list(resources.values())


def create_resources(
    client: RESTClient,
    dependency_graph: graphlib.TopologicalSorter,
    endpoint_resource_map: Dict[str, EndpointResource],
    resolved_param_map: Dict[str, Optional[ResolvedParam]],
) -> Dict[str, DltResource]:
    resources = {}

    for resource_name in dependency_graph.static_order():
        resource_name = cast(str, resource_name)
        endpoint_resource = endpoint_resource_map[resource_name]
        endpoint_config = cast(Endpoint, endpoint_resource.pop("endpoint"))
        request_params = endpoint_config.get("params", {})
        paginator = create_paginator(endpoint_config.get("paginator"))

        resolved_param: ResolvedParam = resolved_param_map[resource_name]

        include_from_parent: List[str] = endpoint_resource.pop(
            "include_from_parent", []
        )
        if not resolved_param and include_from_parent:
            raise ValueError(
                f"Resource {resource_name} has include_from_parent but is not "
                "dependent on another resource"
            )

        incremental_object, incremental_param = setup_incremental_object(
            request_params, endpoint_resource.get("incremental")
        )

        hooks = create_response_hooks(endpoint_config.get("response_actions"))

        # try to guess if list of entities or just single entity is returned
        if single_entity_path(endpoint_config["path"]):
            data_selector = "$"
        else:
            data_selector = None

        if resolved_param is None:

            def paginate_resource(
                method: HTTPMethodBasic,
                path: str,
                params: Dict[str, Any],
                paginator: Optional[BasePaginator],
                data_selector: Optional[jsonpath.TJsonPath],
                hooks: Optional[Dict[str, Any]],
                incremental_object: Optional[Incremental[Any]] = incremental_object,
                incremental_param: str = incremental_param,
            ) -> Generator[Any, None, None]:
                if incremental_object:
                    params[incremental_param] = incremental_object.last_value

                yield from client.paginate(
                    method=method,
                    path=path,
                    params=params,
                    paginator=paginator,
                    data_selector=data_selector,
                    hooks=hooks,
                )

            resources[resource_name] = dlt.resource(  # type: ignore[call-overload]
                paginate_resource,
                **endpoint_resource,  # TODO: implement typing.Unpack
            )(
                method=endpoint_config.get("method", "get"),
                path=endpoint_config.get("path"),
                params=request_params,
                paginator=paginator,
                data_selector=endpoint_config.get("data_selector") or data_selector,
                hooks=hooks,
            )

        else:
            predecessor = resources[resolved_param.resolve_config.resource_name]

            request_params.pop(resolved_param.param_name, None)

            def paginate_dependent_resource(
                items: List[Dict[str, Any]],
                method: HTTPMethodBasic,
                path: str,
                params: Dict[str, Any],
                paginator: Optional[BasePaginator],
                data_selector: Optional[jsonpath.TJsonPath],
                hooks: Optional[Dict[str, Any]],
                resolved_param: ResolvedParam = resolved_param,
                include_from_parent: List[str] = include_from_parent,
            ) -> Generator[Any, None, None]:
                field_path = resolved_param.resolve_config.field_path

                items = items or []
                for item in items:
                    formatted_path = path.format(
                        **{resolved_param.param_name: item[field_path]}
                    )
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
                        data_selector=data_selector,
                        hooks=hooks,
                    ):
                        if parent_record:
                            for child_record in child_page:
                                child_record.update(parent_record)
                        yield child_page

            resources[resource_name] = dlt.resource(  # type: ignore[call-overload]
                paginate_dependent_resource,
                data_from=predecessor,
                **endpoint_resource,  # TODO: implement typing.Unpack
            )(
                method=endpoint_config.get("method", "get"),
                path=endpoint_config.get("path"),
                params=request_params,
                paginator=paginator,
                data_selector=endpoint_config.get("data_selector") or data_selector,
                hooks=hooks,
            )

    return resources


def check_connection(
    source: DltSource,
    *resource_names: str,
) -> Tuple[bool, str]:
    try:
        list(source.with_resources(*resource_names).add_limit(1))
        return (True, "")
    except Exception as e:
        logger.error(f"Error checking connection: {e}")
        return (False, str(e))


# XXX: This is a workaround pass test_dlt_init.py
# since the source uses dlt.source as a function
def _register_source(source_func: Callable[..., DltSource]) -> None:
    import inspect
    from dlt.common.configuration import get_fun_spec
    from dlt.common.source import _SOURCES, SourceInfo

    spec = get_fun_spec(source_func)
    func_module = inspect.getmodule(source_func)
    _SOURCES[source_func.__name__] = SourceInfo(
        SPEC=spec,
        f=source_func,
        module=func_module,
    )


_register_source(rest_api_source)

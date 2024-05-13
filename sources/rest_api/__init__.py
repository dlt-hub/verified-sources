"""Generic API Source"""

from typing import (
    Type,
    Any,
    Dict,
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
from dlt.common import jsonpath
from dlt.common.schema.schema import Schema
from dlt.common.schema.typing import TSchemaContract
from dlt.common.configuration.specs import BaseConfiguration

from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.detector import single_entity_path
from dlt.sources.helpers.rest_client.paginators import BasePaginator
from dlt.sources.helpers.rest_client.typing import HTTPMethodBasic
from .typing import (
    ClientConfig,
    ResolvedParam,
    Endpoint,
    EndpointResource,
    RESTAPIConfig,
)
from .config_setup import (
    IncrementalParam,
    create_auth,
    create_paginator,
    build_resource_dependency_graph,
    make_parent_key_name,
    setup_incremental_object,
    create_response_hooks,
)
from .utils import check_connection, exclude_keys  # noqa: F401


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
    """Creates and configures a REST API source for data extraction.

    Args:
        config (RESTAPIConfig): Configuration for the REST API source.
        name (str, optional): Name of the source.
        section (str, optional): Section of the configuration file.
        max_table_nesting (int, optional): Maximum depth of nested table above which
            the remaining nodes are loaded as structs or JSON.
        root_key (bool, optional): Enables merging on all resources by propagating
            root foreign key to child tables. This option is most useful if you
            plan to change write disposition of a resource to disable/enable merge.
            Defaults to False.
        schema (Schema, optional): An explicit `Schema` instance to be associated
            with the source. If not present, `dlt` creates a new `Schema` object
            with provided `name`. If such `Schema` already exists in the same
            folder as the module containing the decorated function, such schema
            will be loaded from file.
        schema_contract (TSchemaContract, optional): Schema contract settings
            that will be applied to this resource.
        spec (Type[BaseConfiguration], optional): A specification of configuration
            and secret values required by the source.

    Returns:
        DltSource: A configured dlt source.

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
    """Creates a list of resources from a REST API configuration.

    Args:
        config (RESTAPIConfig): Configuration for the REST API source.

    Returns:
        List[DltResource]: List of dlt resources.

    Example:
        github_source = rest_api_resources({
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
        client_config,
        dependency_graph,
        endpoint_resource_map,
        resolved_param_map,
    )

    return list(resources.values())


def create_resources(
    client_config: ClientConfig,
    dependency_graph: graphlib.TopologicalSorter,
    endpoint_resource_map: Dict[str, EndpointResource],
    resolved_param_map: Dict[str, Optional[ResolvedParam]],
) -> Dict[str, DltResource]:
    resources = {}

    for resource_name in dependency_graph.static_order():
        resource_name = cast(str, resource_name)
        endpoint_resource = endpoint_resource_map[resource_name]
        endpoint_config = cast(Endpoint, endpoint_resource["endpoint"])
        request_params = endpoint_config.get("params", {})
        request_json = endpoint_config.get("json", None)
        paginator = create_paginator(endpoint_config.get("paginator"))

        resolved_param: ResolvedParam = resolved_param_map[resource_name]

        include_from_parent: List[str] = endpoint_resource.get(
            "include_from_parent", []
        )
        if not resolved_param and include_from_parent:
            raise ValueError(
                f"Resource {resource_name} has include_from_parent but is not "
                "dependent on another resource"
            )

        (
            incremental_object,
            incremental_param,
        ) = setup_incremental_object(request_params, endpoint_config.get("incremental"))

        client = RESTClient(
            base_url=client_config["base_url"],
            headers=client_config.get("headers"),
            auth=create_auth(client_config.get("auth")),
            paginator=create_paginator(client_config.get("paginator")),
        )

        hooks = create_response_hooks(endpoint_config.get("response_actions"))

        # try to guess if list of entities or just single entity is returned
        if single_entity_path(endpoint_config["path"]):
            data_selector = "$"
        else:
            data_selector = None

        resource_kwargs = exclude_keys(
            endpoint_resource, {"endpoint", "include_from_parent"}
        )

        if resolved_param is None:

            def paginate_resource(
                method: HTTPMethodBasic,
                path: str,
                params: Dict[str, Any],
                json: Optional[Dict[str, Any]],
                paginator: Optional[BasePaginator],
                data_selector: Optional[jsonpath.TJsonPath],
                hooks: Optional[Dict[str, Any]],
                client: RESTClient = client,
                incremental_object: Optional[Incremental[Any]] = incremental_object,
                incremental_param: IncrementalParam = incremental_param,
            ) -> Generator[Any, None, None]:
                if incremental_object:
                    params[incremental_param.start] = incremental_object.last_value
                    if incremental_param.end:
                        params[incremental_param.end] = incremental_object.end_value

                yield from client.paginate(
                    method=method,
                    path=path,
                    params=params,
                    json=json,
                    paginator=paginator,
                    data_selector=data_selector,
                    hooks=hooks,
                )

            resources[resource_name] = dlt.resource(
                paginate_resource,
                **resource_kwargs,  # TODO: implement typing.Unpack
            )(
                method=endpoint_config.get("method", "get"),
                path=endpoint_config.get("path"),
                params=request_params,
                json=request_json,
                paginator=paginator,
                data_selector=endpoint_config.get("data_selector") or data_selector,
                hooks=hooks,
            )

        else:
            predecessor = resources[resolved_param.resolve_config.resource_name]

            base_params = exclude_keys(request_params, {resolved_param.param_name})

            def paginate_dependent_resource(
                items: List[Dict[str, Any]],
                method: HTTPMethodBasic,
                path: str,
                params: Dict[str, Any],
                paginator: Optional[BasePaginator],
                data_selector: Optional[jsonpath.TJsonPath],
                hooks: Optional[Dict[str, Any]],
                client: RESTClient = client,
                resolved_param: ResolvedParam = resolved_param,
                include_from_parent: List[str] = include_from_parent,
            ) -> Generator[Any, None, None]:
                field_path = resolved_param.resolve_config.field_path

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
                **resource_kwargs,  # TODO: implement typing.Unpack
            )(
                method=endpoint_config.get("method", "get"),
                path=endpoint_config.get("path"),
                params=base_params,
                paginator=paginator,
                data_selector=endpoint_config.get("data_selector") or data_selector,
                hooks=hooks,
            )

    return resources


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

import copy
from typing import (
    Type,
    Any,
    Dict,
    Tuple,
    List,
    Optional,
    Union,
    Callable,
    cast,
)
import graphlib  # type: ignore[import,unused-ignore]

import dlt
from dlt.extract.incremental import Incremental
from dlt.common import logger
from dlt.common.utils import update_dict_nested
from dlt.common.typing import TSecretStrValue
from dlt.sources.helpers.requests import Response

from .auth import BearerTokenAuth, AuthConfigBase
from .paginators import (
    BasePaginator,
    HeaderLinkPaginator,
    JSONResponsePaginator,
    SinglePagePaginator,
    JSONResponseCursorPaginator,
)
from .typing import (
    AuthConfig,
    IncrementalArgs,
    IncrementalConfig,
    PaginatorType,
    ResolveConfig,
    ResolvedParam,
    ResponseAction,
    Endpoint,
    EndpointResource,
    DefaultEndpointResource,
)
from .exceptions import IgnoreResponseException


PAGINATOR_MAP: Dict[str, Type[BasePaginator]] = {
    "json_links": JSONResponsePaginator,
    "header_links": HeaderLinkPaginator,
    "auto": None,
    "single_page": SinglePagePaginator,
    "cursor": JSONResponseCursorPaginator,
}


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
        paginator_type = paginator_config.pop("type", "auto")
        paginator_class = get_paginator_class(paginator_type)
        return paginator_class(**paginator_config)

    return None


def create_auth(
    auth_config: Optional[Union[AuthConfig, AuthConfigBase]],
) -> Optional[AuthConfigBase]:
    if isinstance(auth_config, AuthConfigBase):
        return auth_config
    return (
        BearerTokenAuth(cast(TSecretStrValue, auth_config.get("token")))
        if auth_config
        else None
    )


def setup_incremental_object(
    request_params: Dict[str, Any],
    incremental_config: Optional[IncrementalConfig] = None,
) -> Tuple[Optional[Incremental[Any]], Optional[str]]:
    for key, value in request_params.items():
        if isinstance(value, dlt.sources.incremental):
            return value, key
        if isinstance(value, dict):
            param_type = value.pop("type")
            if param_type == "incremental":
                return (
                    dlt.sources.incremental(**value),
                    key,
                )
    if incremental_config:
        param = incremental_config.pop("param")
        return (
            dlt.sources.incremental(**cast(IncrementalArgs, incremental_config)),
            param,
        )

    return None, None


def make_parent_key_name(resource_name: str, field_name: str) -> str:
    return f"_{resource_name}_{field_name}"


def build_resource_dependency_graph(
    resource_defaults: DefaultEndpointResource,
    resource_list: List[Union[str, EndpointResource]],
) -> Tuple[Any, Dict[str, EndpointResource], Dict[str, Optional[ResolvedParam]]]:
    dependency_graph = graphlib.TopologicalSorter()
    endpoint_resource_map: Dict[str, EndpointResource] = {}
    resolved_param_map: Dict[str, ResolvedParam] = {}

    for resource_kwargs in resource_list:
        endpoint_resource = make_endpoint_resource(resource_kwargs, resource_defaults)

        resource_name = endpoint_resource["name"]

        if not isinstance(resource_name, str):
            raise ValueError(
                f"Resource name must be a string, got {type(resource_name)}"
            )

        if resource_name in endpoint_resource_map:
            raise ValueError(f"Resource {resource_name} has already been defined")

        resolved_params = find_resolved_params(
            cast(Endpoint, endpoint_resource["endpoint"])
        )

        if len(resolved_params) > 1:
            raise ValueError(
                f"Multiple resolved params for resource {resource_name}: {resolved_params}"
            )

        predecessors = set(x.resolve_config.resource_name for x in resolved_params)

        dependency_graph.add(resource_name, *predecessors)

        endpoint_resource_map[resource_name] = endpoint_resource
        resolved_param_map[resource_name] = (
            resolved_params[0] if resolved_params else None
        )

    return dependency_graph, endpoint_resource_map, resolved_param_map


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
        return update_dict_nested(copy.deepcopy(default_config), resource)  # type: ignore[type-var]

    if "endpoint" in resource and isinstance(resource["endpoint"], str):
        resource["endpoint"] = {"path": resource["endpoint"]}

    if "name" not in resource:
        raise ValueError("Resource must have a name")

    if "path" not in resource["endpoint"]:
        resource["endpoint"]["path"] = resource["name"]  # type: ignore

    return update_dict_nested(copy.deepcopy(default_config), resource)  # type: ignore[type-var]


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


def _handle_response_actions(
    response: Response, actions: List[ResponseAction]
) -> Optional[str]:
    """Handle response actions based on the response and the provided actions.

    Example:
    response_actions = [
        {"status_code": 404, "action": "ignore"},
        {"content": "Not found", "action": "ignore"},
        {"status_code": 429, "action": "retry"},
        {"status_code": 200, "content": "some text", "action": "retry"},
    ]
    action_type = client.handle_response_actions(response, response_actions)
    """
    content = response.text

    for action in actions:
        status_code = action.get("status_code")
        content_substr: str = action.get("content")
        action_type: str = action.get("action")

        if status_code is not None and content_substr is not None:
            if response.status_code == status_code and content_substr in content:
                return action_type

        elif status_code is not None:
            if response.status_code == status_code:
                return action_type

        elif content_substr is not None:
            if content_substr in content:
                return action_type

    return None


def _create_response_actions_hook(
    response_actions: List[ResponseAction],
) -> Callable[[Response, Any, Any], None]:
    def response_actions_hook(response: Response, *args: Any, **kwargs: Any) -> None:
        action_type = _handle_response_actions(response, response_actions)
        if action_type == "ignore":
            logger.info(
                f"Ignoring response with code {response.status_code} "
                f"and content '{response.json()}'."
            )
            raise IgnoreResponseException

    return response_actions_hook


def create_response_hooks(
    response_actions: Optional[List[ResponseAction]],
) -> Optional[Dict[str, Any]]:
    if response_actions:
        return {"response": [_create_response_actions_hook(response_actions)]}
    return None

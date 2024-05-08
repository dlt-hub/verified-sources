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
    NamedTuple,
)
import graphlib  # type: ignore[import,unused-ignore]

import dlt
from dlt.extract.incremental import Incremental
from dlt.common import logger
from dlt.common.utils import update_dict_nested
from dlt.common.typing import TSecretStrValue
from dlt.sources.helpers.requests import Response
from dlt.sources.helpers.rest_client.paginators import (
    BasePaginator,
    SinglePagePaginator,
    HeaderLinkPaginator,
    JSONResponsePaginator,
    JSONResponseCursorPaginator,
    OffsetPaginator,
)
from dlt.sources.helpers.rest_client.exceptions import IgnoreResponseException
from dlt.sources.helpers.rest_client.auth import (
    AuthConfigBase,
    HttpBasicAuth,
    BearerTokenAuth,
    APIKeyAuth,
    OAuthJWTAuth,
)

from .typing import (
    SimpleTokenAuthConfig,
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
from .utils import exclude_keys


PAGINATOR_MAP: Dict[str, Type[BasePaginator]] = {
    "json_links": JSONResponsePaginator,
    "header_links": HeaderLinkPaginator,
    "auto": None,
    "single_page": SinglePagePaginator,
    "cursor": JSONResponseCursorPaginator,
    "offset": OffsetPaginator,
}


class IncrementalParam(NamedTuple):
    start: str
    end: Optional[str]


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
        return paginator_class(**exclude_keys(paginator_config, {"type"}))

    return None


def create_auth(
    auth_config: Optional[Union[SimpleTokenAuthConfig, AuthConfigBase, Dict[str, str]]],
) -> Optional[AuthConfigBase]:
    if isinstance(auth_config, AuthConfigBase):
        return auth_config

    if isinstance(auth_config, dict):
        # Handle a shorthand auth configuration
        if "token" in auth_config and len(auth_config) == 1:
            return BearerTokenAuth(cast(TSecretStrValue, auth_config["token"]))

        # Handle full auth configurations
        auth_config = cast(AuthConfigBase, auth_config)
        if auth_config.get("type") == "http":
            if auth_config.get("scheme") == "basic":
                return HttpBasicAuth(
                    username=auth_config["username"], password=auth_config["password"]
                )
            elif auth_config.get("scheme") == "bearer":
                return BearerTokenAuth(cast(TSecretStrValue, auth_config["token"]))
            else:
                raise ValueError(f"Invalid auth scheme: {auth_config['scheme']}")
        elif auth_config.get("type") == "apiKey":
            return APIKeyAuth(
                name=auth_config["name"],
                api_key=cast(TSecretStrValue, auth_config["api_key"]),
                location=auth_config.get("location"),
            )
        elif auth_config.get("type") == "oauth2":
            return OAuthJWTAuth(
                client_id=auth_config["client_id"],
                private_key=cast(TSecretStrValue, auth_config["private_key"]),
                auth_endpoint=auth_config["auth_endpoint"],
                scopes=auth_config["scopes"],
                headers=auth_config.get("headers"),
                private_key_passphrase=auth_config.get("private_key_passphrase"),
                default_token_expiration=auth_config.get("default_token_expiration"),
            )
        else:
            raise ValueError(f"Invalid auth type: {auth_config['type']}")

    if auth_config:
        raise ValueError(f"Invalid auth config: {auth_config}")

    return None


def setup_incremental_object(
    request_params: Dict[str, Any],
    incremental_config: Optional[IncrementalConfig] = None,
) -> Tuple[Optional[Incremental[Any]], Optional[IncrementalParam]]:
    for key, value in request_params.items():
        if isinstance(value, dlt.sources.incremental):
            return value, IncrementalParam(start=key, end=None)
        if isinstance(value, dict) and value.get("type") == "incremental":
            config = exclude_keys(value, {"type"})
            return (
                dlt.sources.incremental(**config),
                IncrementalParam(start=key, end=None),
            )
    if incremental_config:
        config = exclude_keys(incremental_config, {"start_param", "end_param"})
        return (
            dlt.sources.incremental(**cast(IncrementalArgs, config)),
            IncrementalParam(
                start=incremental_config["start_param"],
                end=incremental_config.get("end_param"),
            ),
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
    """Handle response actions based on the response and the provided actions."""
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

        # If no action has been taken and the status code indicates an error,
        # raise an HTTP error based on the response status
        if not action_type and response.status_code >= 400:
            response.raise_for_status()

    return response_actions_hook


def create_response_hooks(
    response_actions: Optional[List[ResponseAction]],
) -> Optional[Dict[str, Any]]:
    """Create response hooks based on the provided response actions. Note
    that if the error status code is not handled by the response actions,
    the default behavior is to raise an HTTP error.

    Example:
        response_actions = [
            {"status_code": 404, "action": "ignore"},
            {"content": "Not found", "action": "ignore"},
            {"status_code": 429, "action": "retry"},
            {"status_code": 200, "content": "some text", "action": "retry"},
        ]
        hooks = create_response_hooks(response_actions)
    """
    if response_actions:
        return {"response": [_create_response_actions_hook(response_actions)]}
    return None

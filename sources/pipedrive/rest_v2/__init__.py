"""Pipedrive API v2 source using REST API v2 endpoints

Pipedrive API v2 docs: https://pipedrive.readme.io/docs/pipedrive-api-v2
"""


from typing import Iterable, Dict, Any, List, Optional, Union, cast

import dlt
from dlt.sources import DltResource
from dlt.sources.rest_api import rest_api_source, RESTAPIConfig
from dlt.sources.rest_api.typing import EndpointResource

from ..settings import ENTITIES_V2, NESTED_ENTITIES_V2


@dlt.source(name="pipedrive_v2")
def pipedrive_v2_source(
    pipedrive_api_key: Optional[str] = None,
    company_domain: Optional[str] = None,
    resources: Optional[List[str]] = None,
    prefix: str = "v2_",
) -> Iterable[DltResource]:
    """
    Get data from the Pipedrive API v2.

    Args:
        pipedrive_api_key: API token for authentication
        company_domain: Your Pipedrive company domain
        resources: List of resource names to load (e.g., ["deals", "persons"]). If None, loads all available v2 resources.
        prefix: Prefix for table names (default: "v2_")

    Returns:
        Resources for v2 endpoints. Nested endpoints (e.g., deal_products, deal_followers) are automatically included when their parent resource is selected.

    See also: https://pipedrive.readme.io/docs/pipedrive-api-v2#api-v2-availability
    """
    resources = resources or list(ENTITIES_V2.keys())

    # Filter valid v2 endpoints
    v2_resources_config = {
        resource: ENTITIES_V2[resource]
        for resource in resources
        if resource in ENTITIES_V2  # this ensures that resource is supported by v2 api
    }

    if not v2_resources_config:
        raise ValueError(
            f"No valid v2 endpoints found in: {resources}. "
            f"Available endpoints: {list(ENTITIES_V2.keys())}"
        )

    # Only include nested endpoints if their parent is in the v2 endpoints list
    nested_configs_to_create = {
        nested_name: nested_config
        for nested_name, nested_config in NESTED_ENTITIES_V2.items()
        if nested_config["parent"] in v2_resources_config
    }

    # Create and yield v2 resources
    v2_resources = rest_v2_resources(
        pipedrive_api_key,
        company_domain,
        v2_resources_config,
        nested_configs_to_create,
        prefix,
    )
    for resource in v2_resources:
        yield resource


def rest_v2_resources(
    pipedrive_api_key: str,
    company_domain: str,
    resource_configs: Dict[str, Any],
    nested_configs: Dict[str, Dict[str, Any]],
    prefix: str,
) -> Iterable[DltResource]:
    """
    Build and yield REST v2 resources for the given resource configurations.
    Includes nested endpoints that depend on parent resources.
    """
    # Build resources list
    resources: List[Dict[str, Any]] = []

    # Build the resources list for the config from the provided resource configs
    for resource_name, endpoint_config in resource_configs.items():
        resource_def: Dict[str, Any] = {
            "name": resource_name,
            "endpoint": endpoint_config,
        }
        resources.append(resource_def)

    # Add nested resources using native rest_api_source support
    for nested_name, nested_config in nested_configs.items():
        parent_name = nested_config["parent"]
        endpoint_path = nested_config["endpoint_path"]
        params = nested_config.get("params", {})
        primary_key: Union[str, List[str]] = nested_config.get("primary_key", "id")
        include_from_parent = nested_config.get("include_from_parent")

        # Use native rest_api_source nested endpoint syntax: {resources.parent_name.id}
        nested_resource_def: Dict[str, Any] = {
            "name": nested_name,
            "endpoint": {
                "path": endpoint_path.replace(
                    "{id}", f"{{resources.{parent_name}.id}}"
                ),
                "params": params,
            },
        }
        if include_from_parent:
            nested_resource_def["include_from_parent"] = include_from_parent
        if primary_key != "id":
            nested_resource_def["primary_key"] = primary_key
        resources.append(nested_resource_def)

    config: RESTAPIConfig = {
        "client": {
            "base_url": f"https://{company_domain}.pipedrive.com/api/v2/",
            "auth": {
                "type": "api_key",
                "name": "api_token",
                "api_key": pipedrive_api_key,
                "location": "query",
            },
        },
        "resource_defaults": {
            "primary_key": "id",
            "write_disposition": "merge",
            "endpoint": {
                "params": {
                    "limit": 500,
                    "sort_by": "update_time",
                    "sort_direction": "desc",
                },
                "data_selector": "data",
                "paginator": {
                    "type": "cursor",
                    "cursor_path": "additional_data.next_cursor",
                    "cursor_param": "cursor",
                },
            },
        },
        "resources": cast(List[Union[str, EndpointResource, DltResource]], resources),
    }

    api_source = rest_api_source(config)
    for resource in api_source.resources.values():
        yield resource.with_name(f"{prefix}{resource.name}")

"""Pipedrive API v2 source. Loads data from Pipedrive API v2 endpoints with incremental support."""

from typing import Iterable, Dict, Any, List, Optional, Union, cast
import dlt
from dlt.common import pendulum
from dlt.common.time import ensure_pendulum_datetime
from dlt.sources import DltResource
from dlt.sources.rest_api import rest_api_resources, RESTAPIConfig, EndpointResource

from ..settings import ENTITIES_V2, NESTED_ENTITIES_V2

INCREMENTAL_API_RESOURCES = {
    "activities",
    "deals",
    "persons",
    "organizations",
    "products",
}


@dlt.source(name="pipedrive_v2", section="pipedrive")
def pipedrive_v2_source(
    pipedrive_api_key: Optional[str] = dlt.secrets.value,
    company_domain: Optional[str] = dlt.secrets.value,
    resources: Optional[List[str]] = None,
    prefix: str = "v2_",
    since_timestamp: Optional[Union[pendulum.DateTime, str]] = "1970-01-01 00:00:00",
) -> Iterable[DltResource]:
    """Load Pipedrive API v2 data with incremental filtering.

    Args:
        pipedrive_api_key: The API key for authentication.
        company_domain: The company domain for the Pipedrive account.
        resources: List of resource names to load. If None, all available resources are loaded.
        prefix: Prefix to add to resource names. Defaults to "v2_".
        since_timestamp: Start timestamp for incremental loading. Defaults to "1970-01-01 00:00:00".

    Returns:
        Iterable[DltResource]: Resources for Pipedrive API v2 endpoints.
    """
    resources = resources or list(ENTITIES_V2.keys())

    v2_resources_config = {
        res: ENTITIES_V2[res] for res in resources if res in ENTITIES_V2
    }

    if not v2_resources_config:
        raise ValueError(f"No valid v2 endpoints found in: {resources}")

    nested_configs_to_create = {
        name: conf
        for name, conf in NESTED_ENTITIES_V2.items()
        if conf["parent"] in v2_resources_config
    }
    # pipdrive API expects RFC3339 format for updated_since parameter
    since_timestamp_rfc3339 = ensure_pendulum_datetime(
        since_timestamp
    ).to_rfc3339_string()

    v2_resources = rest_v2_resources(
        pipedrive_api_key,
        company_domain,
        v2_resources_config,
        nested_configs_to_create,
        prefix,
        since_timestamp_rfc3339,
    )
    for resource in v2_resources:
        yield resource


def rest_v2_resources(
    pipedrive_api_key: str,
    company_domain: str,
    resource_configs: Dict[str, Any],
    nested_configs: Dict[str, Dict[str, Any]],
    prefix: str,
    since_timestamp: str,
) -> Iterable[DltResource]:
    """Build REST v2 resources with nested endpoints.

    Args:
        pipedrive_api_key: The API key for authentication.
        company_domain: The company domain for the Pipedrive account.
        resource_configs: Configuration for main resources.
        nested_configs: Configuration for nested/dependent resources.
        prefix: Prefix to add to resource names.
        since_timestamp: Timestamp for incremental filtering.

    Returns:
        Iterable[DltResource]: Configured REST API resources.
    """
    resources = []

    for resource_name, endpoint_config in resource_configs.items():
        endpoint_def = {
            **endpoint_config,
            "path": resource_name,
            "params": {**endpoint_config.get("params", {})},
        }

        if resource_name in INCREMENTAL_API_RESOURCES:
            endpoint_def["params"]["updated_since"] = "{incremental.start_value}"
            endpoint_def["incremental"] = {
                "cursor_path": "update_time",
                "initial_value": since_timestamp,
            }

        resources.append(
            {
                "name": f"{prefix}{resource_name}",
                "endpoint": endpoint_def,
            }
        )

    for name, conf in nested_configs.items():
        nested_res = {
            "name": f"{prefix}{name}",
            "endpoint": {
                "path": conf["endpoint_path"].replace(
                    "{id}", f"{{resources.{prefix}{conf['parent']}.id}}"
                ),
                "params": {**conf.get("params", {})},
            },
        }

        if "include_from_parent" in conf:
            nested_res["include_from_parent"] = conf["include_from_parent"]

        pk = conf.get("primary_key", "id")
        if isinstance(pk, list):
            nested_res["primary_key"] = [k for k in pk if not k.startswith("_")]
        elif pk != "id":
            nested_res["primary_key"] = pk

        resources.append(nested_res)

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

    yield from rest_api_resources(config)

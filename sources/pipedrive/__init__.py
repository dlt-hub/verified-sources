"""Highly customizable source for Pipedrive, supports endpoint addition, selection and column rename

Pipedrive api docs: https://developers.pipedrive.com/docs/api/v1

Pipedrive changes or deprecates fields and endpoints without versioning the api.
If something breaks, it's a good idea to check the changelog.
Api changelog: https://developers.pipedrive.com/changelog

To get an api key: https://pipedrive.readme.io/docs/how-to-find-the-api-token
"""

from typing import Any, Dict, Iterator, List, Optional, Union, Iterable, Iterator, Tuple

import dlt

from .helpers.custom_fields_munger import update_fields_mapping, rename_fields
from .helpers.pages import get_recent_items_incremental, get_pages
from .helpers import parse_timestamp, group_deal_flows
from .typing import TDataPage
from .settings import ENTITY_MAPPINGS, RECENTS_ENTITIES
from dlt.sources import DltResource
from dlt.common import pendulum
from dlt.extract.typing import DataItemWithMeta


@dlt.source(name="pipedrive")
def pipedrive_source(
    pipedrive_api_key: str = dlt.secrets.value,
    since_timestamp: Optional[Union[pendulum.DateTime, str]] = dlt.config.value,
) -> Iterator[DltResource]:
    """
    Get data from the Pipedrive API. Supports incremental loading and custom fields mapping.

    Args:
        pipedrive_api_key: https://pipedrive.readme.io/docs/how-to-find-the-api-token
        since_timestamp: Starting timestamp for incremental loading. By default complete history is loaded on first run.
        incremental: Enable or disable incremental loading.

    Returns resources:
        custom_fields_mapping
        activities
        activityTypes
        deals
        deals_flow
        deals_participants
        files
        filters
        notes
        persons
        organizations
        pipelines
        products
        stages
        users
        leads

    For custom fields rename the `custom_fields_mapping` resource must be selected or loaded before other resources.

    Resources that depend on another resource are implemented as transformers
    so they can re-use the original resource data without re-downloading.
    Examples:  deals_participants, deals_flow
    """

    # yield nice rename mapping
    yield create_state(pipedrive_api_key) | parsed_mapping

    # parse timestamp and build kwargs
    since_timestamp = parse_timestamp(since_timestamp)
    resource_kwargs: Any = (
        {"since_timestamp": since_timestamp} if since_timestamp else {}
    )

    # create resources for all endpoints
    endpoints_resources = {}
    for entity, resource_name in RECENTS_ENTITIES.items():
        endpoints_resources[resource_name] = dlt.resource(
            get_recent_items_incremental,
            name=resource_name,
            primary_key="id",
            write_disposition="merge",
        )(entity, pipedrive_api_key, **resource_kwargs)

    yield from endpoints_resources.values()

    # create transformers for deals to participants and flows
    yield endpoints_resources["deals"] | dlt.transformer(
        name="deals_participants", write_disposition="merge", primary_key="id"
    )(_get_deals_participants)(pipedrive_api_key)

    yield endpoints_resources["deals"] | dlt.transformer(
        name="deals_flow", write_disposition="merge", primary_key="id"
    )(_get_deals_flow)(pipedrive_api_key)

    yield leads(pipedrive_api_key, update_time=since_timestamp)


def _get_deals_flow(
    deals_page: TDataPage, pipedrive_api_key: str
) -> Iterator[DataItemWithMeta]:
    custom_fields_mapping = dlt.current.source_state().get("custom_fields_mapping", {})
    for row in deals_page:
        url = f"deals/{row['id']}/flow"
        pages = get_pages(url, pipedrive_api_key)
        for entity, page in group_deal_flows(pages):
            yield dlt.mark.with_table_name(
                rename_fields(page, custom_fields_mapping.get(entity, {})),
                "deals_flow_" + entity,
            )


def _get_deals_participants(
    deals_page: TDataPage, pipedrive_api_key: str
) -> Iterator[TDataPage]:
    for row in deals_page:
        url = f"deals/{row['id']}/participants"
        yield from get_pages(url, pipedrive_api_key)


@dlt.resource(selected=False)
def create_state(pipedrive_api_key: str) -> Iterator[Dict[str, Any]]:
    def _get_pages_for_rename(
        entity: str, fields_entity: str, pipedrive_api_key: str
    ) -> Dict[str, Any]:
        existing_fields_mapping: Dict[
            str, Dict[str, str]
        ] = custom_fields_mapping.setdefault(entity, {})
        # we need to process all pages before yielding
        for page in get_pages(fields_entity, pipedrive_api_key):
            existing_fields_mapping = update_fields_mapping(
                page, existing_fields_mapping
            )
        return existing_fields_mapping

    # gets all *Fields data and stores in state
    custom_fields_mapping = dlt.current.source_state().setdefault(
        "custom_fields_mapping", {}
    )
    for entity, fields_entity, _ in ENTITY_MAPPINGS:
        if fields_entity is None:
            continue
        custom_fields_mapping[entity] = _get_pages_for_rename(
            entity, fields_entity, pipedrive_api_key
        )

    yield custom_fields_mapping


@dlt.transformer(
    name="custom_fields_mapping",
    write_disposition="replace",
    columns={"options": {"data_type": "complex"}},
)
def parsed_mapping(
    custom_fields_mapping: Dict[str, Any]
) -> Optional[Iterator[List[Dict[str, str]]]]:
    """
    Parses and yields custom fields' mapping in order to be stored in destiny by dlt
    """
    for endpoint, data_item_mapping in custom_fields_mapping.items():
        yield [
            {
                "endpoint": endpoint,
                "hash_string": hash_string,
                "name": names["name"],
                "normalized_name": names["normalized_name"],
                "options": names["options"],
                "field_type": names["field_type"],
            }
            for hash_string, names in data_item_mapping.items()
        ]


@dlt.resource(primary_key="id", write_disposition="merge")
def leads(
    pipedrive_api_key: str = dlt.secrets.value,
    update_time: dlt.sources.incremental[str] = dlt.sources.incremental(
        "update_time", initial_value=None,
    ),
) -> Iterator[TDataPage]:
    """Resource to incrementally load pipedrive leads by update_time"""
    # Leads inherit custom fields from deals
    fields_mapping = (
        dlt.current.source_state().get("custom_fields_mapping", {}).get("deals", {})
    )
    # Load leads pages sorted from newest to oldest and stop loading when
    # last incremental value is reached
    last_value = update_time.last_value
    pages = get_pages(
        "leads",
        pipedrive_api_key,
        extra_params={"sort": "update_time DESC"},
    )
    for page in pages:
        # TODO: This check can be replaced with `update_time.start_out_of_range` in dlt 0.3.5
        if last_value:
            # Just check whether first item is lower, worst case we load 1 redundant page before break
            first_item = page[0] if page else None
            if first_item and first_item["update_time"] < last_value:
                return
        yield rename_fields(page, fields_mapping)

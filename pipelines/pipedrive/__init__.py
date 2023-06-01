"""Highly customizable pipeline for Pipedrive, supports endpoint addition, selection and column rename

Pipedrive api docs: https://developers.pipedrive.com/docs/api/v1

Pipedrive changes or deprecates fields and endpoints without versioning the api.
If something breaks, it's a good idea to check the changelog.
Api changelog: https://developers.pipedrive.com/changelog

To get an api key: https://pipedrive.readme.io/docs/how-to-find-the-api-token
"""

from typing import Any, Dict, Iterator, List, Optional, Union, Iterable, Iterator, Tuple
from itertools import groupby


import dlt

from .custom_fields_munger import update_fields_mapping, rename_fields
from .recents import _get_recent_items_incremental
from .helpers import _get_pages
from .typing import TDataPage
from dlt.extract.source import DltResource
from dlt.common import pendulum
from dlt.extract.typing import DataItemWithMeta


ENTITY_MAPPINGS = [
    ("activity", "activityFields", {"user_id": 0}),
    ("organization", "organizationFields", None),
    ("person", "personFields", None),
    ("product", "productFields", None),
    ("deal", "dealFields", None),
    ("pipeline", None, None),
    ("stage", None, None),
    ("user", None, None),
]

RECENTS_ENTITIES = {
    "activity": "activities",
    "activityType": "activityTypes",
    "deal": "deals",
    "file": "files",
    "filter": "filters",
    "note": "notes",
    "person": "persons",
    "organization": "organizations",
    "pipeline": "pipelines",
    "product": "products",
    "stage": "stages",
    "user": "users",
}


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

    For custom fields rename the `custom_fields_mapping` resource must be selected or loaded before other resources.

    Resources that depend on another resource are implemented as transformers
    so they can re-use the original resource data without re-downloading.
    Examples:  deals_participants, deals_flow
    """

    mapping_state = create_state(pipedrive_api_key)
    # yield nice rename mapping
    yield mapping_state | parsed_mapping
    kw = {}
    if since_timestamp:
        if isinstance(since_timestamp, str):
            since_timestamp = pendulum.parse(since_timestamp)  # type: ignore[assignment]
        assert isinstance(
            since_timestamp, pendulum.DateTime
        ), "since_timestamp must be a valid ISO datetime string or pendulum.DateTime object"
        since_timestamp = since_timestamp.in_timezone("UTC")
        kw["since_timestamp"] = (
            since_timestamp.to_iso8601_string().replace("T", " ").replace("Z", "")
        )  # pd datetime format

    endpoints_resources = {}
    for entity, resource_name in RECENTS_ENTITIES.items():
        endpoints_resources[resource_name] = dlt.resource(
            _get_recent_items_incremental,
            name=resource_name,
            primary_key="id",
            write_disposition="merge",
        )(entity, **kw)

    yield from endpoints_resources.values()
    yield endpoints_resources["deals"] | dlt.transformer(
        name="deals_participants", write_disposition="merge", primary_key="id"
    )(_get_deals_participants)(pipedrive_api_key)

    yield endpoints_resources["deals"] | dlt.transformer(
        name="deals_flow", write_disposition="merge", primary_key="id"
    )(_get_deals_flow)(pipedrive_api_key)


def _deals_flow_group_key(item: Dict[str, Any]) -> str:
    return item["object"]  # type: ignore[no-any-return]


def _grouped_deals_flow(
    pages: Iterable[Iterable[Dict[str, Any]]]
) -> Iterator[Tuple[str, List[Dict[str, Any]]]]:
    for page in pages:
        for entity, items in groupby(
            sorted(page, key=_deals_flow_group_key), key=_deals_flow_group_key
        ):
            yield entity, [
                dict(item["data"], timestamp=item["timestamp"]) for item in items
            ]


def _get_deals_flow(
    deals_page: TDataPage, pipedrive_api_key: str
) -> Iterator[DataItemWithMeta]:
    custom_fields_mapping = dlt.current.source_state().get("custom_fields_mapping", {})
    for row in deals_page:
        url = f"deals/{row['id']}/flow"
        pages = _get_pages(url, pipedrive_api_key)
        for entity, page in _grouped_deals_flow(pages):
            yield dlt.mark.with_table_name(
                rename_fields(page, custom_fields_mapping.get(entity, {})),
                "deals_flow_" + entity,
            )


def _get_deals_participants(
    deals_page: TDataPage, pipedrive_api_key: str
) -> Iterator[TDataPage]:
    for row in deals_page:
        url = f"deals/{row['id']}/participants"
        yield from _get_pages(url, pipedrive_api_key)


@dlt.resource(selected=False)
def create_state(pipedrive_api_key: str) -> Iterator[Dict[str, Any]]:
    def _get_pages_for_rename(
        entity: str, fields_entity: str, pipedrive_api_key: str
    ) -> Dict[str, Any]:
        existing_fields_mapping: Dict[
            str, Dict[str, str]
        ] = custom_fields_mapping.setdefault(entity, {})
        # we need to process all pages before yielding
        for page in _get_pages(fields_entity, pipedrive_api_key):
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

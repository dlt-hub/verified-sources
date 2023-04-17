"""Highly customizable pipeline for Pipedrive, supports endpoint addition, selection and column rename

Pipedrive api docs: https://developers.pipedrive.com/docs/api/v1

Pipedrive changes or deprecates fields and endpoints without versioning the api.
If something breaks, it's a good idea to check the changelog.
Api changelog: https://developers.pipedrive.com/changelog

To get an api key: https://pipedrive.readme.io/docs/how-to-find-the-api-token
"""

import dlt

from .custom_fields_munger import update_fields_mapping, rename_fields
from .recents import grouped_recents, _extract_recents_data
from .helpers import _paginated_get, _get_pages
from .typing import TDataPage
from dlt.common.typing import TDataItems
from dlt.extract.source import DltResource
from dlt.sources.helpers import requests
from dlt.common.schema.typing import TWriteDisposition
from dlt.common import pendulum
from typing import Any, Dict, Iterator, List, Optional, Sequence


ENTITY_MAPPINGS = [
    ('activity', 'activityFields', {'user_id': 0}),
    ('organization', "organizationFields", None),
    ('person', 'personFields', None),
    ('product', 'productFields', None),
    ('deal', 'dealFields', None),
    ("pipeline", None, None),
    ('stage', None, None),
    ('user', None, None)
]

RECENTS_ENTITIES = [
    "activity",
    "activityType",
    "deal",
    "file",
    "filter",
    "note",
    "person",
    "organization",
    "pipeline",
    "product",
    "stage",
    "user",
]


@dlt.source(name="pipedrive")
def pipedrive_source(
    pipedrive_api_key: str = dlt.secrets.value,
    write_disposition: TWriteDisposition = 'merge',
    since_timestamp: Optional[pendulum.DateTime] = dlt.config.value
) -> Iterator[DltResource]:
    mapping_state = create_state(pipedrive_api_key)
    # yield nice rename mapping
    yield mapping_state | parsed_mapping
    kw = {}
    if since_timestamp:
        kw['since_timestamp'] = since_timestamp.to_iso8601_string()

    endpoints_resources = {}
    for entity in RECENTS_ENTITIES:
        endpoints_resources[entity] = dlt.resource(
            _get_recent_pages, name=entity, primary_key="id", write_disposition=write_disposition
        )(entity, **kw)

    yield from endpoints_resources.values()
    yield endpoints_resources["deal"] | _make_deals_child_resource("deal_participant", "participants", pipedrive_api_key, write_disposition)
    yield endpoints_resources["deal"] | _make_deals_child_resource("deal_flow", "flow", pipedrive_api_key, write_disposition)


def _get_recent_pages(
    entity: str,
    pipedrive_api_key: str = dlt.secrets.value,
    since_timestamp: dlt.sources.incremental[str] = dlt.sources.incremental('update_time|modified', '1970-01-01 00:00:00'),
) -> Iterator[TDataPage]:
    """Get a specific entity type from /recents.
    """
    custom_fields_mapping = dlt.state().get('custom_fields_mapping', {}).get(entity, {})
    pages = _get_pages(
        "recents", pipedrive_api_key,
        extra_params=dict(since_timestamp=since_timestamp.last_value, items=entity),
    )
    for page in pages:
        yield rename_fields(_extract_recents_data(page), custom_fields_mapping)


def _get_deals_children(deals_page: TDataPage, endpoint: str, pipedrive_api_key: str) -> Iterator[TDataPage]:
    for row in deals_page:
        endpoint = f"deals/{row['id']}/{endpoint}"
        yield from _get_pages(endpoint, pipedrive_api_key)


def _make_deals_child_resource(name: str, endpoint: str, pipedrive_api_key: str, write_disposition: TWriteDisposition) -> DltResource:
    return dlt.transformer(name=name, write_disposition=write_disposition, primary_key="id")(_get_deals_children)(endpoint, pipedrive_api_key)


@dlt.resource(selected=False)
def create_state(pipedrive_api_key: str) -> Iterator[Dict[str, Any]]:
    def _get_pages_for_rename(entity: str, pipedrive_api_key: str) -> Dict[str, Any]:

        existing_fields_mapping: Dict[str, Any] = custom_fields_mapping.setdefault(entity, {})
        # we need to process all pages before yielding
        for page in _get_pages(entity, pipedrive_api_key):
            existing_fields_mapping = update_fields_mapping(page, existing_fields_mapping)
        return existing_fields_mapping


    # gets all *Fields data and stores in state
    custom_fields_mapping = dlt.state().setdefault('custom_fields_mapping', {})
    for entity, fields_entity, _ in ENTITY_MAPPINGS:
        if fields_entity is None:
            continue
        custom_fields_mapping[entity] = _get_pages_for_rename(fields_entity, pipedrive_api_key)

    yield custom_fields_mapping


@dlt.transformer(name='custom_fields_mapping', write_disposition='replace')
def parsed_mapping(custom_fields_mapping: Dict[str, Any]) -> Optional[Iterator[List[Dict[str, str]]]]:
    """
    Parses and yields custom fields' mapping in order to be stored in destiny by dlt
    """
    for endpoint, data_item_mapping in custom_fields_mapping.items():
        yield [{'endpoint': endpoint, 'hash_string': hash_string, 'name': names['name'], 'normalized_name': names['normalized_name']} for hash_string, names in data_item_mapping.items()]

"""Highly customizable pipeline for Pipedrive, supports endpoint addition, selection and column rename

Pipedrive api docs: https://developers.pipedrive.com/docs/api/v1

Pipedrive changes or deprecates fields and endpoints without versioning the api.
If something breaks, it's a good idea to check the changelog.
Api changelog: https://developers.pipedrive.com/changelog

To get an api key: https://pipedrive.readme.io/docs/how-to-find-the-api-token
"""

import dlt

from .custom_fields_munger import update_fields_mapping, rename_fields
from dlt.common.typing import TDataItems
from dlt.extract.source import DltResource
from dlt.sources.helpers import requests
from typing import Any, Dict, Iterator, List, Optional, Sequence


ENTITY_MAPPINGS = [
    ('activities', 'activityFields', {'user_id': 0}),
    ('organizations', "organizationFields", None),
    ('persons', 'personFields', None),
    ('products', 'productFields', None),
    ('deals', 'dealFields', None),
    ("pipelines", None, None),
    ('stages', None, None),
    ('users', None, None)
]


@dlt.source(name='pipedrive')
def pipedrive_source(pipedrive_api_key: str = dlt.secrets.value) -> Iterator[DltResource]:
    """

    Args:
    pipedrive_api_key: https://pipedrive.readme.io/docs/how-to-find-the-api-token

    Returns resources:
        activityFields
        dealFields
        deals
        deals_flow
        deals_participants
        organizationFields
        organizations
        personFields
        persons
        pipelines
        productFields
        products
        stages
        users

    Resources that depend on another resource are implemented as transformers
    so they can re-use the original resource data without re-downloading.
    Examples:  deals_participants, deals_flow

    """

    # create state
    mapping_state = create_state(pipedrive_api_key)
    # yield nice rename mapping
    yield mapping_state | parsed_mapping

    endpoints_resources = {}
    # the *Fields resources send the rename data to regular resources
    for entity, fields_entity, extra_params in ENTITY_MAPPINGS:
        if fields_entity is None:
            # regular resource
            resource = dlt.resource(_get_pages(entity, pipedrive_api_key, extra_params=extra_params), name=entity, write_disposition='replace')
            endpoints_resources[resource.name] = resource
        else:
            # currently we make entities dependent on fieldEntities. this is not a good idea. we just need to make sure that state is created before the entities
            # are extracted. if there's not state the renames do not happen. this lets people for example to implement their own source for rename state
            field_resource = dlt.resource(_get_rename_from_state(None, entity), name=fields_entity, write_disposition='replace', selected=False)
            entity_transformer = dlt.transformer(name=entity, write_disposition='replace')(_get_pages_with_rename)(entity, pipedrive_api_key, extra_params=extra_params)
            endpoints_resources[entity_transformer.name] =  field_resource | entity_transformer

    yield from endpoints_resources.values()
    yield endpoints_resources["deals"] | deals_participants(pipedrive_api_key=pipedrive_api_key)
    yield endpoints_resources["deals"] | deals_flow(pipedrive_api_key=pipedrive_api_key)


def _paginated_get(url: str, headers: Dict[str, Any], params: Dict[str, Any]) -> Iterator[Iterator[Dict[str, Any]]]:
    """
    Requests and yields data 500 records at a time
    Documentation: https://pipedrive.readme.io/docs/core-api-concepts-pagination
    """
    # pagination start and page limit
    is_next_page = True
    params['start'] = 0
    params['limit'] = 500
    while is_next_page:
        response = requests.get(url, headers=headers, params=params)
        page = response.json()
        # yield data only
        data = page['data']
        if data:
            yield data
        # check if next page exists
        pagination_info = page.get('additional_data', {}).get('pagination', {})
        # is_next_page is set to True or False
        is_next_page = pagination_info.get('more_items_in_collection', False)
        if is_next_page:
            params['start'] = pagination_info.get('next_start')


def _get_pages(entity: str, pipedrive_api_key: str, extra_params: Dict[str, Any] = None) -> Iterator[Iterator[Dict[str, Any]]]:
    """
    Generic method to retrieve endpoint data based on the required headers and params.

    Args:
        entity: the endpoint you want to call
        pipedrive_api_key:
        extra_params: any needed request params except pagination.
        munge_custom_fields: whether to perform custom fields' data munging or not

    Returns:

    """
    headers = {'Content-Type': 'application/json'}
    params = {'api_token': pipedrive_api_key}
    if extra_params:
        params.update(extra_params)
    url = f'https://app.pipedrive.com/v1/{entity}'
    yield from _paginated_get(url, headers=headers, params=params)


def _get_rename_from_state(custom_fields_mapping, entity: str) -> Iterator[Dict[str, Any]]:
    custom_fields_mapping = custom_fields_mapping or dlt.state()['custom_fields_mapping']
    yield custom_fields_mapping.get(entity, {})


def _get_pages_with_rename(data_item_mapping: Dict[str, Any], entity: str, pipedrive_api_key: str, extra_params: Dict[str, Any] = None) -> Iterator[TDataItems]:
    for page in _get_pages(entity, pipedrive_api_key, extra_params):
        yield rename_fields(page, data_item_mapping)


@dlt.transformer(write_disposition='replace')
def deals_participants(deals_page: Any, pipedrive_api_key: str = dlt.secrets.value) -> Optional[Iterator[TDataItems]]:
    """
    This transformer builds on deals resource data.
    The data is passed to it page by page (as the upstream resource returns it)
    """
    for row in deals_page:
        endpoint = f'deals/{row["id"]}/participants'
        data = _get_pages(endpoint, pipedrive_api_key)
        if data:
            yield from data


@dlt.transformer(write_disposition='replace')
def deals_flow(deals_page: Any, pipedrive_api_key: str = dlt.secrets.value) -> Optional[Iterator[TDataItems]]:
    """
    This transformer builds on deals resource data.
    The data is passed to it page by page (as the upstream resource returns it)
    """
    for row in deals_page:
        endpoint = f'deals/{row["id"]}/flow'
        data = _get_pages(endpoint, pipedrive_api_key)
        if data:
            yield from data


@dlt.resource(selected=False)
def create_state(pipedrive_api_key: str):
    def _get_pages_for_rename(entity: str, pipedrive_api_key: str) -> Dict[str, Any]:

        existing_fields_mapping = custom_fields_mapping.setdefault(entity, {})
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
def parsed_mapping(custom_fields_mapping) -> Optional[Iterator[List[Dict[str, str]]]]:
    """
    Parses and yields custom fields' mapping in order to be stored in destiny by dlt
    """
    for endpoint, data_item_mapping in custom_fields_mapping.items():
        yield [{'endpoint': endpoint, 'hash_string': hash_string, 'name': names['name'], 'normalized_name': names['normalized_name']} for hash_string, names in data_item_mapping.items()]

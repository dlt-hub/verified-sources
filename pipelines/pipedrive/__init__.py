"""Highly customizable pipeline for Pipedrive, supports endpoint addition, selection and column rename

Pipedrive api docs: https://developers.pipedrive.com/docs/api/v1

Pipedrive changes or deprecates fields and endpoints without versioning the api.
If something breaks, it's a good idea to check the changelog.
Api changelog: https://developers.pipedrive.com/changelog

To get an api key: https://pipedrive.readme.io/docs/how-to-find-the-api-token
"""

import dlt
import functools

from .custom_fields_munger import entities_mapping, entity_fields_mapping, munge_push_func, pull_munge_func, parsed_mapping
from .incremental_loading_helpers import (
    entities_mapping as recents_entities_mapping, entity_items_mapping as recents_entity_items_mapping, get_entity_items_param, get_since_timestamp, set_last_timestamp, max_datetime,
    parse_datetime_obj, parse_datetime_str, UTC_OFFSET
)
from dlt.common.typing import TDataItems
from dlt.extract.source import DltResource
from dlt.sources.helpers import requests
from typing import Any, Dict, Iterator, Optional, Sequence


@dlt.source(name='pipedrive')
def pipedrive_source(pipedrive_api_key: str = dlt.secrets.value) -> Sequence[DltResource]:
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

    Resources that depend on another resource are implemented as tranformers
    so they can re-use the original resource data without re-downloading.
    Examples:  deals_participants, deals_flow

    """

    # we need to add entity fields' resources before entities' resources in order to let custom fields data munging work properly
    entity_fields_endpoints = ['activityFields', 'dealFields', 'organizationFields', 'personFields', 'productFields']
    endpoints_resources = [dlt.resource(_get_endpoint(endpoint, pipedrive_api_key), name=endpoint, write_disposition='replace') for endpoint in entity_fields_endpoints]

    entities_endpoints = ['pipelines', 'persons', 'stages', 'users']
    endpoints_resources += [dlt.resource(_get_endpoint(endpoint, pipedrive_api_key), name=endpoint, write_disposition='replace') for endpoint in entities_endpoints]

    # add incremental loading resources
    entity_item_params = ['organization', 'product']
    endpoints_resources += [
        dlt.resource(_get_endpoint(RECENTS_ENDPOINT, pipedrive_api_key, extra_params={'items': entity_item_param}), name=recents_entity_items_mapping[entity_item_param], write_disposition='append')
        for entity_item_param in entity_item_params
    ]

    # add activities
    activities_resource = dlt.resource(_get_endpoint('activities', pipedrive_api_key, extra_params={'user_id': 0}), name='activities', write_disposition='replace')
    endpoints_resources.append(activities_resource)
    # add resources that need 2 requests

    # we make the resource explicitly and put it in a variable
    deals = dlt.resource(_get_endpoint('deals', pipedrive_api_key), name='deals', write_disposition='replace')

    endpoints_resources.append(deals)

    # in order to avoid calling the deal endpoint 3 times (once for deal, twice for deal_entity)
    # we use a transformer instead of a resource.
    # The transformer can use the deals resource from cache instead of having to get the data again.
    # We use the pipe operator to pass the deals to

    endpoints_resources.append(deals | deals_participants(pipedrive_api_key=pipedrive_api_key))
    endpoints_resources.append(deals | deals_flow(pipedrive_api_key=pipedrive_api_key))

    # add custom fields' mapping
    endpoints_resources.append(parsed_mapping())

    return endpoints_resources


RECENTS_ENDPOINT = 'recents'


def _paginated_get(base_url: str, endpoint: str, headers: Dict[str, Any], params: Dict[str, Any]) -> Optional[Iterator[TDataItems]]:
    """
    Requests and yields data 500 records at a time
    Documentation: https://pipedrive.readme.io/docs/core-api-concepts-pagination
    """
    url = f'{base_url}/{endpoint}'
    last_timestamp = None
    # pagination start and page limit
    is_next_page = True
    params['start'] = 0
    params['limit'] = 500
    while is_next_page:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        page = response.json()
        # yield data only
        data = page['data']
        if data:
            if page.get('additional_data', {}).get('since_timestamp', ''):  # checks 'recents' endpoint's data
                data = [data_item['data'] for data_item in data]  # filters and flattens 'recents' endpoint's data
            last_timestamp_str = data[-1].get('add_time', '')  # regardless of full or incremental loading
            last_timestamp = max_datetime(last_timestamp, parse_datetime_str(last_timestamp_str + UTC_OFFSET))
            yield data
        # check if next page exists
        pagination_info = page.get('additional_data', {}).get('pagination', {})
        # is_next_page is set to True or False
        is_next_page = pagination_info.get('more_items_in_collection', False)
        if is_next_page:
            params['start'] = pagination_info.get('next_start')

        if last_timestamp:
            # store last timestamp in dlt's state
            if endpoint in recents_entities_mapping:
                last_timestamp_str = parse_datetime_obj(last_timestamp)
                set_last_timestamp(endpoint, last_timestamp_str)
            elif endpoint == RECENTS_ENDPOINT:
                entity_items_param = params.get('items', '')
                if entity_items_param in recents_entity_items_mapping:
                    endpoint = recents_entity_items_mapping[entity_items_param]  # turns entity items' param into entities' endpoint
                    last_timestamp_str = parse_datetime_obj(last_timestamp)
                    set_last_timestamp(endpoint, last_timestamp_str)


BASE_URL = 'https://app.pipedrive.com/v1'


def _get_endpoint(entity: str, pipedrive_api_key: str, extra_params: Dict[str, Any] = None, munge_custom_fields: bool = True) -> Optional[Iterator[TDataItems]]:
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

    if entity == RECENTS_ENDPOINT:
        entity_items_param = get_entity_items_param(params)
        params['since_timestamp'] = get_since_timestamp(recents_entity_items_mapping.get(entity_items_param, ''))

    pages = _paginated_get(base_url=BASE_URL, endpoint=entity, headers=headers, params=params)
    if munge_custom_fields:
        if entity in entity_fields_mapping:  # checks if it's an entity fields' endpoint (e.g.: activityFields)
            munging_func = munge_push_func
        else:
            munging_func = pull_munge_func
            if entity == RECENTS_ENDPOINT:
                entity = recents_entity_items_mapping.get(entity_items_param, '')  # turns entity items' param into entities' endpoint
            entity = entities_mapping.get(entity, '')  # turns entities' endpoint into entity fields' endpoint
        pages = map(functools.partial(munging_func, endpoint=entity), pages)
    yield from pages


@dlt.transformer(write_disposition='replace')
def deals_participants(deals_page: Any, pipedrive_api_key: str = dlt.secrets.value) -> Optional[Iterator[TDataItems]]:
    """
    This transformer builds on deals resource data.
    The data is passed to it page by page (as the upstream resource returns it)
    """
    for row in deals_page:
        endpoint = f'deals/{row["id"]}/participants'
        data = _get_endpoint(endpoint, pipedrive_api_key, munge_custom_fields=False)
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
        data = _get_endpoint(endpoint, pipedrive_api_key, munge_custom_fields=False)
        if data:
            yield from data

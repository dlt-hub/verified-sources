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
    entities_mapping as recents_entities_mapping,
    entity_items_mapping as recents_entity_items_mapping,
    get_entity_items_param,
    get_last_timestamp,
    get_last_metadatum_from_state,
    is_single_endpoint,
    set_last_metadata
)
from dlt.common.typing import TDataItems
from dlt.extract.source import DltResource
from dlt.sources.helpers import requests
from time import sleep
from timeit import default_timer
from typing import Any, Dict, Iterator, Optional, Sequence


@dlt.source(name='pipedrive')
def pipedrive_source(
    pipedrive_api_key: str = dlt.secrets.value, api_version: str = dlt.config.value, limit_window: int = dlt.config.value, rate_limit: int = dlt.config.value, initial_days_back: int = dlt.config.value
) -> Sequence[DltResource]:
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
    endpoints_resources = [
        dlt.resource(_get_endpoint(endpoint, pipedrive_api_key, api_version, limit_window, rate_limit, initial_days_back), name=endpoint, write_disposition='replace')
        for endpoint in entity_fields_endpoints
    ]

    entities_endpoints = ['pipelines', 'persons', 'stages', 'users']
    endpoints_resources += [
        dlt.resource(_get_endpoint(endpoint, pipedrive_api_key, api_version, limit_window, rate_limit, initial_days_back), name=endpoint, write_disposition='replace')
        for endpoint in entities_endpoints
    ]

    # add incremental loading resources
    entity_item_params = ['organization', 'product']
    endpoints_resources += [
        dlt.resource(
            _get_endpoint(RECENTS_ENDPOINT, pipedrive_api_key, api_version, limit_window, rate_limit, initial_days_back, extra_params={'items': entity_item_param}),
            name=recents_entity_items_mapping[entity_item_param],
            write_disposition='append'
        )
        for entity_item_param in entity_item_params
    ]

    # add activities
    activities_resource = dlt.resource(
        _get_endpoint('activities', pipedrive_api_key, api_version, limit_window, rate_limit, initial_days_back, extra_params={'user_id': 0}),
        name='activities',
        write_disposition='replace'
    )
    endpoints_resources.append(activities_resource)
    # add resources that need 2 requests

    # we make the resource explicitly and put it in a variable
    deals = dlt.resource(_get_endpoint('deals', pipedrive_api_key, api_version, limit_window, rate_limit, initial_days_back), name='deals', write_disposition='replace')

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


FIELDS_SUFFIX = 'Fields'
RECENTS_ENDPOINT = 'recents'


def _paginated_get(base_url: str, endpoint: str, headers: Dict[str, Any], params: Dict[str, Any], limit_window: int, rate_limit: int) -> Optional[Iterator[TDataItems]]:
    """
    Requests and yields data 500 records at a time
    Documentation: https://pipedrive.readme.io/docs/core-api-concepts-pagination
    """
    url = f'{base_url}/{endpoint}'
    # pagination start and page limit
    is_next_page = True
    params['start'] = 0
    params['limit'] = 500
    last_timestamp = ''
    last_ids = []
    while is_next_page:
        start_time = default_timer()
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        page = response.json()
        # yield data only
        data = page['data']
        if data:

            if endpoint == RECENTS_ENDPOINT:
                data = [datum['data'] for datum in data]  # filters and flattens 'recents' endpoint's data

            # deduplicate data
            last_ids_from_state = get_last_metadatum_from_state(endpoint, 'ids')
            if last_ids_from_state:
                last_ids_from_state = set(last_ids_from_state)
                data = list(filter(lambda datum: datum['id'] not in last_ids_from_state, data))

            if all([data, is_single_endpoint(endpoint), FIELDS_SUFFIX not in endpoint]):
                timestamp_field = 'created' if any([endpoint == 'users', get_entity_items_param(params) == 'user']) else 'update_time'
                if data[-1][timestamp_field] != last_timestamp:
                    last_timestamp = data[-1][timestamp_field]
                    last_ids = [datum['id'] for datum in data if datum[timestamp_field] == last_timestamp]
                else:
                    last_ids += [datum['id'] for datum in data if datum[timestamp_field] == last_timestamp]

            yield data
        # check if next page exists
        pagination_info = page.get('additional_data', {}).get('pagination', {})
        # is_next_page is set to True or False
        is_next_page = pagination_info.get('more_items_in_collection', False)
        if is_next_page:
            params['start'] = pagination_info.get('next_start')

        end_time = default_timer()
        elapsed_time = end_time - start_time
        if elapsed_time < limit_window / rate_limit:
            # https://pipedrive.readme.io/docs/core-api-concepts-rate-limiting
            sleep(limit_window / rate_limit - elapsed_time)

        if all([last_timestamp, last_ids]):
            # store last metadata in dlt's state
            if endpoint == RECENTS_ENDPOINT:
                entity_items_param = get_entity_items_param(params)
                if recents_entity_items_mapping.get(entity_items_param):
                    endpoint = recents_entity_items_mapping[entity_items_param]  # turns entity items' param into entities' endpoint
                    set_last_metadata(endpoint, last_timestamp, last_ids)
            elif endpoint not in entity_fields_mapping:
                set_last_metadata(endpoint, last_timestamp, last_ids)


def _get_endpoint(
    entity: str, pipedrive_api_key: str, api_version: str, limit_window: int, rate_limit: int,  initial_days_back: int, extra_params: Dict[str, Any] = None, munge_custom_fields: bool = True
) -> Optional[Iterator[TDataItems]]:
    """
    Generic method to retrieve endpoint data based on the required headers and params.

    Args:
        entity: the endpoint you want to call
        pipedrive_api_key:
        api_version:
        limit_window:
        rate_limit:
        initial_days_back:
        extra_params: any needed request params except pagination.
        munge_custom_fields: whether to perform custom fields' data munging or not

    Returns:

    """
    base_url = f'https://app.pipedrive.com/{api_version}'
    headers = {'Content-Type': 'application/json'}
    params = {'api_token': pipedrive_api_key}
    if extra_params:
        params.update(extra_params)

    entity_items_param = ''
    if entity == RECENTS_ENDPOINT:
        entity_items_param = get_entity_items_param(params)
        if recents_entity_items_mapping.get(entity_items_param):
            timestamp_format = '%Y-%m-%d %H:%M:%S'
            params['since_timestamp'] = get_last_timestamp(recents_entity_items_mapping[entity_items_param], initial_days_back, timestamp_format)
    elif all([is_single_endpoint(entity), FIELDS_SUFFIX not in entity]):
        timestamp_field = 'created' if entity == 'users' else 'update_time'
        params['sort'] = f'{timestamp_field} ASC'

    pages = _paginated_get(base_url=base_url, endpoint=entity, headers=headers, params=params, limit_window=limit_window, rate_limit=rate_limit)
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
def deals_participants(
    deals_page: Any,
    pipedrive_api_key: str = dlt.secrets.value,
    api_version: str = dlt.config.value,
    limit_window: int = dlt.config.value,
    rate_limit: int = dlt.config.value,
    initial_days_back: int = dlt.config.value
) -> Optional[Iterator[TDataItems]]:
    """
    This transformer builds on deals resource data.
    The data is passed to it page by page (as the upstream resource returns it)
    """
    for row in deals_page:
        endpoint = f'deals/{row["id"]}/participants'
        data = _get_endpoint(endpoint, pipedrive_api_key, api_version, limit_window, rate_limit, initial_days_back, munge_custom_fields=False)
        if data:
            yield from data


@dlt.transformer(write_disposition='replace')
def deals_flow(
    deals_page: Any,
    pipedrive_api_key: str = dlt.secrets.value,
    api_version: str = dlt.config.value,
    limit_window: int = dlt.config.value,
    rate_limit: int = dlt.config.value,
    initial_days_back: int = dlt.config.value
) -> Optional[Iterator[TDataItems]]:
    """
    This transformer builds on deals resource data.
    The data is passed to it page by page (as the upstream resource returns it)
    """
    for row in deals_page:
        endpoint = f'deals/{row["id"]}/flow'
        data = _get_endpoint(endpoint, pipedrive_api_key, api_version, limit_window, rate_limit, initial_days_back, munge_custom_fields=False)
        if data:
            yield from data

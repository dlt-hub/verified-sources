"""Highly customizable pipeline for Shopify, supports endpoint addition, selection, full and incremental loading

Shopify REST Admin API docs: https://shopify.dev/docs/api/admin-rest

To get an admin access token: https://shopify.dev/docs/apps/auth/admin-app-access-tokens
"""

import dlt
import requests

from dlt.common.typing import TDataItems
from dlt.extract.source import DltResource
from time import sleep
from timeit import default_timer
from typing import Any, Dict, Iterator, Optional, Sequence

from .incremental_loading_helpers import get_last_timestamp, get_last_metadatum_from_state, set_last_metadata


@dlt.source(name='shopify')
def shopify_source(
    access_token: str = dlt.secrets.value,
    store_name: str = dlt.secrets.value,
    api_version: str = dlt.config.value,
    limit_window: int = dlt.config.value,
    rate_limit: int = dlt.config.value,
    initial_days_back: int = dlt.config.value
) -> Sequence[DltResource]:
    """

    Args:
    access_token: https://shopify.dev/docs/apps/auth/admin-app-access-tokens

    Returns resources:
        orders
        customers
        events
        marketing_events
        draft_orders
        locations
        checkouts
        price_rules
        products
        tender_transactions
        balance_transactions (if the store has payments enabled and the corresponding endpoint is available in the region)
        disputes (if the store has payments enabled and the corresponding endpoint is available in the region)
        payouts (if the store has payments enabled and the corresponding endpoint is available in the region)

    """

    # add incremental loading resource
    resources = [dlt.resource(_get_endpoint('orders', access_token, store_name, api_version, limit_window, rate_limit, initial_days_back), name='orders', write_disposition='append')]

    # add full loading resources
    single_endpoints = ['customers', 'events', 'marketing_events', 'draft_orders', 'locations', 'checkouts', 'price_rules', 'products', 'tender_transactions']
    resources += [
        dlt.resource(_get_endpoint(endpoint, access_token, store_name, api_version, limit_window, rate_limit, initial_days_back, incrementally=False), name=endpoint, write_disposition='replace')
        for endpoint in single_endpoints
    ]

    # the following endpoints return a 404 error whether the store doesn't have payments enabled or isn't available in the region
    '''
    compound_endpoints = {'balance_transactions': 'shopify_payments/balance/transactions', 'disputes': 'shopify_payments/disputes', 'payouts': 'shopify_payments/payouts'}
    resources += [
        dlt.resource(
            _get_endpoint(endpoint_alias, access_token, store_name, api_version, limit_window, rate_limit, initial_days_back, extra_params={endpoint_alias: endpoint}, incrementally=False),
            name=endpoint_alias,
            write_disposition='replace'
        )
        for endpoint_alias, endpoint in compound_endpoints.items()
    ]
    '''

    return resources


def _paginated_get(base_url: str, endpoint: str, headers: Dict[str, Any], params: Dict[str, Any], limit_window: int, rate_limit: int) -> Optional[Iterator[TDataItems]]:
    """
    Requests and yields data 250 records at a time
    Documentation: https://shopify.dev/docs/api/usage/pagination-rest
    """
    if params.get(endpoint):
        url = f'{base_url}/{params[endpoint]}.json'  # turns endpoint alias' url into endpoint's url
        params.pop(endpoint, '')
    else:
        url = f'{base_url}/{endpoint}.json'
    # pagination page limit
    is_next_page = True
    params['limit'] = 250
    last_timestamp = ''
    last_ids = []
    while is_next_page:
        start_time = default_timer()
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        page = response.json()
        data = page[endpoint]
        # deduplicate data
        last_ids_from_state = get_last_metadatum_from_state(endpoint, 'ids')
        if last_ids_from_state:
            last_ids_from_state = set(last_ids_from_state)
            data = list(filter(lambda datum: datum['id'] not in last_ids_from_state, data))
        # yield data only
        if data:
            timestamp_field = 'processed_at' if endpoint == 'tender_transactions' else 'created_at'
            if data[-1][timestamp_field] != last_timestamp:
                last_timestamp = data[-1][timestamp_field]
                last_ids = [datum['id'] for datum in data if datum[timestamp_field] == last_timestamp]
            else:
                last_ids += [datum['id'] for datum in data if datum[timestamp_field] == last_timestamp]
            yield data
        # check if next page exists
        linked_url = response.links.get('next', {}).get('url', '')
        # is_next_page is set to True or False
        is_next_page = linked_url != ''
        if is_next_page:
            url = linked_url
            if not response.links.get('previous'):
                params.pop('status', '')
                params.pop('order', '')
                filtering_param = 'processed_at_min' if endpoint == 'tender_transactions' else 'created_at_min'
                params.pop(filtering_param, '')
        end_time = default_timer()
        elapsed_time = end_time - start_time
        if elapsed_time < limit_window / rate_limit:
            # https://shopify.dev/docs/api/usage/rate-limits
            sleep(limit_window / rate_limit - elapsed_time)

    if all([last_timestamp, last_ids]):
        # store last metadata in dlt's state
        set_last_metadata(endpoint, last_timestamp, last_ids)


def _get_endpoint(
    entity: str, access_token: str, store_name: str, api_version: str, limit_window: int, rate_limit: int, initial_days_back: int, extra_params: Dict[str, Any] = None, incrementally: bool = True
) -> Optional[Iterator[TDataItems]]:
    """
    Generic method to retrieve endpoint data based on the required headers and params.

    Args:
        entity: the endpoint you want to call
        access_token:
        store_name:
        api_version:
        limit_window:
        rate_limit:
        initial_days_back:
        extra_params: any needed request params except pagination.
        incrementally: whether to perform incremental loading or not (full loading)

    Returns:

    """
    headers = {'Content-Type': 'application/json', 'X-Shopify-Access-Token': str(access_token)}
    timestamp_field = 'processed_at' if entity == 'tender_transactions' else 'created_at'
    params = {'status': 'any', 'order': f'{timestamp_field} asc'}
    if extra_params:
        params.update(extra_params)
    if incrementally:
        filtering_param = 'processed_at_min' if entity == 'tender_transactions' else 'created_at_min'
        timestamp_format = '%Y-%m-%dT%H:%M:%S%z'
        last_timestamp = get_last_timestamp(entity, initial_days_back, timestamp_format)
        params[filtering_param] = last_timestamp
    base_url = f'https://{store_name}.myshopify.com/admin/api/{api_version}'
    pages = _paginated_get(base_url, endpoint=entity, headers=headers, params=params, limit_window=limit_window, rate_limit=rate_limit)
    yield from pages

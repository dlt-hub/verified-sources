"""Highly customizable pipeline for Salesforce, supports endpoint addition, selection, full and incremental loading

Salesforce REST API docs: https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/intro_rest.htm
"""

import dlt

from dlt.common.typing import TDataItems
from dlt.extract.source import DltResource
from dlt.sources.helpers import requests
from typing import Any, Dict, Iterator, Optional, Sequence

from .helpers.credentials import SalesforceCredentialsToken
from .helpers.incremental_loading_helpers import get_last_timestamp, get_last_metadatum_from_state, set_last_metadata


@dlt.source(name='salesforce')
def salesforce_source(
    credentials: SalesforceCredentialsToken = dlt.secrets.value, domain: str = dlt.secrets.value, api_version: str = dlt.config.value, initial_days_back: int = dlt.config.value
) -> Sequence[DltResource]:
    """

    Args:
    credentials:
    domain:
    api_version:
    initial_days_back:

    Returns resources:
        Account
        Order
        Organization

    """

    access_token = _authenticate(credentials)

    # add incremental loading resource
    resources = [dlt.resource(_get_endpoint('Order', access_token, domain, api_version, initial_days_back), name='order', write_disposition='append')]

    # add full loading resources
    endpoints = ['Account', 'Organization']
    resources += [
        dlt.resource(_get_endpoint(endpoint, access_token, domain, api_version, initial_days_back, incrementally=False), name=endpoint.lower(), write_disposition='replace')
        for endpoint in endpoints
    ]

    return resources


def _paginated_get(base_url: str, endpoint: str, headers: Dict[str, Any], params: Dict[str, Any]) -> Optional[Iterator[TDataItems]]:
    """
    Requests and yields data 2000 records at a time
    Documentation: https://developer.salesforce.com/docs/atlas.en-us.242.0.api_rest.meta/api_rest/resources_query.htm
    """
    url = base_url
    is_next_page = True
    last_timestamp = ''
    last_ids = []
    while is_next_page:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        page = response.json()
        data = page['records']
        # deduplicate data
        last_ids_from_state = get_last_metadatum_from_state(endpoint, 'ids')
        if last_ids_from_state:
            last_ids_from_state = set(last_ids_from_state)
            data = list(filter(lambda datum: datum['Id'] not in last_ids_from_state, data))
        # yield data only
        if data:

            timestamp_field = 'CreatedDate'
            if data[-1][timestamp_field] != last_timestamp:
                last_timestamp = data[-1][timestamp_field]
                last_ids = [datum['Id'] for datum in data if datum[timestamp_field] == last_timestamp]
            else:
                last_ids += [datum['Id'] for datum in data if datum[timestamp_field] == last_timestamp]

            for datum in data:
                datum.pop('attributes', '')

            yield data
        # check if next page exists
        # is_next_page is set to True or False
        is_next_page = not page['done']
        if is_next_page:
            # https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/resources_query_more_results.htm
            url = base_url + '/queryLocator'
            params['queryLocator'] = page['nextRecordsUrl']
            params.pop('q', '')

    if all([last_timestamp, last_ids]):
        # store last metadata in dlt's state
        set_last_metadata(endpoint, last_timestamp, last_ids)


def _authenticate(credentials: SalesforceCredentialsToken) -> Any:
    # https://developer.salesforce.com/docs/atlas.en-us.212.0.api_rest.meta/api_rest/intro_understanding_username_password_oauth_flow.htm
    url = 'https://login.salesforce.com/services/oauth2/token'
    data = {
        'grant_type': 'password',
        'client_id': credentials.client_id,
        'client_secret': credentials.client_secret,
        'username': credentials.username,
        'password': credentials.password + credentials.security_token,
    }
    headers = {'Content-Type': 'application/json'}
    response = requests.post(url, data=data, headers=headers)
    response.raise_for_status()
    json_response = response.json()
    access_token = json_response['access_token']
    return access_token


def _get_endpoint(
    entity: str, access_token: str, domain: str, api_version: str, initial_days_back: int, extra_params: Dict[str, Any] = None, incrementally: bool = True
) -> Optional[Iterator[TDataItems]]:
    """
    Generic method to retrieve endpoint data based on the required headers and params.

    Args:
        entity: the endpoint you want to call
        access_token:
        domain:
        api_version:
        initial_days_back:
        extra_params: any needed request params except pagination.
        incrementally: whether to perform incremental loading or not (full loading)

    Returns:

    """
    headers = {'Content-Type': 'application/json', 'Authorization': f'Bearer {access_token}', 'Sforce-Query-Options': 'batchSize=2000'}
    timestamp_field = 'CreatedDate'
    if incrementally:
        timestamp_format = '%Y-%m-%dT%H:%M:%S%z'
        last_timestamp = get_last_timestamp(entity, initial_days_back, timestamp_format)
        params = {'q': f'SELECT+FIELDS(ALL)+FROM+{entity}+WHERE+{timestamp_field}+>=+{last_timestamp}+ORDER+BY+{timestamp_field}+ASC'}  # soql
    else:
        params = {'q': f'SELECT+FIELDS(ALL)+FROM+{entity}+ORDER+BY+{timestamp_field}+ASC'}  # soql
    if extra_params:
        params.update(extra_params)
    base_url = f'https://{domain}.my.salesforce.com/services/data/{api_version}/query'
    pages = _paginated_get(base_url, endpoint=entity, headers=headers, params=params)
    yield from pages

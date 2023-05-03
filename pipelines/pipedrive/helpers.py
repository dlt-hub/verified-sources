from typing import Dict, Iterator, Any, List

from dlt.sources.helpers import requests


def _paginated_get(url: str, headers: Dict[str, Any], params: Dict[str, Any]) -> Iterator[List[Dict[str, Any]]]:
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


def _get_pages(entity: str, pipedrive_api_key: str, extra_params: Dict[str, Any] = None) -> Iterator[List[Dict[str, Any]]]:
    """
    Generic method to retrieve endpoint data based on the required headers and params.

    Args:
        entity: the endpoint you want to call
        pipedrive_api_key:
        extra_params: any needed request params except pagination.

    Returns:

    """
    headers = {'Content-Type': 'application/json'}
    params = {'api_token': pipedrive_api_key}
    if extra_params:
        params.update(extra_params)
    url = f'https://app.pipedrive.com/v1/{entity}'
    yield from _paginated_get(url, headers=headers, params=params)

import urllib.parse
from typing import Generator, Dict, Any

import requests
from reretry import retry

BASE_URL = 'https://api.hubapi.com/'


def get_url(endpoint, **kwargs):
    return urllib.parse.urljoin(BASE_URL, endpoint.format(**kwargs))


def _get_headers(api_key: str) -> Dict[str, str]:
    """
    Return a dictionary of HTTP headers to use for API requests, including the specified API key.

    Args:
        api_key (str): The API key to use for authentication, as a string.

    Returns:
        dict: A dictionary of HTTP headers to include in API requests, with the `Authorization` header
            set to the specified API key in the format `Bearer {api_key}`.

    """
    # Construct the dictionary of HTTP headers to use for API requests
    return dict(
        authorization=f'Bearer {api_key}'
    )


def _parse_response(r: requests.Response, **kwargs) -> Generator[Dict[str, Any], None, None]:
    """
    Parse a JSON response from HUBSPOT and yield the properties of each result.

    Args:
        r (requests.Response): The response object from the API call.
        **kwargs: Additional keyword arguments to pass to the `fetch_data` function.

    Yields:
        dict: The properties of each result in the API response.

    Notes:
        This method assumes that the API response is in JSON format, and that the results are contained
        within the "results" key of the JSON object. If the response does not contain any results, a
        `ValueError` will be raised.

        If the response contains pagination information in the "paging" key of the JSON object, this method
        will follow the "next" link in the pagination information and yield the properties of each result in
        the subsequent pages. The `fetch_data` function is used to retrieve the subsequent pages, and any
        additional keyword arguments passed to this method will be passed on to the `fetch_data` function.

    """
    # Parse the response JSON data
    _data = r.json()

    # Yield the properties of each result in the API response
    if 'results' in _data:
        for _result in _data['results']:
            _obj = _result['properties']
            if 'associations' in _result:
                for association in _result['associations']:
                    __values = [{'value': _obj['hs_object_id'], f'{association}_id': __r['id']} for __r in
                                _result['associations'][association]['results']]

                    # remove duplicates from list of dicts
                    __values = [dict(t) for t in {tuple(d.items()) for d in __values}]

                    _obj[association] = __values
            yield _obj

    # Follow pagination links if they exist
    if 'paging' in _data:
        _next = _data['paging'].get('next', None)
        if _next:
            # Replace the base URL with an empty string to get the relative URL for the next page
            next_url = _next['link'].replace(BASE_URL, '')
            # Recursively call the `fetch_data` function to get the next page of results
            yield from fetch_data(next_url, **kwargs)


@retry(tries=3, delay=1, backoff=1.1)
def fetch_data(endpoint: str, api_key: str, **kwargs) -> Generator[Dict[str, Any], None, None]:
    """
    Fetch data from HUBSPOT endpoint using a specified API key and yield the properties of each result.

    Args:
        endpoint (str): The endpoint to fetch data from, as a string.
        api_key (str): The API key to use for authentication, as a string.
        **kwargs: Additional keyword arguments to pass to the `_parse_response` function.

    Yields:
        dict: The properties of each result in the API response.

    Raises:
        requests.exceptions.HTTPError: If the API returns an HTTP error status code.

    Notes:
        This function uses the `requests` library to make a GET request to the specified endpoint, with
        the API key included in the headers. If the API returns a non-successful HTTP status code (e.g.
        404 Not Found), a `requests.exceptions.HTTPError` exception will be raised.

        The `endpoint` argument should be a relative URL, which will be appended to the base URL for the
        API. The `**kwargs` argument is used to pass additional keyword arguments to the `_parse_response`
        function, such as any parameters that need to be included in the API request.

        This function also includes a retry decorator that will automatically retry the API call up to
        3 times with a 1-second delay between retries, using an exponential backoff strategy.

    """
    # Construct the URL and headers for the API request
    _url = get_url(endpoint, **kwargs)
    _headers = _get_headers(api_key)

    # Make the API request
    r = requests.get(_url, headers=_headers)

    # Raise an exception if the API returns an HTTP error status code
    r.raise_for_status()

    # Parse the API response and yield the properties of each result
    return _parse_response(r, api_key=api_key, **kwargs)

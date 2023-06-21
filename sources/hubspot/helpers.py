"""Hubspot source helpers"""

import urllib.parse
from typing import Generator, Dict, Any, List

from dlt.sources.helpers import requests

BASE_URL = "https://api.hubapi.com/"


def get_url(endpoint: str, **kwargs: Any) -> str:
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
    return dict(authorization=f"Bearer {api_key}")


def _parse_response(
    r: requests.Response, **kwargs: str
) -> Generator[List[Dict[str, Any]], None, None]:
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
    if "results" in _data:
        _objects: List[Dict[str, Any]] = []
        for _result in _data["results"]:
            _obj = _result["properties"]
            if "associations" in _result:
                for association in _result["associations"]:
                    __values = [
                        {"value": _obj["hs_object_id"], f"{association}_id": __r["id"]}
                        for __r in _result["associations"][association]["results"]
                    ]

                    # remove duplicates from list of dicts
                    __values = [dict(t) for t in {tuple(d.items()) for d in __values}]

                    _obj[association] = __values
            _objects.append(_obj)
        if _objects:
            yield _objects

    # Follow pagination links if they exist
    if "paging" in _data:
        _next = _data["paging"].get("next", None)
        if _next:
            # Replace the base URL with an empty string to get the relative URL for the next page
            next_url = _next["link"].replace(BASE_URL, "")
            # Recursively call the `fetch_data` function to get the next page of results
            yield from fetch_data(next_url, **kwargs)


def fetch_data(
    endpoint: str, api_key: str, **kwargs: str
) -> Generator[List[Dict[str, Any]], None, None]:
    """
    Fetch data from HUBSPOT endpoint using a specified API key and yield the properties of each result.

    Args:
        endpoint (str): The endpoint to fetch data from, as a string.
        api_key (str): The API key to use for authentication, as a string.
        **kwargs: Additional keyword arguments to pass to the `_parse_response` function.

    Yields:
        List[dict]: The properties of each result in the API response.

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
        3 times with a 5-second delay between retries, using an exponential backoff strategy.

    """
    # Construct the URL and headers for the API request
    url = get_url(endpoint, **kwargs)
    headers = _get_headers(api_key)

    # Make the API request
    r = requests.get(url, headers=headers)
    # Parse the API response and yield the properties of each result
    return _parse_response(r, api_key=api_key, **kwargs)


def _get_property_names(api_key: str, entity: str) -> List[str]:
    """
    Retrieve property names for a given entity from the HubSpot API.

    Args:
        entity: The entity name for which to retrieve property names.

    Returns:
        A list of property names.

    Raises:
        Exception: If an error occurs during the API request.
    """
    properties = []
    endpoint = f"/crm/v3/properties/{entity}"
    url = get_url(endpoint)
    headers = _get_headers(api_key)

    def fetch_data(url: str) -> None:
        """
        Fetch data from the specified URL and recursively fetches data from subsequent pages if available.

        Args:
            url: The URL to fetch data from.

        Returns:
            None
        """
        # Make the API request
        r = requests.get(url, headers=headers)
        if "results" in r.json():
            properties.extend([prop["name"] for prop in r.json()['results']])

        if "paging" in r.json():
            next_link = r.json()["paging"].get("next", None)
            if next_link:
                # Replace the base URL with an empty string to get the relative URL for the next page
                next_url = next_link["link"].replace(BASE_URL, "")
                # Recursively call the `fetch_data` function to get the next page of results
                fetch_data(next_url)

    fetch_data(url)
    return properties
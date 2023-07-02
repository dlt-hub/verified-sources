"""Hubspot source helpers"""

import urllib.parse
from typing import Iterator, Dict, Any, List, Optional, Iterable, Tuple

from dlt.sources.helpers import requests
from .settings import OBJECT_TYPE_PLURAL

BASE_URL = "https://api.hubapi.com/"


def get_url(endpoint: str) -> str:
    """Get absolute hubspot endpoint URL"""
    return urllib.parse.urljoin(BASE_URL, endpoint)


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


def extract_property_history(objects: List[Dict[str, Any]]) -> Iterator[Dict[str, Any]]:
    for item in objects:
        history = item.get("propertiesWithHistory")
        if not history:
            return
        # Yield a flat list of property history entries
        for key, changes in history.items():
            if not changes:
                continue
            for entry in changes:
                yield {"object_id": item["id"], "property_name": key, **entry}


def fetch_object(
    endpoint: str, api_key: str, params: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """Fetch a single data object from the API. From e.g. `.../contacts/{id}` endpoint"""
    url = get_url(endpoint)
    headers = _get_headers(api_key)
    r = requests.get(url, headers=headers, params=params)
    return r.json()  # type: ignore


def fetch_data_with_history(
    endpoint: str, api_key: str, params: Optional[Dict[str, Any]] = None
) -> Iterator[Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]]:
    """
    Fetch data from HUBSPOT endpoint using a specified API key and yield the properties of each result.
    For paginated endpoint this function yields item from all pages.
    For objects that support it and When params includes `propertiesWithHistory`
    a flattened list of history entries is included in the return tuple.

    Args:
        endpoint (str): The endpoint to fetch data from, as a string.
        api_key (str): The API key to use for authentication, as a string.
        params: Optional dict of query params to include in the request

    Yields:
        A tuple consisting of 1. List of CRM object dicts and 2. List of property history entries

    Raises:
        requests.exceptions.HTTPError: If the API returns an HTTP error status code.

    Notes:
        This function uses the `requests` library to make a GET request to the specified endpoint, with
        the API key included in the headers. If the API returns a non-successful HTTP status code (e.g.
        404 Not Found), a `requests.exceptions.HTTPError` exception will be raised.

        The `endpoint` argument should be a relative URL, which will be appended to the base URL for the
        API. The `params` argument is used to pass additional query parameters to the request

        This function also includes a retry decorator that will automatically retry the API call up to
        3 times with a 5-second delay between retries, using an exponential backoff strategy.
    """
    # Construct the URL and headers for the API request
    url = get_url(endpoint)
    headers = _get_headers(api_key)

    # Make the API request
    r = requests.get(url, headers=headers, params=params)
    # Parse the API response and yield the properties of each result

    # Parse the response JSON data
    _data = r.json()
    # Yield the properties of each result in the API response
    while _data is not None:
        if "results" in _data:
            _objects: List[Dict[str, Any]] = []
            for _result in _data["results"]:
                _obj = _result.get("properties", _result)
                if "id" not in _obj and "id" in _result:
                    # Move id from properties to top level
                    _obj["id"] = _result["id"]
                if "associations" in _result:
                    for association in _result["associations"]:
                        __values = [
                            {
                                "value": _obj["hs_object_id"],
                                f"{association}_id": __r["id"],
                            }
                            for __r in _result["associations"][association]["results"]
                        ]

                        # remove duplicates from list of dicts
                        __values = [
                            dict(t) for t in {tuple(d.items()) for d in __values}
                        ]

                        _obj[association] = __values
                _objects.append(_obj)
            yield _objects, list(extract_property_history(_data["results"]))

        # Follow pagination links if they exist
        _next = _data.get("paging", {}).get("next", None)
        if _next:
            next_url = _next["link"]
            # Get the next page response
            r = requests.get(next_url, headers=headers)
            _data = r.json()
        else:
            _data = None


def fetch_data(
    endpoint: str, api_key: str, params: Optional[Dict[str, Any]] = None
) -> Iterator[List[Dict[str, Any]]]:
    """Fetch data objects from the hubspot API.
    Same as `fetch_data_with_history` but does not include history entries.
    """
    for page, _ in fetch_data_with_history(endpoint, api_key, params=params):
        yield page


def _get_property_names(api_key: str, object_type: str) -> List[str]:
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
    endpoint = f"/crm/v3/properties/{OBJECT_TYPE_PLURAL[object_type]}"

    for page in fetch_data(endpoint, api_key):
        properties.extend([prop["name"] for prop in page])

    return properties

"""Hubspot source helpers"""

from typing import Union

import urllib.parse
from typing import Any, Dict, Iterator, List, Optional

from dlt.common import logger
from dlt.common.schema.typing import TColumnSchema
from dlt.sources.helpers import requests

from .settings import (
    CRM_ASSOCIATIONS_ENDPOINT,
    CRM_SEARCH_ENDPOINT,
    OBJECT_TYPE_PLURAL,
    HS_TO_DLT_TYPE,
)

BASE_URL = "https://api.hubapi.com/"


class SearchOutOfBoundsException(Exception):
    pass


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


def pagination(
    _data: Dict[str, Any], headers: Dict[str, Any]
) -> Optional[Dict[str, Any]]:
    _next = _data.get("paging", {}).get("next", None)
    # _next = False
    if _next:
        next_url = _next["link"]
        # Get the next page response
        r = requests.get(next_url, headers=headers)
        return r.json()  # type: ignore
    else:
        return None


def search_pagination(
    url: str,
    _data: Dict[str, Any],
    headers: Dict[str, Any],
    params: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    _after = _data.get("paging", {}).get("next", {}).get("after", False)
    if _after and _after != "10000":
        # Get the next page response
        r = requests.post(url, headers=headers, json={**params, "after": _after})
        return r.json()  # type: ignore
    else:
        return None


def extract_association_data(
    _obj: Dict[str, Any],
    data: Dict[str, Any],
    association: str,
    headers: Dict[str, Any],
) -> List[Dict[str, Any]]:
    values = []

    while data is not None:
        for r in data["results"]:
            values.append(
                {
                    "value": _obj["hs_object_id"],
                    f"{association}_id": r["id"],
                }
            )
        data = pagination(data, headers)
    return values


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


def fetch_property_history(
    endpoint: str,
    api_key: str,
    props: str,
    params: Optional[Dict[str, Any]] = None,
) -> Iterator[List[Dict[str, Any]]]:
    """Fetch property history from the given CRM endpoint.

    Args:
        endpoint: The endpoint to fetch data from, as a string.
        api_key: The API key to use for authentication, as a string.
        props: A comma separated list of properties to retrieve the history for
        params: Optional dict of query params to include in the request

    Yields:
         List of property history entries (dicts)
    """
    # Construct the URL and headers for the API request
    url = get_url(endpoint)
    headers = _get_headers(api_key)

    params = dict(params or {})
    params["propertiesWithHistory"] = props
    params["limit"] = 50
    # Make the API request
    r = requests.get(url, headers=headers, params=params)
    # Parse the API response and yield the properties of each result

    # Parse the response JSON data
    _data = r.json()
    while _data is not None:
        if "results" in _data:
            yield list(extract_property_history(_data["results"]))

        # Follow pagination links if they exist
        _next = _data.get("paging", {}).get("next", None)
        if _next:
            next_url = _next["link"]
            # Get the next page response
            r = requests.get(next_url, headers=headers)
            _data = r.json()
        else:
            _data = None


def search_data_since(
    endpoint: str,
    api_key: str,
    last_modified: str,
    last_modified_prop: str,
    props: List[str],
    associations: Optional[List[str]] = None,
    context: Optional[Dict[str, Any]] = None,
) -> Iterator[List[Dict[str, Any]]]:
    """
    Fetch data from the HUBSPOT search endpoint, based on a given root endpoint, using a specified
    API key and yield the properties of each result. This function yields results from a last modified
    point in time based on the provided last modified property.

    Args:
        endpoint (str): The root endpoint to fetch data from, as a string.
        api_key (str): The API key to use for authentication, as a string.
        last_modified (str): The date from which to start the search, as a string in ISO format.
        last_modified_prop (str): The property used to check the last modified date against, as a string.
        props: The list of properties to include for the object in the request.
        associations: Optional dict of associations to search for for each object.
        context (Optional[Dict[str, Any]]): Additional data which need to be added in the resulting page.

    Yields:
        A List of CRM object dicts

    Raises:
        requests.exceptions.HTTPError: If the API returns an HTTP error status code.

    Notes:
        This function uses the `requests` library to make a POST request to the specified endpoint, with
        the API key included in the headers. If the API returns a non-successful HTTP status code (e.g.
        404 Not Found), a `requests.exceptions.HTTPError` exception will be raised.

        The `endpoint` argument should be a relative URL, which will be modified to a search endpoint
        and then appended to the base URL for the API. `last_modified`, `last_modified_prop`, and `props`
        are used to pass additional parameters to the request
    """
    # Construct the URL and headers for the API request
    url = get_url(CRM_SEARCH_ENDPOINT.format(crm_endpoint=endpoint))
    headers = _get_headers(api_key)
    body: Dict[str, Any] = {
        "properties": sorted(props),
        "limit": 200,
        "filterGroups": [
            {
                "filters": [
                    {
                        "propertyName": last_modified_prop,
                        "operator": "GTE",
                        "value": last_modified,
                    }
                ]
            }
        ],
        "sorts": [{"propertyName": last_modified_prop, "direction": "ASCENDING"}],
    }

    # Make the API request
    r = requests.post(url, headers=headers, json=body)
    # Parse the API response and yield the properties of each result
    # Parse the response JSON data
    _data = r.json()

    _total = _data.get("total", 0)
    logger.info(f"Getting {_total} new objects from {url} starting at {last_modified}")
    _max_last_modified = last_modified
    # Yield the properties of each result in the API response
    while _data is not None:
        if "results" in _data:
            for _result in _data["results"]:
                if _result["updatedAt"]:
                    _max_last_modified = max(_max_last_modified, _result["updatedAt"])
            yield _data_to_objects(
                _data, endpoint, headers, associations=associations, context=context
            )

        # Follow pagination links if they exist
        _data = search_pagination(url, _data, headers, body)

    if _total > 9999:
        if _max_last_modified == last_modified:
            raise SearchOutOfBoundsException
        logger.info(f"Starting new search iteration at {_max_last_modified}")
        yield from search_data_since(
            endpoint,
            api_key,
            _max_last_modified,
            last_modified_prop,
            props,
            associations,
            context,
        )


def fetch_data(
    endpoint: str,
    api_key: str,
    params: Optional[Dict[str, Any]] = None,
    context: Optional[Dict[str, Any]] = None,
) -> Iterator[List[Dict[str, Any]]]:
    """
    Fetch data from HUBSPOT endpoint using a specified API key and yield the properties of each result.
    For paginated endpoint this function yields item from all pages.

    Args:
        endpoint (str): The endpoint to fetch data from, as a string.
        api_key (str): The API key to use for authentication, as a string.
        params: Optional dict of query params to include in the request.
        context (Optional[Dict[str, Any]]): Additional data which need to be added in the resulting page.

    Yields:
        A List of CRM object dicts

    Raises:
        requests.exceptions.HTTPError: If the API returns an HTTP error status code.

    Notes:
        This function uses the `requests` library to make a GET request to the specified endpoint, with
        the API key included in the headers. If the API returns a non-successful HTTP status code (e.g.
        404 Not Found), a `requests.exceptions.HTTPError` exception will be raised.

        The `endpoint` argument should be a relative URL, which will be appended to the base URL for the
        API. The `params` argument is used to pass additional query parameters to the request.

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
            yield _data_to_objects(_data, endpoint, headers, context=context)

        # Follow pagination links if they exist
        _data = pagination(_data, headers)


def _data_to_objects(
    data: Any,
    endpoint: str,
    headers: Dict[str, str],
    associations: Optional[List[str]] = None,
    context: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    _objects: List[Dict[str, Any]] = []
    for _result in data["results"]:
        _obj = _result.get("properties", _result)
        if "id" not in _obj and "id" in _result:
            # Move id from properties to top level
            _obj["id"] = _result["id"]
        if "associations" in _result:
            for association in _result["associations"]:
                __data = _result["associations"][association]
                _add_association_data(__data, association, headers, _obj)
        elif associations is not None:
            for association in associations:
                __endpoint = get_url(
                    CRM_ASSOCIATIONS_ENDPOINT.format(
                        crm_endpoint=endpoint,
                        object_id=_result["id"],
                        association=association,
                    )
                )
                r = requests.get(__endpoint, headers=headers, params={"limit": 500})
                __data = r.json()
                _add_association_data(__data, association, headers, _obj)
        if context:
            _obj.update(context)
        _objects.append(_obj)
    return _objects


def _add_association_data(
    data: Any, association: str, headers: Dict[str, str], obj: Any
) -> None:
    __values = extract_association_data(obj, data, association, headers)

    # remove duplicates from list of dicts
    __values = [dict(t) for t in {tuple(d.items()) for d in __values}]

    obj[association] = __values


def _get_property_names_types(
    api_key: str, object_type: str
) -> Dict[str, Union[str, None]]:
    """
    Retrieve property names and their types if present for a given entity from the HubSpot API.

    Args:
        entity: The entity name for which to retrieve property names.

    Returns:
        A dict of propery names and their types if present.

    Raises:
        Exception: If an error occurs during the API request.
    """
    props_to_type: Dict[str, str] = {}
    endpoint = f"/crm/v3/properties/{OBJECT_TYPE_PLURAL[object_type]}"

    for page in fetch_data(endpoint, api_key):
        for prop in page:
            props_to_type[prop["name"]] = prop.get("type", None)

    return props_to_type


def get_properties_labels(
    api_key: str, object_type: str, property_name: str
) -> Iterator[Dict[str, Any]]:
    endpoint = f"/crm/v3/properties/{object_type}/{property_name}"
    url = get_url(endpoint)
    headers = _get_headers(api_key)
    r = requests.get(url, headers=headers)
    _data: Optional[Dict[str, Any]] = r.json()
    while _data is not None:
        yield _data
        _data = pagination(_data, headers)


def _to_dlt_columns_schema(col: Dict[str, str]) -> TColumnSchema:
    """Converts hubspot column to dlt column schema that will be
    used as a column hint."""
    col_name, col_type = next(iter(col.items()))
    # NOTE: if col_type is not in HS_TO_DLT_TYPE, we return an empty dict.
    # Downstream, this means no column hints are provided for this property.
    return (
        {"name": col_name, "data_type": HS_TO_DLT_TYPE[col_type]}
        if col_type in HS_TO_DLT_TYPE
        else {}
    )

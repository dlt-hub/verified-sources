"""Strapi source helpers"""

import math
from typing import Iterable

from dlt.common.typing import TDataItem
from dlt.sources.helpers import requests


def get_endpoint(token: str, domain: str, endpoint: str) -> Iterable[TDataItem]:
    """
    A generator that yields data from a paginated API endpoint.

    Args:
        token (str): The access token for the API.
        domain (str): The domain name of the API.
        endpoint (str): The API endpoint to query, defaults to ''.

    Yields:
        TDataItem: A data item from the API endpoint.
    """
    api_endpoint = f"https://{domain}/api/{endpoint}"
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {token}"}
    page_size = 25
    params = {
        "pagination[start]": 0,
        "pagination[limit]": page_size,
        "pagination[withCount]": 1,
    }

    # get the total number of pages
    response = requests.get(api_endpoint, headers=headers, params=params)
    total_results = response.json()["meta"]["pagination"]["total"]
    pages_total = math.ceil(total_results / page_size)

    # yield page by page
    for page_number in range(pages_total):
        params["pagination[start]"] = page_number * page_size
        response = requests.get(api_endpoint, headers=headers, params=params)
        data = response.json().get("data")
        if data:
            yield from data

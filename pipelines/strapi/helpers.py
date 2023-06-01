import math
from typing import Iterable

from dlt.common.typing import TDataItem
from dlt.extract.source import DltResource, DltSource
from dlt.sources.helpers import requests


def get_endpoint(token: str, domain: str, endpoint: str) -> Iterable[TDataItem]:
    """
    A generator that yields data from a paginated API endpoint.

    :param token: The access token for the API.
    :type token: str
    :param domain: The domain name of the API.
    :type domain: str
    :param endpoint: The API endpoint to query, defaults to ''.
    :type endpoint: str
    :yield: A list of data from the API endpoint.
    :rtype: list
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

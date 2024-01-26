"""Chess source helpers"""

import re
from datetime import date
from typing import Dict, List, Iterator

from dlt.common.typing import DictStrAny, DictStrStr
from dlt.sources.helpers import requests

from .settings import BASE_URL, HEADERS


def get_url_with_retry(url: str, params: DictStrStr) -> DictStrAny:
    r = requests.get(url, headers=HEADERS, params=params)
    r.raise_for_status()
    return r.json()  # type: ignore


def get_stats_with_retry(api_path: str, params: DictStrStr) -> List[DictStrAny]:
    url = f"{BASE_URL}{api_path}"
    response = get_url_with_retry(url, params)
    return response.get("d")  # type: ignore


def parse_response(
    response: List[DictStrAny], site_url: str, page: str = None
) -> Iterator[DictStrAny]:
    """
    Adds site_url from the request to the response.
    Otherwise, we would not know to which site_url a page and its statistics belong.
    Further, corrects that what the API returns as 'Query' is actually the 'page'.
    It parses response from GetPageStats API:
    >>> list(_parse_response(response=[{
      "__type": "QueryStats:#Microsoft.Bing.Webmaster.Api",
      "AvgClickPosition": 1,
      "AvgImpressionPosition": 1,
      "Clicks": 100,
      "Date": "/Date(1700179200000)/",
      "Impressions": 1000,
      "Query": "https://dlthub.com/why/"
    }], site_url="dlthub.com"))
    [{'AvgClickPosition': 1, 'AvgImpressionPosition': 1, 'Clicks': 100, 'Date': datetime.date(2023, 11, 17), 'Impressions': 1000, 'page': 'https://dlthub.com/why/', 'site_url': 'dlthub.com'}]

    It parses response from GetPageQueryStats API:
    >>> list(_parse_response(response=[{
      "__type": "QueryStats:#Microsoft.Bing.Webmaster.Api",
      "AvgClickPosition": 1,
      "AvgImpressionPosition": 1,
      "Clicks": 100,
      "Date": "/Date(1700179200000)/",
      "Impressions": 1000,
      "Query": "dlt documentation"
    }], site_url="dlthub.com", page="https://dlthub.com/docs/intro"))
    [{'AvgClickPosition': 1, 'AvgImpressionPosition': 1, 'Clicks': 100, 'Date': datetime.date(2023, 11, 17), 'Impressions': 1000, 'Query': 'dlt documentation', 'page': 'https://dlthub.com/docs/intro', 'site_url': 'dlthub.com'}]
    """
    for r in response:
        if page is None:
            # in GetPageStats endpoint the page is under the key "Query"
            r.update({"page": r.get("Query")})
            del r["Query"]
        else:
            r.update({"page": page})
        r.update({"site_url": site_url, "Date": _parse_date(r)})
        del r["__type"]
        yield r


def _parse_date(record: DictStrStr) -> date:
    """Parses Microsoft's date format into a date. The number is a unix timestamp
    >>> _parse_date({'Date': '/Date(1700179200000)/'})
    datetime.date(2023, 11, 17)
    """
    match = re.findall(r"\d+", record.get("Date"))  # extract the digits
    timestamp_in_seconds = int(match[0]) // 1000
    return date.fromtimestamp(timestamp_in_seconds)

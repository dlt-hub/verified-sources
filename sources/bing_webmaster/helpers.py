"""Bing Webmaster source helpers"""

import re
from typing import Iterator, List
from urllib.parse import urljoin

from dlt.common import logger, pendulum
from dlt.common.typing import DictStrAny, DictStrStr
from dlt.sources.helpers import requests

from .settings import BASE_URL, HEADERS


def check_api_key(api_key: str) -> None:
    if api_key is None or len(api_key) == 0:
        raise ValueError(
            """Please provide a Bing Webmaster API key in secrets.toml under
            [sources.bing_webmaster]
            api_key="your_api_key" """
        )


def get_url_with_retry(url: str, params: DictStrStr) -> DictStrAny:
    try:
        r = requests.get(url, headers=HEADERS, params=params)
        return r.json()  # type: ignore
    except requests.HTTPError as e:
        if e.response.status_code == 400:
            logger.warning(
                f"""HTTP Error {e.response.status_code}.
                Is your API key authorized to fetch data about the domain
                '{params.get('siteUrl')}'?"""
            )
        e.response.raise_for_status()
        return e.response.json()  # type: ignore


def get_stats_with_retry(api_path: str, params: DictStrStr) -> List[DictStrAny]:
    url = urljoin(BASE_URL, api_path)
    response = get_url_with_retry(url, params)
    return response.get("d")  # type: ignore


def parse_response(
    response: List[DictStrAny], site_url: str, page: str = None
) -> Iterator[DictStrAny]:
    """
    Adds site_url from the request to the response.
    Otherwise, we would not know to which site_url a page and its statistics belong.
    Further, corrects that what the API returns as 'Query' is actually the 'page'.
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


def _parse_date(record: DictStrStr) -> pendulum.Date:
    """Parses Microsoft's date format into a date. The number is a unix timestamp"""
    match = re.findall(r"\d+", record.get("Date"))  # extract the digits
    timestamp_in_seconds = int(match[0]) // 1000
    d: pendulum.Date = pendulum.Date.fromtimestamp(timestamp_in_seconds)
    return d

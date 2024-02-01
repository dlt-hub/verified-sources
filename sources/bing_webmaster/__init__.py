"""
A source loading history of organic search traffic from Bing Webmaster API
See documentation: https://learn.microsoft.com/en-us/dotnet/api/microsoft.bing.webmaster.api.interfaces.iwebmasterapi?view=bing-webmaster-dotnet
"""

import time
from typing import Iterable, Iterator, List, Sequence

import dlt
from dlt.common.typing import DictStrAny, DictStrStr
from dlt.sources import DltResource

from .helpers import get_stats_with_retry, parse_response


@dlt.source(name="bing_webmaster")
def source(
    site_urls: List[str] = None, site_url_pages: Iterable[DictStrStr] = None
) -> Sequence[DltResource]:
    """
    A dlt source for the Bing Webmaster api.
    It groups resources for the APIs which return organic search traffic statistics
    Args:
        site_urls: List[str]: A list of site_urls, e.g, ["dlthub.com", "dlthub.de"]. Use this if you need the weekly traffic per site_url and page
        site_url_pages: Iterable[DictStrStr]: A list of pairs of site_url and page. Use this if you need the weekly traffic per site_url, page, and query
    Returns:
        Sequence[DltResource]: A sequence of resources that can be selected from including page_stats and page_query_stats.
    """
    return (
        page_stats(site_urls),
        page_query_stats(site_url_pages),
    )


@dlt.resource(
    write_disposition="merge",
    merge_key=("date", "page", "site_url"),
    primary_key=("date", "page", "site_url"),
    table_name="bing_page_stats",
)
def page_stats(
    site_urls: List[str], bing_webmaster_api_key: str = dlt.secrets.value
) -> Iterator[Iterator[DictStrAny]]:
    """
    Get detailed traffic statistics for top pages belonging to a site_url
    API documentation:
    https://learn.microsoft.com/en-us/dotnet/api/microsoft.bing.webmaster.api.interfaces.iwebmasterapi.getpagestats
    Args:
        site_urls (List[str]): List of site_urls to retrieve statistics for.
    Yields:
        Iterator[Dict[str, Any]]: An iterator over list of organic traffic statistics.
    """
    api_path = "GetPageStats"
    for site_url in site_urls:
        params = {"siteUrl": site_url, "apikey": bing_webmaster_api_key}
        print(f"Fetching for site_url: {site_url}")
        response = get_stats_with_retry(api_path, params)
        if len(response) > 0:
            yield parse_response(response, site_url)


@dlt.resource(
    write_disposition="merge",
    merge_key=("date", "page", "site_url", "query"),
    primary_key=("date", "page", "site_url", "query"),
    table_name="bing_page_query_stats",
)
def page_query_stats(
    site_url_pages: Iterable[DictStrStr],
    bing_webmaster_api_key: str = dlt.secrets.value,
) -> Iterator[Iterator[DictStrAny]]:
    """
    Yields weekly statistics and queries for each pair of page and site_url.
    Contains the entire available history. Thus, we recommend to use write_disposition="merge"
    API documentation:
    https://learn.microsoft.com/en-us/dotnet/api/microsoft.bing.webmaster.api.interfaces.iwebmasterapi.getpagequerystats

    Args:
        site_url_page (Iterable[DictStrStr]): Iterable of site_url and pages to retrieve statistics for. Can be result of a SQL query, a parsed sitemap, etc.
    Yields:
        Iterator[Dict[str, Any]]: An iterator over list of organic traffic statistics.
    """
    api_path = "GetPageQueryStats"
    for record in site_url_pages:
        time.sleep(0.5)  # this avoids rate limit observed after dozens of requests
        site_url = record.get("site_url")
        page = record.get("page")
        params = {"siteUrl": site_url, "page": page, "apikey": bing_webmaster_api_key}
        print(f"Fetching for site_url: {site_url}, page: {page}")
        response = get_stats_with_retry(api_path, params)
        if len(response) > 0:
            yield parse_response(response, site_url, page)

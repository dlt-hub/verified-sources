"""
A source loading history of organic search traffic from Bing Webmaster API
See documentation: https://learn.microsoft.com/en-us/dotnet/api/microsoft.bing.webmaster.api.interfaces.iwebmasterapi?view=bing-webmaster-dotnet
"""

from typing import Any, Dict, Iterator, List, Sequence

import dlt
from dlt.sources import DltResource
from dlt.common.typing import TDataItem
import time

from .helpers import get_stats_with_retry, parse_response


@dlt.source(name="bing_webmaster")
def source(
    site_urls: List[str] = None, site_url_pages: Iterator[Dict[str, str]] = None
) -> Sequence[DltResource]:
    """
    A dlt source for the Bing Webmaster api.
    It groups resources for the APIs which return organic search traffic statistics
    Args:
        site_urls: List[str]: A list of site_urls, e.g, ["dlthub.com", "dlthub.de"]. Use this if you need the weekly traffic per site_url and page
        site_url_pages: Iterator[Dict[str, str]]: A list of pairs of site_url and page. Use this if you need the weekly traffic per site_url, page, and query
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
) -> Iterator[List[TDataItem]]:
    """
    Get detailed traffic statistics for top pages belonging to a site_url
    API documentation:
    https://learn.microsoft.com/en-us/dotnet/api/microsoft.bing.webmaster.api.interfaces.iwebmasterapi.getpagestats
    Args:
        players (List[str]): List of player usernames to retrieve archives for.
    Yields:
        Iterator[List[TDataItem]]: An iterator over list of player archive data.
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
    site_url_pages: Iterator[Dict[str, str]],
    bing_webmaster_api_key: str = dlt.secrets.value,
) -> Iterator[List[TDataItem]]:
    """
    Yields weekly statistics and queries for each pair of page and site_url.
    Contains the entire available history. Thus, we recommend to use write_disposition="merge"
    API documentation:
    https://learn.microsoft.com/en-us/dotnet/api/microsoft.bing.webmaster.api.interfaces.iwebmasterapi.getpagequerystats

    As an input, use `helpers.site_url_pages`

    Args:
        players (List[str]): List of player usernames to retrieve archives for.
    Yields:
        Iterator[List[TDataItem]]: An iterator over list of player archive data.
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


@dlt.source
def chess_dlt_config_example(
    secret_str: str = dlt.secrets.value,
    secret_dict: Dict[str, Any] = dlt.secrets.value,
    config_int: int = dlt.config.value,
) -> DltResource:
    """
    An example of a source that uses dlt to provide secrets and config values.
    Args:
        secret_str (str, optional): Secret string provided by dlt.secrets.value. Defaults to dlt.secrets.value.
        secret_dict (Dict[str, Any], optional): Secret dictionary provided by dlt.secrets.value. Defaults to dlt.secrets.value.
        config_int (int, optional): Config integer provided by dlt.config.value. Defaults to dlt.config.value.
    Returns:
        DltResource: Returns a resource yielding the configured values.
    """
    print(secret_str)
    print(secret_dict)
    print(config_int)

    # returns a resource yielding the configured values - it is just a test
    return dlt.resource([secret_str, secret_dict, config_int], name="config_values")


# if __name__ == "__main__":
#     import doctest

#     doctest.testmod()

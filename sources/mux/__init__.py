"""Loads Mux views data using https://docs.mux.com/api-reference"""

from typing import Iterable

import dlt
from dlt.common import pendulum
from dlt.sources.helpers import requests
from requests.auth import HTTPBasicAuth
from dlt.common.typing import TDataItem
from dlt.sources import DltResource

from .settings import API_BASE_URL, DEFAULT_LIMIT


@dlt.source
def mux_source() -> Iterable[DltResource]:
    """
    Source function that passes all video assets and every video view from yesterday to be loaded.

    Yields:
        DltResource: Video assets and video views to be loaded.
    """
    yield assets_resource
    yield views_resource


@dlt.resource(write_disposition="merge")
def assets_resource(
    mux_api_access_token: str = dlt.secrets.value,
    mux_api_secret_key: str = dlt.secrets.value,
    limit: int = DEFAULT_LIMIT,
) -> Iterable[TDataItem]:
    """
    Resource function that yields metadata about every asset to be loaded.

    Args:
        mux_api_access_token (str): API access token for Mux.
        mux_api_secret_key (str): API secret key for Mux.
        limit (int): Limit on the number of assets to retrieve. Defaults to DEFAULT_LIMIT.

    Yields:
        TDataItem: Data of each asset.
    """
    url = f"{API_BASE_URL}/video/v1/assets"
    params = {"limit": limit}

    response = requests.get(
        url, params=params, auth=HTTPBasicAuth(mux_api_access_token, mux_api_secret_key)
    )
    response.raise_for_status()
    yield response.json()["data"]


@dlt.resource(write_disposition="append")
def views_resource(
    mux_api_access_token: str = dlt.secrets.value,
    mux_api_secret_key: str = dlt.secrets.value,
    limit: int = DEFAULT_LIMIT,
) -> Iterable[DltResource]:
    """
    Resource function that yields metadata about every video view from yesterday to be loaded.

    Args:
        mux_api_access_token (str): API access token for Mux.
        mux_api_secret_key (str): API secret key for Mux.
        limit (int): Limit on the number of video views to retrieve. Defaults to DEFAULT_LIMIT.

    Yields:
        DltResource: Data for each video view from yesterday.
    """
    url = f"{API_BASE_URL}/data/v1/video-views"
    page = 1
    today = pendulum.today()
    yest_start = today.subtract(days=1).int_timestamp
    yest_end = today.int_timestamp

    while True:
        params = {"limit": limit, "page": page, "timeframe[]": [yest_start, yest_end]}
        response = requests.get(
            url,
            params=params,  # type: ignore
            auth=HTTPBasicAuth(mux_api_access_token, mux_api_secret_key),
        )
        response.raise_for_status()
        if response.json()["data"] == []:
            break
        yield response.json()["data"]
        page += 1

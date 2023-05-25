"""Loads Mux views data using https://docs.mux.com/api-reference"""

import dlt
from dlt.common import pendulum
from dlt.sources.helpers import requests
from requests.auth import HTTPBasicAuth

API_BASE_URL = 'https://api.mux.com'


@dlt.source
def mux_source(mux_api_access_token=dlt.secrets.value, mux_api_secret_key=dlt.secrets.value):
    """Passes all video assets and every video view from yesterday to be loaded"""
    yield assets_resource(mux_api_access_token, mux_api_secret_key)
    yield views_resource(mux_api_access_token, mux_api_secret_key)


@dlt.resource(write_disposition="merge")
def assets_resource(mux_api_access_token=dlt.secrets.value, mux_api_secret_key=dlt.secrets.value, limit=100):
    """Passes metadata about every asset to be loaded"""
    url = f"{API_BASE_URL}/video/v1/assets"
    params = { "limit": limit }

    response = requests.get(url, params=params, auth=HTTPBasicAuth(mux_api_access_token, mux_api_secret_key))
    response.raise_for_status()
    yield response.json()["data"]


@dlt.resource(write_disposition="append")
def views_resource(mux_api_access_token=dlt.secrets.value, mux_api_secret_key=dlt.secrets.value, limit = 100):
    """Passes metadata about every video view from yesterday to be loaded"""
    url = f"{API_BASE_URL}/data/v1/video-views"
    page = 1
    today = pendulum.today()
    yest_start = today.subtract(days=1).int_timestamp
    yest_end = today.int_timestamp

    while True:
        params = { "limit": limit, "page": page, "timeframe[]": [yest_start, yest_end] }
        response = requests.get(url, params=params, auth=HTTPBasicAuth(mux_api_access_token, mux_api_secret_key))
        response.raise_for_status()
        if response.json()["data"] == []:
            break
        else:
            yield response.json()["data"]
            page += 1
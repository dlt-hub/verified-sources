"""Loads Mux data using https://docs.mux.com/api-reference"""
import dlt
import time
from dlt.sources.helpers import requests
from requests.auth import HTTPBasicAuth
from datetime import datetime, timedelta

API_BASE_URL = 'https://api.mux.com'


@dlt.source
def mux_source(MUX_API_ACCESS_TOKEN=dlt.secrets.value, MUX_API_SECRET_KEY=dlt.secrets.value):
    """Passes all video assets and every video view from yesterday to be loaded"""
    yield assets_resource(MUX_API_ACCESS_TOKEN, MUX_API_SECRET_KEY)
    yield views_resource(MUX_API_ACCESS_TOKEN, MUX_API_SECRET_KEY)


@dlt.resource(write_disposition="merge")
def assets_resource(MUX_API_ACCESS_TOKEN=dlt.secrets.value, MUX_API_SECRET_KEY=dlt.secrets.value):
    """Passes metadata about every asset to be loaded"""
    url = f"{API_BASE_URL}/video/v1/assets"
    limit = 100 # loads only 100 assets
    params = { "limit": limit }

    response = requests.get(url, params=params, auth=HTTPBasicAuth(MUX_API_ACCESS_TOKEN, MUX_API_SECRET_KEY))
    response.raise_for_status()
    yield response.json()["data"]


@dlt.resource(write_disposition="append")
def views_resource(MUX_API_ACCESS_TOKEN=dlt.secrets.value, MUX_API_SECRET_KEY=dlt.secrets.value):
    """Passes metadata about every video view from yesterday to be loaded"""
    url = f"{API_BASE_URL}/data/v1/video-views"
    limit = 100
    page = 1
    today = datetime.utcnow().date()
    yest_start = int(time.mktime((today - timedelta(1)).timetuple()))
    yest_end = int(time.mktime(today.timetuple()))

    while True:
        params = { "limit": limit, "page": page, "timeframe[]": [yest_start, yest_end] }
        response = requests.get(url, params=params, auth=HTTPBasicAuth(MUX_API_ACCESS_TOKEN, MUX_API_SECRET_KEY))
        response.raise_for_status()
        if response.json()["data"] == []:
            break
        else:
            yield response.json()["data"]
            page += 1
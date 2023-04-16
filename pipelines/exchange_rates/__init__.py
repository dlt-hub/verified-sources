import datetime
import json
from datetime import datetime, timedelta
from urllib.parse import urlencode, urlunparse

import arrow
import dlt
import requests


@dlt.source
def exchangerates_source(
    currency_list,
    base_currency,
    exchangerates_api_key=dlt.secrets.value,
):
    """Data source function that retrieves exchange rates for a given list of currencies,
    base currency, and last updated date using the exchangerates API.

    Args:
        currency_list (list): A list of currency codes (e.g., ["AUD", "BRL", "CAD"]).
        base_currency (str, optional): The base currency to convert from (default is "EUR").
        exchangerates_api_key (str, optional): The API key for the exchangerates API. Defaults to the value stored in the pipeline secrets.

    Returns:
        Generator: A generator that yields JSON response objects from the exchangerates API.
    """
    yield exchangerates_resource(
        currency_list,
        base_currency=base_currency,
        exchangerates_api_key=exchangerates_api_key,
    )


def _create_auth_headers(exchangerates_api_key):
    """Helper function that constructs Bearer type authorization header for exchangerates API using the provided API key.

    Args:
        exchangerates_api_key (str): The API key for the exchangerates API.

    Returns:
        dict: A dictionary containing the authorization headers for the exchangerates API.
    """
    headers = {"apikey": f"{exchangerates_api_key}"}
    return headers


def _build_url(
    hostname: str, path: str, endpoint: str, scheme: str = "https", query: dict = None
) -> str:
    """
    Builds a URL string using the given hostname, path, endpoint, scheme and query parameters.

    Args:
        hostname (str): The hostname for the URL.
        path (str): The path for the URL.
        endpoint (str): The endpoint for the URL.
        scheme (str, optional): The scheme for the URL. Defaults to "https".
        query (dict, optional): A dictionary containing query parameters for the URL. Defaults to None.

    Returns:
        str: A string representation of the built URL.
    """
    query = urlencode(query) if query is not None else None
    url = urlunparse((scheme, hostname, path + endpoint, "", query, ""))
    return url


@dlt.resource(write_disposition="append")
def exchangerates_resource(
    currency_list: list,
    base_currency: str,
    exchangerates_api_key: str = dlt.secrets.value,
) -> dict:
    """
    Retrieves the exchange rates for the given currency list and base currency, starting from the given last_updated_at date.

    Args:
        currency_list (list): A list of currency codes for which to retrieve exchange rates.
        base_currency (str): The base currency for which to retrieve exchange rates.
        exchangerates_api_key (str, optional): The API key for the ExchangeRates API. Defaults to dlt.secrets.value.

    Yields:
        dict: A dictionary containing the exchange rates for the given date and currency list.
    """
    headers = _create_auth_headers(exchangerates_api_key)
    payload = {}
    now = arrow.get()
    last_updated_at = dlt.current.state().setdefault("date", "2023-04-10T00:00:00Z")
    date = arrow.get(last_updated_at)
    if date.date() < now.date():
        while date.date() <= now.date():
            url = _build_url(
                "api.apilayer.com",
                "exchangerates_data/",
                date.date().isoformat(),
                query=dict(symbols=",".join(currency_list), base=base_currency),
            )
            date = date.shift(days=1)
            res = requests.get(url, headers=headers, data=payload)
            json_object = res.json()
            yield (
                {
                    "to": base_currency,
                    "date": json_object["date"],
                    "rate": f'{(1 / json_object["rates"][rate]):.10f}',
                    "from": rate,
                }
                for rate in json_object["rates"]
            )
            dlt.current.state()["date"] = json_object["date"]

from typing import Any, Union, Generator, Optional
import logging

import requests
import dlt
from pendulum import DateTime, datetime, parse


@dlt.source(name="workable")
def workable_source(access_token=dlt.secrets.value, config=dlt.config.value, start_date: Optional[DateTime] = None, fetch: bool = False,):
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {access_token}"
    }
    url = config["subdomain_url"]
    start_date_unix = start_date.isoformat() if start_date is not None else datetime(2000, 1, 1).isoformat()

    if fetch:
        @dlt.resource(write_disposition="merge", primary_key="id")
        def fetch_candidates_resource(
            updated_at: Optional[Any] = dlt.sources.incremental("updated_at", initial_value=start_date_unix)
        ) -> list:
            logging.warning("Fetching data by 'updated_at'. Loading modified and new data...")
            params = {"updated_after": updated_at.last_value}
            yield dlt.mark.with_table_name(pagination(url, "candidates", headers, params), "candidates")

        return fetch_candidates_resource()
    else:
        @dlt.resource(write_disposition="append", primary_key="id")
        def load_candidates_resource(
            created_at: Optional[Any] = dlt.sources.incremental("created_at", initial_value=start_date_unix),
        ) -> list:
            logging.warning("'fetch' is False. Loading only new data by 'created_at'...")
            params = {"created_after": created_at.last_value}
            yield dlt.mark.with_table_name(pagination(url, "candidates", headers, params), "candidates")

        return load_candidates_resource()


@dlt.resource
def workable_events(headers: dict, config: dict) -> list:
    url = f"{config['subdomain_url']}/events"
    response = requests.get(url, headers=headers)

    print(response.text)

    yield response.json()["events"]


def pagination(url: str, endpoint: str, headers: dict, params: dict) -> Generator:
    url = f"{url}/{endpoint}"

    has_more = True
    while has_more:
        response = requests.get(url, headers=headers, params=params)
        response_json = response.json()
        paging = response_json.get("paging")

        if paging is not None:
            url = paging.get("next")
        else:
            has_more = False

        yield response_json[endpoint]


def transform_date(date: Union[str, DateTime, int]) -> int:
    if isinstance(date, str):
        date = parse(date)
    if isinstance(date, DateTime):
        # convert to unix timestamp
        date = int(date.timestamp())
    return date



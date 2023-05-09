from typing import Any, Dict, Generator, Optional

import requests
import dlt
from pendulum import DateTime, datetime, parse


@dlt.source(name="workable")
def workable_source(access_token=dlt.secrets.value, config=dlt.config.value):
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {access_token}"
    }

    return candidates_resource(headers, config)


@dlt.resource
def workable_events(headers: dict, config: dict) -> list:
    url = f"{config['subdomain_url']}/events"
    response = requests.get(url, headers=headers)

    print(response.text)

    yield response.json()["events"]


@dlt.resource(name="candidates", write_disposition="merge", primary_key="id")
def candidates_resource(
    headers: dict,
    config: dict,
    updated_at: Optional[Any] = dlt.sources.incremental("updated_at", initial_value=datetime(2000, 1, 1))
) -> list:
    url = f"{config['subdomain_url']}/candidates"

    has_more = True

    while has_more:

        response = requests.get(url, headers=headers, params={"updated_after": updated_at.last_value})
        response_json = response.json()
        paging = response_json.get("paging")
        if paging is not None:
            url = paging.get("next")
        else:
            has_more = False

        response_with_data = []
        for resp in response_json["candidates"]:
            response_with_data.append({k: (v if k != "updated_at" else parse(v)) for k, v in resp.items()})

        yield response_with_data

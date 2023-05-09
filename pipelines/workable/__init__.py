from typing import Any, Optional
import logging

import dlt
from pendulum import DateTime, datetime
from .helpers import pagination


@dlt.source(name="workable")
def workable_source(access_token=dlt.secrets.value, config=dlt.config.value, start_date: Optional[DateTime] = None,):
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {access_token}"
    }
    url = config["subdomain_url"]
    start_date_unix = start_date.isoformat() if start_date is not None else datetime(2000, 1, 1).isoformat()

    @dlt.resource(name="candidates", write_disposition="merge", primary_key="id")
    def fetch_candidates_resource(
        updated_at: Optional[Any] = dlt.sources.incremental("updated_at", initial_value=start_date_unix)
    ) -> list:
        logging.warning("Fetching data by 'updated_at'. Loading modified and new data...")
        params = {"updated_after": updated_at.last_value}
        yield pagination(url, "candidates", headers, params)

    @dlt.resource(name="jobs", write_disposition="merge", primary_key="id")
    def jobs_resource(
    ) -> list:
        yield pagination(url, "jobs", headers, {})

    return fetch_candidates_resource()


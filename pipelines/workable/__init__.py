import logging
from typing import Any, Generator, Optional

import dlt
from pendulum import DateTime

from .workable_client import WorkableClient

logging.basicConfig(format="%(asctime)s - %(message)s", level=logging.INFO)


@dlt.source
def workable_load_data(
    endpoints: tuple = ("members", "jobs"),
    credentials=dlt.secrets.value,
    start_date: Optional[DateTime] = None,
):
    workable = WorkableClient(
        credentials["access_token"], credentials["subdomain"], start_date=start_date
    )
    params = {"created_after": workable.start_date_iso}

    def load_data(endpoint: str) -> Generator[list, Any, None]:
        logging.info(
            f"Loading all data from '{endpoint}' by 'created_at' in 'replace' mode..."
        )
        yield workable.pagination(endpoint=endpoint, params=params)

    for endpoint in endpoints:
        yield dlt.resource(
            load_data,
            name=endpoint,
            write_disposition="replace",
        )(endpoint)


@dlt.source
def workable_candidates(
    credentials=dlt.secrets.value,
    start_date: Optional[DateTime] = None,
):
    workable = WorkableClient(
        credentials["access_token"], credentials["subdomain"], start_date=start_date
    )

    @dlt.resource(name="candidates", write_disposition="merge", primary_key="id")
    def fetch_candidates_resource(
        updated_at: Optional[Any] = dlt.sources.incremental(
            "updated_at", initial_value=workable.start_date_iso
        )
    ) -> list:
        logging.info(
            "Fetching data from 'candidates' by 'updated_at'. Loading modified and new data..."
        )
        yield workable.pagination(
            endpoint="candidates", params={"updated_after": updated_at.last_value}
        )

    return fetch_candidates_resource()


@dlt.source
def workable_jobs_activities(
    credentials=dlt.secrets.value,
    start_date: Optional[DateTime] = None,
):
    workable = WorkableClient(
        credentials["access_token"], credentials["subdomain"], start_date=start_date
    )

    @dlt.resource(name="jobs", write_disposition="replace", primary_key="id")
    def jobs_resource():
        yield workable.pagination(
            "jobs", params={"created_after": workable.start_date_iso}
        )

    def _get_activities(url: str, shortcode: str):
        url_with_shortcode = f"{url}/jobs/{shortcode}"

        yield workable.pagination(
            "activities",
            custom_url=url_with_shortcode,
            params={"created_after": workable.start_date_iso},
        )

    @dlt.transformer(data_from=jobs_resource)
    def jobs_activities(job_item):
        for job in job_item:
            yield _get_activities(workable.base_url, job["shortcode"])

    return jobs_activities()

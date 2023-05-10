import logging
from typing import Any, Generator, Optional, Sequence, Tuple

import dlt
from dlt.extract.source import DltResource
from pendulum import DateTime

from .workable_client import WorkableClient

logging.basicConfig(format="%(asctime)s - %(message)s", level=logging.INFO)

"""
To set up authorization for the Workable API, you will need an API access token and your subdomain name.

Follow these steps:

1. Log in to your Workable account and navigate to the API section of the Integrations page.
2. Click the "Generate new token" button to create a new API access token.
3. Copy the generated token to your clipboard.
4. Open .dlt/secrets.toml and write the token in the section [sources.workable]
5. To access data from your own Workable account, set the subdomain in the URL
   to your subdomain name, for example: https://yoursubdomain.workable.com/api/v3/.
6. Write your subdomain name in .dlt/config.toml in the section [sources.workable]
"""


@dlt.source(name="workable")
def workable_source(
    endpoints: Tuple[str],
    access_token: str = dlt.secrets.value,
    subdomain: str = dlt.config.value,
    start_date: Optional[DateTime] = None,
) -> Sequence[DltResource]:
    """
    Retrieves data from the Workable API for the specified endpoints.
    For almost all endpoints Workable API responses do not provide keys "updated_at",
    so in most cases we are forced to load the date in 'replace' mode.
    This source is suitable for all types of endpoints, including 'customers',
    but endpoint 'customers' can also be loaded in incremental mode (see source workable_incremental)

    Parameters:
        endpoints: A tuple of endpoint names to retrieve data from.
        access_token: The API access token for authentication. Defaults to the value in the `dlt.secrets` object.
        subdomain: The subdomain name for the Workable account. Defaults to the value in the `dlt.config` object.
        start_date: An optional start date to limit the data retrieved. Defaults to January 1, 2000.

    """
    workable = WorkableClient(access_token, subdomain, start_date=start_date)
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


@dlt.source(name="workable")
def workable_incremental(
    access_token: str = dlt.secrets.value,
    subdomain: str = dlt.config.value,
    start_date: Optional[DateTime] = None,
) -> DltResource:
    """
    Retrieves updated data from the Workable API for the 'candidates' endpoint in incremental mode.
    'Сandidates' are the only endpoints that have a key 'updated_at',
    which means that we can update the data incrementally,
    without duplicating data and without downloading a huge amount of data each time.

    Parameters:
        access_token: The API access token for authentication. Defaults to the value in the `dlt.secrets` object.
        subdomain: The subdomain name for the Workable account. Defaults to the value in the `dlt.config` object.
        start_date: An optional start date to limit the data retrieved. Defaults to January 1, 2000.
    """
    workable = WorkableClient(access_token, subdomain, start_date=start_date)

    @dlt.resource(name="candidates", write_disposition="merge", primary_key="id")
    def fetch_candidates_resource(
        updated_at: Optional[Any] = dlt.sources.incremental(
            "updated_at", initial_value=workable.start_date_iso
        )
    ) -> list:
        """
        The 'updated_at' parameter is managed by the dlt.sources.incremental method.
        This function is suitable only for the 'candidates' endpoint in incremental mode.
        """
        logging.info("Fetching data from 'candidates' by 'updated_at'. Loading modified and new data...")
        yield workable.pagination(endpoint="candidates", params={"updated_after": updated_at.last_value})

    return fetch_candidates_resource()


@dlt.source(name="workable")
def workable_jobs_activities(
    access_token: str = dlt.secrets.value,
    subdomain: str = dlt.config.value,
    start_date: Optional[DateTime] = None,
) -> Sequence[DltResource]:
    """
    Retrieves jobs and their activities data from the Workable API.
    Returns a transformer function that yields the activities for each job.
    For jobs, Workable API responses do not provide the "updated_at" key, so they can be loaded only in "replace" mode.
    For activities, a custom URL is constructed for each job item to retrieve the corresponding activities.
    The transformer function takes the data from the "jobs_resource" and yields the activities for each job item.

    Parameters:
        access_token: The API access token for authentication. Defaults to the value in the `dlt.secrets` object.
        subdomain: The subdomain name for the Workable account. Defaults to the value in the `dlt.config` object.
        start_date: An optional start date to limit the data retrieved. Defaults to January 1, 2000.
    """
    workable = WorkableClient(access_token, subdomain, start_date=start_date)

    @dlt.resource(name="jobs", write_disposition="replace")
    def jobs_resource():
        yield workable.pagination(
            "jobs", params={"created_after": workable.start_date_iso}
        )

    def _get_activities(url: str, shortcode: str):
        url_with_shortcode = f"{url}/jobs/{shortcode}"
        yield workable.pagination("activities", custom_url=url_with_shortcode)

    @dlt.transformer(data_from=jobs_resource)
    def jobs_activities(job_item):
        for job in job_item:
            yield _get_activities(workable.base_url, job["shortcode"])

    return jobs_resource(), jobs_activities()

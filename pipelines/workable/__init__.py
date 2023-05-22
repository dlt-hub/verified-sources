""" This pipeline uses Workable API and dlt to load data such as Candidates, Jobs, Events, etc. to the database."""

import logging
from typing import Any, Generator, Iterator, Optional

import dlt
from dlt.common.typing import TDataItem, TDataItems
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

DEFAULT_ENDPOINTS = (
    "members",
    "recruiters",
    "stages",
    "requisitions",
    "jobs",
    "custom_attributes",
    "events",
)

DEFAULT_DETAILS = {
    "candidates": (
        "activities",
        "offer",
    ),
    "jobs": (
        "activities",
        "application_form",
        "questions",
        "stages",
        "custom_attributes",
        "members",
        "recruiters",
    ),
}


@dlt.source(name="workable")
def workable_source(
    access_token: str = dlt.secrets.value,
    subdomain: str = dlt.config.value,
    start_date: Optional[DateTime] = None,
    load_details: bool = False,
) -> Iterator[DltResource]:
    """
    Retrieves data from the Workable API for the specified endpoints (DEFAULT_ENDPOINTS + 'candidates').
    For almost all endpoints, Workable API responses do not provide keys "updated_at",
    so in most cases we are forced to load the date in 'replace' mode.
    'Candidates' are the only endpoints that have a key 'updated_at', which means that we can update the data incrementally.

    Resources that depend on another resource are implemented as transformers,
    so they can re-use the original resource data without re-downloading.

    Parameters:
        access_token: The API access token for authentication. Defaults to the value in the `dlt.secrets` object.
        subdomain: The subdomain name for the Workable account. Defaults to the value in the `dlt.config` object.
        start_date: An optional start date to limit the data retrieved. Defaults to January 1, 2000.
                    It does not affect dependent resources (jobs_activities, candidates_activities, etc).
        load_details: A boolean flag enables loading data from endpoints that depend on the main endpoints.
    """
    resources = {}
    workable = WorkableClient(access_token, subdomain, start_date=start_date)
    params = {"created_after": workable.start_date_iso}

    # This resource is suitable for all types of endpoints, including 'candidates',
    # but endpoint 'candidates' can also be loaded in incremental mode (see candidates_resource).
    def load_data(endpoint: str) -> Generator[TDataItems, Any, None]:
        logging.info(
            f"Loading data from '{endpoint}' by 'created_at' in 'replace' mode."
        )
        yield workable.pagination(endpoint=endpoint, params=params)

    for endpoint in DEFAULT_ENDPOINTS:
        resources[endpoint] = dlt.resource(
            load_data,
            name=endpoint,
            write_disposition="replace",
        )(endpoint)

    yield from resources.values()

    @dlt.resource(name="candidates", write_disposition="merge", primary_key="id")
    def candidates_resource(
        updated_at: Optional[Any] = dlt.sources.incremental(
            "updated_at", initial_value=workable.start_date_iso
        )
    ) -> Generator[TDataItems, Any, None]:
        """
        The 'updated_at' parameter is managed by the dlt.sources.incremental method.
        This function is suitable only for the 'candidates' endpoint in incremental mode.
        """
        logging.info(
            "Fetching data from 'candidates' by 'updated_at'. Loading modified and new data."
        )
        yield workable.pagination(
            endpoint="candidates", params={"updated_after": updated_at.last_value}
        )

    yield candidates_resource

    if load_details:

        def _get_details(
            page: Iterator[TDataItem],
            main_endpoint: str,
            sub_endpoint_name: str,
            code_key: str,
        ) -> Generator[TDataItems, Any, None]:
            for item in page:
                yield workable.details_from_endpoint(
                    main_endpoint, item[code_key], sub_endpoint_name
                )

        # A transformer functions that yield the activities, questions, etc. for each job.
        for sub_endpoint in DEFAULT_DETAILS["jobs"]:
            logging.info(
                f"Loading additional data for 'jobs' from '{sub_endpoint}' in 'replace' mode."
            )
            yield resources["jobs"] | dlt.transformer(
                name=f"jobs_{sub_endpoint}", write_disposition="replace"
            )(_get_details)("jobs", sub_endpoint, "shortcode")

        # A transformer functions that yield the activities and offers for each candidate.
        for sub_endpoint in DEFAULT_DETAILS["candidates"]:
            logging.info(
                f"Loading additional data for 'candidates' from '{sub_endpoint}' in 'merge' mode."
            )
            yield candidates_resource | dlt.transformer(
                name=f"candidates_{sub_endpoint}", write_disposition="merge"
            )(_get_details)(
                "candidates", sub_endpoint, "id"
            )

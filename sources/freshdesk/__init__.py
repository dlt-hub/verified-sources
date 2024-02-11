from typing import Iterable, List, Optional

import dlt
from dlt.common.typing import TDataItem
from dlt.sources import DltResource, incremental

from .helpers import get_endpoint, paginated_response
from .settings import DEFAULT_ENDPOINTS


@dlt.source()
def freshdesk_source(
    endpoints: Optional[List[str]] = None,
    page: int = 1,
    per_page: int = 100,
    domain: str = dlt.config.value,
    api_secret_key: str = dlt.secrets.value,
) -> Iterable[DltResource]:
    """
    Fetches data from specified endpoints in "settings.py" from Freshdesk
    API and yields it as DltResource objects.

    This function supports pagination.

    Args:
        endpoints: An optional list of strings specifying the Freshdesk API
                   endpoints to fetch.
        page: The starting page number for API pagination.
        per_page: The number of items to fetch per page, max 100.
        domain: The Freshdesk domain.
        api_secret_key: Freshdesk API key, defaults to "secrets.toml"

    Yields:
        An iterable of DltResource objects, each representing data from one
        of the Freshdesk API endpoints.
    """
    if endpoints is None:
        endpoints = DEFAULT_ENDPOINTS

    for endpoint in endpoints:
        yield dlt.resource(
            get_endpoint(
                api_secret_key=api_secret_key,
                page=page,
                per_page=per_page,
                domain=domain,
                endpoint=endpoint,
            ),
            name=endpoint,
            write_disposition="merge",
            primary_key="id",
        )


@dlt.resource(write_disposition="merge")
def tickets(
    updated_at: incremental[str] = incremental(
        "updated_at", initial_value="2022-01-01T00:00:00Z"
    ),
    page: int = 1,
    per_page: int = 100,  # Can be set to max 100
    domain: str = dlt.config.value,
    api_secret_key: str = dlt.secrets.value,
    endpoint: str = "tickets",
) -> Iterable[TDataItem]:
    """
    Retrieves Freshdesk tickets with support for incremental loading based on ticket
    update time.

    It supports incremental loading by using a timestamp to fetch only tickets updated
    since the last run, minimizing data transfer and processing time.

    Args:
        updated_at: An incremental parameter specifying the timestamp from which to start
                    fetching updated tickets.
        page: The starting page number for fetching data, supporting API pagination.
        per_page: The number of items to fetch per page, max 100.
        domain: The Freshdesk domain.
        api_secret_key: Freshdesk API key, defaults to "secrets.toml"
        endpoint: Specifies the API endpoint, defaulting to "tickets" for fetching ticket data.

    Yields:
        An iterable of TDataItem objects, each representing a ticket fetched from Freshdesk.
    """

    url = f"https://{domain}.freshdesk.com/api/v2/{endpoint}"
    yield from paginated_response(
        url, page, per_page, updated_at.start_value, api_secret_key, endpoint
    )

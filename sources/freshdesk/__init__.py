import dlt
from .helpers import create_auth_headers, get_endpoint, paginated_response
from dlt.sources import DltResource
from dlt.common.typing import TDataItem
from typing import List, Iterable, Optional


@dlt.source
def freshdesk_source(
    endpoints: Optional[List[str]] = None,
    domain: str = dlt.config.value,
    api_secret_key: str = dlt.secrets.value,
) -> Iterable[DltResource]:
    """
    A source function to fetch Freshdesk tickets.

    Args:
        endpoints (Optional[List[str]]): List of Freshdesk API endpoints.
        domain (str): Freshdesk domain.
        api_secret_key (str): Freshdesk API secret key.

    Yields:
        Iterable[DltResource]: Freshdesk ticket data as DltResource.
    """

    if endpoints is None:
        endpoints = []  # Default to an empty list if None provided

    for endpoint in endpoints:
        yield dlt.resource(  # type: ignore
            get_endpoint(
                api_secret_key=api_secret_key, domain=domain, endpoint=endpoint
            ),
            name=endpoint,
            write_disposition="merge",
            primary_key="id",
        )


@dlt.resource(write_disposition="append")
def tickets(
    created_at: dlt.sources.incremental = dlt.sources.incremental(
        "created_at", initial_value="2000-01-01T00:00:00Z"
    ),
    page: int = 1,
    per_page: int = 100,
    domain: str = dlt.config.value,
    api_secret_key: str = dlt.secrets.value,
) -> Iterable[TDataItem]:
    """
    A resource function to retrieve Freshdesk tickets.

    Args:
        created_at (dlt.sources.incremental): Incremental timestamp for filtering.
        page (int): Page number.
        per_page (int): Number of tickets per page.
        domain (str): Freshdesk domain.
        api_secret_key (str): Freshdesk API secret key.

    Yields:
        Iterable[TDataItem]: Freshdesk ticket data as TDataItem.
    """
    headers = create_auth_headers(api_secret_key)
    url = f"https://{domain}.freshdesk.com/api/v2/tickets"
    yield from paginated_response(url, page, per_page, created_at.start_value, headers)

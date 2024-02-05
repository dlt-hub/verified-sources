import dlt
from .helpers import create_auth_headers, get_endpoint, paginated_response
from dlt.sources import DltResource
from dlt.common.typing import TDataItem
from typing import List, Iterable


@dlt.source
def freshdesk_source(
    endpoints: List = None, domain=dlt.config.value, api_secret_key=dlt.secrets.value
) -> Iterable[DltResource]:
    """
    A source function to fetch Freshdesk tickets.

    Args:
        endpoints (List[str]): List of Freshdesk API endpoints.
        domain (str): Freshdesk domain.
        api_secret_key (str): Freshdesk API secret key.

    Yields:
        Iterable[DltResource]: Freshdesk ticket data as DltResource.
    """

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
    created_at=dlt.sources.incremental(
        "created_at", initial_value="2000-01-01T00:00:00Z"
    ),
    page=1,
    per_page=100,  # maximum 100
    domain=dlt.config.value,
    api_secret_key=dlt.secrets.value,
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

from typing import Sequence, Iterator

import dlt

from dlt.common.typing import TDataItems
from dlt.extract.source import DltResource

from hubspot.client import fetch_data
from hubspot.endpoints import CRM_CONTACTS_ENDPOINT, CRM_COMPANIES_ENDPOINT, \
    CRM_DEALS_ENDPOINT, CRM_TICKETS_ENDPOINT, CRM_PRODUCTS_ENDPOINT, CRM_ENGAGEMENTS_CALLS_ENDPOINT, \
    CRM_ENGAGEMENTS_COMMUNICATIONS_ENDPOINT, CRM_ENGAGEMENTS_EMAILS_ENDPOINT, WEB_ANALYTICS_EVENTS_ENDPOINT


@dlt.source
def hubspot(api_key: str = dlt.secrets.value) -> Sequence[DltResource]:
    """
    A DLT source that retrieves data from the HubSpot API using the specified API key.

    This function retrieves data for several HubSpot API endpoints, including companies, contacts, deals,
    tickets, products, engagements calls, engagements communications, engagements emails, and web analytics
    events. It returns a tuple of Dlt resources, one for each endpoint.

    Args:
        api_key (str): The HubSpot API key to use for authentication. This can be passed as a string,
            or can be fetched from a Dlt secrets store using `dlt.secrets.value`.

    Returns:
        tuple: A tuple of Dlt resources, one for each HubSpot API endpoint.

    Notes:
        This function uses the `fetch_data` function to retrieve data from the HubSpot CRM API. The API key
        is passed to `fetch_data` as the `api_key` argument.

        This function is decorated with `dlt.source`, which indicates that it is a Dlt source function.
        When this function is included in a dlt pipeline, it will be executed to retrieve data for the
        specified HubSpot API endpoints.

    """

    @dlt.resource(write_disposition="replace")
    def companies() -> Iterator[TDataItems]:
        yield fetch_data(CRM_COMPANIES_ENDPOINT, api_key=api_key)

    @dlt.resource(write_disposition="replace")
    def contacts() -> Iterator[TDataItems]:
        yield fetch_data(CRM_CONTACTS_ENDPOINT, api_key=api_key)

    @dlt.resource(write_disposition="replace")
    def deals() -> Iterator[TDataItems]:
        yield fetch_data(CRM_DEALS_ENDPOINT, api_key=api_key)

    @dlt.resource(write_disposition="replace")
    def tickets() -> Iterator[TDataItems]:
        yield fetch_data(CRM_TICKETS_ENDPOINT, api_key=api_key)

    @dlt.resource(write_disposition="replace")
    def products() -> Iterator[TDataItems]:
        yield fetch_data(CRM_PRODUCTS_ENDPOINT, api_key=api_key)

    @dlt.resource(write_disposition="replace")
    def web_analytics_events() -> Iterator[TDataItems]:
        yield fetch_data(WEB_ANALYTICS_EVENTS_ENDPOINT, api_key=api_key)

    return companies(), contacts(), deals(), tickets(), products(), web_analytics_events()

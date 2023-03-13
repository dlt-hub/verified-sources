"""
This is a module that provides a DLT source to retrieve data from multiple endpoints of the HubSpot API using a specified API key. The retrieved data is returned as a tuple of Dlt resources, one for each endpoint.
"""

from datetime import datetime
from typing import Sequence, Iterator

import dlt
from dlt.common.typing import TDataItems
from dlt.extract.source import DltResource

from .client import fetch_data
from .endpoints import CRM_CONTACTS_ENDPOINT, CRM_COMPANIES_ENDPOINT, \
    CRM_DEALS_ENDPOINT, CRM_TICKETS_ENDPOINT, CRM_PRODUCTS_ENDPOINT, WEB_ANALYTICS_EVENTS_ENDPOINT, \
    CRM_QUOTES_ENDPOINT


@dlt.source
def hubspot(api_key: str = dlt.secrets.value) -> Sequence[DltResource]:
    """
    A DLT source that retrieves data from the HubSpot API using the specified API key.

    This function retrieves data for several HubSpot API endpoints, including companies, contacts, deals,
    tickets, products and web analytics events. It returns a tuple of Dlt resources, one for each endpoint.

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

    def _get_web_analytics_events(object_type: str, object_id: str):
        """
        A helper function that retrieves web analytics events for a given object from the HubSpot API.

        Args:
            object_type (str): The type of object for which to retrieve web analytics events.
            object_id (str): The ID of the object for which to retrieve web analytics events.

        Yields:
            dict: A dictionary representing a web analytics event.

        """
        _bookmark = dlt.state().setdefault(f"event_bookmark_{object_type}_{object_id}",
                                           datetime(1970, 1, 1).strftime("%Y-%m-%dT%H:%M:%S.000Z"))

        _event = None
        for _event in fetch_data(
            WEB_ANALYTICS_EVENTS_ENDPOINT.format(objectType=object_type, objectId=object_id,
                                                 occurredAfter=_bookmark),
            api_key=api_key
        ):
            yield _event

        if _event and 'occurredAt' in _event:
            dlt.state()[f"event_bookmark_{object_type}_{object_id}"] = _event['occurredAt']

    @dlt.resource(name="companies", write_disposition="replace")
    def companies() -> Iterator[TDataItems]:
        yield fetch_data(CRM_COMPANIES_ENDPOINT, api_key=api_key)

    @dlt.transformer(data_from=companies, name="companies_events", write_disposition="append")
    def companies_events(company: dict = None) -> Iterator[TDataItems]:
        yield from _get_web_analytics_events('company', company["hs_object_id"])

    @dlt.resource(name="contacts", write_disposition="replace")
    def contacts() -> Iterator[TDataItems]:
        yield fetch_data(CRM_CONTACTS_ENDPOINT, api_key=api_key)

    @dlt.transformer(data_from=contacts, name="contacts_events", write_disposition="append")
    def contacts_events(contact: dict = None) -> Iterator[TDataItems]:
        yield _get_web_analytics_events("contact", contact["hs_object_id"])

    @dlt.resource(name="deals", write_disposition="replace")
    def deals() -> Iterator[TDataItems]:
        yield fetch_data(CRM_DEALS_ENDPOINT, api_key=api_key)

    @dlt.transformer(data_from=deals, name="deals_events", write_disposition="append")
    def deals_events(deal: dict = None) -> Iterator[TDataItems]:
        yield _get_web_analytics_events("deal", deal["hs_object_id"])

    @dlt.resource(name="tickets", write_disposition="replace")
    def tickets() -> Iterator[TDataItems]:
        yield fetch_data(CRM_TICKETS_ENDPOINT, api_key=api_key)

    @dlt.transformer(data_from=tickets, name="tickets_events", write_disposition="append")
    def tickets_events(ticket: dict = None) -> Iterator[TDataItems]:
        yield _get_web_analytics_events("ticket", ticket["hs_object_id"])

    @dlt.resource(name="products", write_disposition="replace")
    def products() -> Iterator[TDataItems]:
        yield fetch_data(CRM_PRODUCTS_ENDPOINT, api_key=api_key)

    @dlt.transformer(data_from=products, name="products_events", write_disposition="append")
    def products_events(product: dict = None) -> Iterator[TDataItems]:
        yield _get_web_analytics_events("product", product["hs_object_id"])

    @dlt.resource(name="quotes", write_disposition="replace")
    def quotes() -> Iterator[TDataItems]:
        yield fetch_data(CRM_QUOTES_ENDPOINT, api_key=api_key)

    @dlt.transformer(data_from=quotes, name="quotes_events", write_disposition="append")
    def quotes_events(quote: dict = None) -> Iterator[TDataItems]:
        yield _get_web_analytics_events("quote", quote["hs_object_id"])

    return [companies(), companies_events(),
            contacts(), contacts_events(),
            deals(), deals_events(),
            tickets(), tickets_events(),
            products(), products_events(),
            quotes(), quotes_events()]


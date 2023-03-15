"""
This is a module that provides a DLT source to retrieve data from multiple endpoints of the HubSpot API using a specified API key. The retrieved data is returned as a tuple of Dlt resources, one for each endpoint.

The source retrieves data from the following endpoints:
- CRM Companies
- CRM Contacts
- CRM Deals
- CRM Tickets
- CRM Products
- CRM Quotes
- Web Analytics Events

For each endpoint, a resource and transformer function are defined to retrieve data and transform it to a common format.
The resource functions yield the raw data retrieved from the API, while the transformer functions are used to retrieve
additional information from the Web Analytics Events endpoint.

The source also supports enabling Web Analytics Events for each endpoint by setting the corresponding enable flag to True.

Example:
To retrieve data from all endpoints and enable Web Analytics Events for CRM Companies and CRM Contacts, use the following code:

python

>>> resources = hubspot(api_key="your_api_key")
>>> resources.companies_events.selected = True
>>> resources.contacts_events.selected = True

Note:
The Web Analytics Events endpoint bookmarks the latest event using a state.
If there is no state saved, it starts from the beginning of time (1970-01-01T00:00:00Z).
The bookmark is updated after each retrieval, so it can be used to continue from where the previous retrieval left off.
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
        api_key (str, optional): The API key used to authenticate with the HubSpot API. Defaults to dlt.secrets.value.

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

    @dlt.transformer(data_from=companies, name="companies_events", write_disposition="append", selected=False)
    def companies_events(company: dict = None) -> Iterator[TDataItems]:
        yield from _get_web_analytics_events('company', company["hs_object_id"])

    @dlt.resource(name="contacts", write_disposition="replace")
    def contacts() -> Iterator[TDataItems]:
        yield fetch_data(CRM_CONTACTS_ENDPOINT, api_key=api_key)

    @dlt.transformer(data_from=contacts, name="contacts_events", write_disposition="append", selected=False)
    def contacts_events(contact: dict = None) -> Iterator[TDataItems]:
        yield _get_web_analytics_events("contact", contact["hs_object_id"])

    @dlt.resource(name="deals", write_disposition="replace")
    def deals() -> Iterator[TDataItems]:
        yield fetch_data(CRM_DEALS_ENDPOINT, api_key=api_key)

    @dlt.transformer(data_from=deals, name="deals_events", write_disposition="append", selected=False)
    def deals_events(deal: dict = None) -> Iterator[TDataItems]:
        yield _get_web_analytics_events("deal", deal["hs_object_id"])

    @dlt.resource(name="tickets", write_disposition="replace")
    def tickets() -> Iterator[TDataItems]:
        yield fetch_data(CRM_TICKETS_ENDPOINT, api_key=api_key)

    @dlt.transformer(data_from=tickets, name="tickets_events", write_disposition="append", selected=False)
    def tickets_events(ticket: dict = None) -> Iterator[TDataItems]:
        yield _get_web_analytics_events("ticket", ticket["hs_object_id"])

    @dlt.resource(name="products", write_disposition="replace")
    def products() -> Iterator[TDataItems]:
        yield fetch_data(CRM_PRODUCTS_ENDPOINT, api_key=api_key)

    @dlt.transformer(data_from=products, name="products_events", write_disposition="append", selected=False)
    def products_events(product: dict = None) -> Iterator[TDataItems]:
        yield _get_web_analytics_events("product", product["hs_object_id"])

    @dlt.resource(name="quotes", write_disposition="replace")
    def quotes() -> Iterator[TDataItems]:
        yield fetch_data(CRM_QUOTES_ENDPOINT, api_key=api_key)

    @dlt.transformer(data_from=quotes, name="quotes_events", write_disposition="append", selected=False)
    def quotes_events(quote: dict = None) -> Iterator[TDataItems]:
        yield _get_web_analytics_events("quote", quote["hs_object_id"])

    return [companies(), companies_events(),
            contacts(), contacts_events(),
            deals(), deals_events(),
            tickets(), tickets_events(),
            products(), products_events(),
            quotes(), quotes_events()]

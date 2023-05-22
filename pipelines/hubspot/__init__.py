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
To retrieve data from all endpoints, use the following code:

python

>>> resources = hubspot(api_key="your_api_key")
"""

from typing import Any, Dict, List, Literal, Sequence, Iterator

import dlt
from dlt.common import pendulum
from dlt.common.typing import TDataItems
from dlt.extract.source import DltResource

from .client import fetch_data
from .endpoints import CRM_CONTACTS_ENDPOINT, CRM_COMPANIES_ENDPOINT, \
    CRM_DEALS_ENDPOINT, CRM_TICKETS_ENDPOINT, CRM_PRODUCTS_ENDPOINT, WEB_ANALYTICS_EVENTS_ENDPOINT, \
    CRM_QUOTES_ENDPOINT

FIRST_DAY_OF_MILLENNIUM = pendulum.datetime(year=2000, month=1, day=1)
THubspotObjectType = Literal["company", "contact", "deal", "ticket", "product", "quote"]


@dlt.source(name="hubspot")
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

    """

    @dlt.resource(name="companies", write_disposition="replace")
    def companies() -> Iterator[TDataItems]:
        yield from fetch_data(CRM_COMPANIES_ENDPOINT, api_key=api_key)

    @dlt.resource(name="contacts", write_disposition="replace")
    def contacts() -> Iterator[TDataItems]:
        yield from fetch_data(CRM_CONTACTS_ENDPOINT, api_key=api_key)

    @dlt.resource(name="deals", write_disposition="replace")
    def deals() -> Iterator[TDataItems]:
        yield from fetch_data(CRM_DEALS_ENDPOINT, api_key=api_key)

    @dlt.resource(name="tickets", write_disposition="replace")
    def tickets() -> Iterator[TDataItems]:
        yield from fetch_data(CRM_TICKETS_ENDPOINT, api_key=api_key)

    @dlt.resource(name="products", write_disposition="replace")
    def products() -> Iterator[TDataItems]:
        yield from fetch_data(CRM_PRODUCTS_ENDPOINT, api_key=api_key)

    @dlt.resource(name="quotes", write_disposition="replace")
    def quotes() -> Iterator[TDataItems]:
        yield from fetch_data(CRM_QUOTES_ENDPOINT, api_key=api_key)

    return [
        companies(),
        contacts(),
        deals(),
        tickets(),
        products(),
        quotes(),
    ]


@dlt.resource
def hubspot_events_for_objects(
    object_type: THubspotObjectType,
    object_ids: List[str],
    api_key: str = dlt.secrets.value,
    start_date: pendulum.DateTime = FIRST_DAY_OF_MILLENNIUM) -> DltResource:
    """
    A standalone DLT resources that retrieves web analytics events from the HubSpot API for a particular object type and list of object ids.

    Args:
        object_type(THubspotObjectType, required): One of the hubspot object types see definition of THubspotObjectType literal
        object_ids: (List[THubspotObjectType], required): List of object ids to track events
        api_key (str, optional): The API key used to authenticate with the HubSpot API. Defaults to dlt.secrets.value.
        start_date (datetime, optional): The initial date time from which start getting events, default to FIRST_DAY_OF_MILLENNIUM

    Returns:
        incremental dlt resource to track events for objects from the list

    """

    end_date = pendulum.now().isoformat()
    name = object_type + "_events"

    def _get_web_analytics_events(occurred_at: dlt.sources.incremental[str]) -> Iterator[List[Dict[str, Any]]]:
        """
        A helper function that retrieves web analytics events for a given object type from the HubSpot API.

        Args:
            object_type (str): The type of object for which to retrieve web analytics events.

        Yields:
            dict: A dictionary representing a web analytics event.

        """
        from urllib.parse import quote

        for object_id in object_ids:
            yield from fetch_data(
                WEB_ANALYTICS_EVENTS_ENDPOINT.format(
                    objectType=object_type,
                    objectId=object_id,
                    occurredAfter=quote(occurred_at.last_value),
                    occurredBefore=quote(end_date)
                ),
                api_key=api_key
            )

    return dlt.resource(
        _get_web_analytics_events,
        name=name,
        primary_key="id",
        write_disposition="append",
        selected=True,
        table_name=lambda e: name + "_" + str(e["eventType"]))(
            dlt.sources.incremental("occurredAt", initial_value=start_date.isoformat())
        )

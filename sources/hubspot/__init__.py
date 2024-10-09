"""
This is a module that provides a DLT source to retrieve data from multiple endpoints of the HubSpot API
using a specified API key. The retrieved data is returned as a tuple of Dlt resources, one for each endpoint.

The source retrieves data from the following endpoints:
- CRM Companies
- CRM Contacts
- CRM Deals
- CRM Tickets
- CRM Products
- CRM Quotes
- CRM Owners
- CRM Pipelines
- Web Analytics Events

For each endpoint, a resource and transformer function are defined to retrieve data and transform it to a common format.
The resource functions yield the raw data retrieved from the API, while the transformer functions are used to retrieve
additional information from the Web Analytics Events endpoint.

The source also supports enabling Web Analytics Events for each endpoint by setting the corresponding enable flag to True.

Example:
To retrieve data from all endpoints, use the following code:
"""

from typing import Any, Dict, Iterator, List, Literal, Optional, Sequence, Tuple
from urllib.parse import quote

import dlt
from dlt.common import pendulum
from dlt.common.typing import TDataItems
from dlt.sources import DltResource

from .helpers import (
    _get_property_names,
    fetch_data,
    fetch_property_history,
    get_properties_labels,
)
from .settings import (
    ALL,
    ALL_OBJECTS,
    ARCHIVED_PARAM,
    CRM_OBJECT_ENDPOINTS,
    CRM_PIPELINES_ENDPOINT,
    ENTITY_PROPERTIES,
    MAX_PROPS_LENGTH,
    OBJECT_TYPE_PLURAL,
    OBJECT_TYPE_SINGULAR,
    PIPELINES_OBJECTS,
    SOFT_DELETE_KEY,
    STAGE_PROPERTY_PREFIX,
    STARTDATE,
    WEB_ANALYTICS_EVENTS_ENDPOINT,
)

THubspotObjectType = Literal["company", "contact", "deal", "ticket", "product", "quote"]


def extract_properties_list(props: Sequence[Any]) -> List[str]:
    """
    Flatten a list of property dictionaries to extract property names.

    Args:
        props (Sequence[Any]): List of property names or property dictionaries.

    Returns:
        List[str]: List of property names.
    """
    return [prop if isinstance(prop, str) else prop.get("name") for prop in props]


def fetch_data_for_properties(
    props: Sequence[str],
    api_key: str,
    object_type: str,
    soft_delete: bool,
) -> Iterator[TDataItems]:
    """
    Fetch data for a given set of properties from the HubSpot API.

    Args:
        props (Sequence[str]): List of property names to fetch.
        api_key (str): HubSpot API key for authentication.
        object_type (str): The type of HubSpot object (e.g., 'company', 'contact').
        soft_delete (bool): Flag to fetch soft-deleted (archived) records.

    Yields:
        Iterator[TDataItems]: Data retrieved from the HubSpot API.
    """

    params: Dict[str, Any] = {"properties": props, "limit": 100}
    context: Optional[Dict[str, Any]] = {SOFT_DELETE_KEY: False} if soft_delete else None

    yield from fetch_data(
        CRM_OBJECT_ENDPOINTS[object_type], api_key, params=params, context=context
    )
    if soft_delete:
        yield from fetch_data(
            CRM_OBJECT_ENDPOINTS[object_type],
            api_key,
            params={**params, **ARCHIVED_PARAM},
            context={SOFT_DELETE_KEY: True},
        )


def crm_objects(
    object_type: str,
    api_key: str = dlt.secrets.value,
    props: Optional[Sequence[str]] = None,
    include_custom_props: bool = True,
    archived: bool = False,
) -> Iterator[TDataItems]:
    """
    Fetch CRM object data (e.g., companies, contacts) from the HubSpot API.

    Args:
        object_type (str): Type of HubSpot object (e.g., 'company', 'contact').
        api_key (str, optional): API key for HubSpot authentication.
        props (Optional[Sequence[str]], optional): List of properties to retrieve. Defaults to None.
        include_custom_props (bool, optional): Include custom properties in the result. Defaults to True.
        archived (bool, optional): Fetch archived (soft-deleted) objects. Defaults to False.

    Yields:
        Iterator[TDataItems]: Data items retrieved from the API.
    """
    props = fetch_props(object_type, api_key, props, include_custom_props)
    yield from fetch_data_for_properties(props, api_key, object_type, archived)


def crm_object_history(
    object_type: THubspotObjectType,
    api_key: str = dlt.secrets.value,
    include_custom_props: bool = True,
) -> Iterator[TDataItems]:
    """
    Fetch the history of property changes for a given CRM object type.

    Args:
        object_type (THubspotObjectType): Type of HubSpot object (e.g., 'company', 'contact').
        api_key (str, optional): API key for HubSpot authentication.
        include_custom_props (bool, optional): Include custom properties in the result. Defaults to True.

    Yields:
        Iterator[TDataItems]: Historical property data.
    """

    # Fetch the properties from ENTITY_PROPERTIES or default to "All"
    props: str = ENTITY_PROPERTIES.get(object_type, "All")

    # Fetch the properties with the option to include custom properties
    props = fetch_props(object_type, api_key, props, include_custom_props)

    # Yield the property history
    yield from fetch_property_history(
        CRM_OBJECT_ENDPOINTS[object_type],
        api_key,
        props,
    )


def resource_template(
    entity: THubspotObjectType,
    api_key: str = dlt.config.value,
    props: Optional[Sequence[str]] = None,  # Add props as an argument
    include_custom_props: bool = False,
    soft_delete: bool = False,
) -> Iterator[TDataItems]:
    """
    Template function to yield CRM resources for a specific HubSpot entity.

    Args:
        entity (THubspotObjectType): Type of HubSpot object (e.g., 'company', 'contact').
        api_key (str, optional): HubSpot API key for authentication.
        props (Optional[Sequence[str]], optional): List of properties to retrieve. Defaults to None.
        include_custom_props (bool, optional): Include custom properties in the result. Defaults to False.
        soft_delete (bool, optional): Fetch soft-deleted (archived) records. Defaults to False.

    Yields:
        Iterator[TDataItems]: CRM object data retrieved from the API.
    """

    # Use provided props or fetch from ENTITY_PROPERTIES if not provided
    properties: List[str] = ENTITY_PROPERTIES.get(entity, list(props or []))

    # Use these properties to yield the crm_objects
    yield from crm_objects(
        entity,
        api_key,
        props=properties,  # Pass the properties to the crm_objects function
        include_custom_props=include_custom_props,
        archived=soft_delete,
    )


def resource_history_template(
    entity: THubspotObjectType,
    api_key: str = dlt.config.value,
    include_custom_props: bool = False,
) -> Iterator[TDataItems]:
    """
    Template function to yield historical CRM resource data for a specific HubSpot entity.

    Args:
        entity (THubspotObjectType): Type of HubSpot object (e.g., 'company', 'contact').
        api_key (str, optional): HubSpot API key for authentication.
        include_custom_props (bool, optional): Include custom properties in the result. Defaults to False.

    Yields:
        Iterator[TDataItems]: Historical data for the CRM object.
    """
    yield from crm_object_history(
        entity, api_key, include_custom_props=include_custom_props
    )


@dlt.resource(name="properties", write_disposition="replace")
def hubspot_properties(
    properties_list: Optional[List[Dict[str, Any]]] = None,
    api_key: str = dlt.secrets.value,
)  -> Iterator[TDataItems]:
    """
    A DLT resource that retrieves HubSpot properties for a given list of objects.

    Args:
        properties_list (Optional[List[Dict[str, Any]]], optional): List of properties to retrieve.
        api_key (str, optional): HubSpot API key for authentication.

    Yields:
        DltResource: A DLT resource containing properties for HubSpot objects.
    """

    def get_properties_description(
        properties_list_inner: List[Dict[str, Any]]
    ) -> Iterator[Dict[str, Any]]:
        """Fetch properties."""
        for property_info in properties_list_inner:
            yield get_properties_labels(
                api_key=api_key,
                object_type=property_info["object_type"],
                property_name=property_info["property_name"],
            )

    # Ensure properties_list is defined
    properties_list_inner: List[Dict[str, Any]] = properties_list or []
    yield from get_properties_description(properties_list_inner)


def pivot_stages_properties(
    data: List[Dict[str, Any]],
    property_prefix: str = STAGE_PROPERTY_PREFIX,
    id_prop: str = "id",
) -> List[Dict[str, Any]]:
    """
    Transform the data by pivoting stage properties.

    Args:
        data (List[Dict[str, Any]]): Data containing stage properties.
        property_prefix (str, optional): Prefix for stage properties. Defaults to STAGE_PROPERTY_PREFIX.
        id_prop (str, optional): Name of the ID property. Defaults to "id".

    Returns:
        List[Dict[str, Any]]: Transformed data with pivoted stage properties.
    """
    new_data: List[Dict[str, Any]] = []
    for record in data:
        record_not_null: Dict[str, Any] = {k: v for k, v in record.items() if v is not None}
        if id_prop not in record_not_null:
            continue
        id_val = record_not_null.pop(id_prop)
        new_data += [
            {id_prop: id_val, property_prefix: v, "stage": k.split(property_prefix)[1]}
            for k, v in record_not_null.items()
            if k.startswith(property_prefix)
        ]
    return new_data


def stages_timing(
    object_type: str,
    api_key: str = dlt.config.value,
    soft_delete: bool = False,
    limit: Optional[int] = None,
) -> Iterator[TDataItems]:
    """
    Fetch stage timing data for a specific object type from the HubSpot API.

    Args:
        object_type (str): Type of HubSpot object (e.g., 'deal', 'ticket').
        api_key (str, optional): HubSpot API key for authentication.
        soft_delete (bool, optional): Fetch soft-deleted (archived) records. Defaults to False.
        limit (Optional[int], optional): Limit the number of properties to fetch. Defaults to None.

    Yields:
        Iterator[TDataItems]: Stage timing data.
    """
    all_properties: List[str] = list(_get_property_names(api_key, object_type))
    date_entered_properties: List[str] = [
        prop for prop in all_properties if prop.startswith(STAGE_PROPERTY_PREFIX)
    ]
    props: str = ",".join(date_entered_properties)
    idx: int = 0
    if limit is None:
        limit = len(date_entered_properties)
    while idx < limit:
        if len(props) - idx < MAX_PROPS_LENGTH:
            props_part = ",".join(props[idx: idx + MAX_PROPS_LENGTH].split(",")[:-1])
        else:
            props_part = props[idx: idx + MAX_PROPS_LENGTH]
        idx += len(props_part)
        for data in fetch_data_for_properties(
            props_part, api_key, object_type, soft_delete
        ):
            yield pivot_stages_properties(data)


def owners(
    api_key: str,
    soft_delete: bool = False,
) -> Iterator[TDataItems]:
    """
    Fetch HubSpot owners data.

    Args:
        api_key (str): HubSpot API key for authentication.
        soft_delete (bool, optional): Fetch soft-deleted (archived) owners. Defaults to False.

    Yields:
        Iterator[TDataItems]: Owner data.
    """

    # Fetch data for owners
    for page in fetch_data(endpoint=CRM_OBJECT_ENDPOINTS["owner"], api_key=api_key):
        yield page

    # Fetch soft-deleted owners if requested
    if soft_delete:
        for page in fetch_data(
            endpoint=CRM_OBJECT_ENDPOINTS["owner"],
            params=ARCHIVED_PARAM,
            api_key=api_key,
            context={SOFT_DELETE_KEY: True},
        ):
            yield page


@dlt.source(name="hubspot")
def hubspot(
    api_key: str = dlt.secrets.value,
    include_history: bool = False,
    soft_delete: bool = False,
    include_custom_props: bool = True,
    props: Optional[Sequence[str]] = None,  # Add props argument here
) -> Iterator[DltResource]:
    """
    A DLT source that retrieves data from the HubSpot API using the
    specified API key.

    This function retrieves data for several HubSpot API endpoints,
    including companies, contacts, deals, tickets, products and web
    analytics events. It returns a tuple of Dlt resources, one for
    each endpoint.

    Args:
        api_key (Optional[str]):
            The API key used to authenticate with the HubSpot API. Defaults
            to dlt.secrets.value.
        include_history (Optional[bool]):
            Whether to load history of property changes along with entities.
            The history entries are loaded to separate tables.
        soft_delete (bool):
            Whether to fetch deleted properties and mark them as `is_deleted`.
        include_custom_props (bool):
            Whether to include custom properties.

    Returns:
        Sequence[DltResource]: Dlt resources, one for each HubSpot API endpoint.

    Notes:
        This function uses the `fetch_data` function to retrieve data from the
        HubSpot CRM API. The API key is passed to `fetch_data` as the
        `api_key` argument.
    """

    def hubspot_pipelines_for_objects(
        api_key_inner: str = dlt.secrets.value,
    ) -> Iterator[DltResource]:
        """
        A standalone DLT resource that retrieves pipelines for HubSpot objects.

        Args:
            api_key_inner (str, optional): The API key used to authenticate with the HubSpot API. Defaults to dlt.secrets.value.

        Yields:
            Iterator[DltResource]: DLT resources for pipelines and stages.
        """

        def get_pipelines(object_type: THubspotObjectType) -> Iterator[TDataItems]:
            yield from fetch_data(
                CRM_PIPELINES_ENDPOINT.format(objectType=object_type),
                api_key=api_key_inner,
            )

        for obj_type in PIPELINES_OBJECTS:
            name = f"pipelines_{obj_type}"
            yield dlt.resource(
                get_pipelines,
                name=name,
                write_disposition="merge",
                merge_key="id",
                table_name=name,
                primary_key="id",
            )(obj_type)

            name = f"stages_timing_{obj_type}"
            if obj_type in OBJECT_TYPE_SINGULAR:
                yield dlt.resource(
                    stages_timing,
                    name=name,
                    write_disposition="merge",
                    primary_key=["id", "stage"],
                )(OBJECT_TYPE_SINGULAR[obj_type], soft_delete=soft_delete)

    yield dlt.resource(
        owners,
        name="owners",
        write_disposition="merge",
        primary_key="id",
    )(
        api_key=api_key,  # Pass the API key here
        soft_delete=soft_delete  # Pass the soft_delete flag here
    )

    for obj in ALL_OBJECTS:
        yield dlt.resource(
            resource_template,
            name=OBJECT_TYPE_PLURAL[obj],
            write_disposition="merge",
            primary_key="id",
        )(
            entity=obj,
            props=props,  # Pass the props argument here
            include_custom_props=include_custom_props,
            soft_delete=soft_delete,
        )

    if include_history:
        for obj in ALL_OBJECTS:
            yield dlt.resource(
                resource_history_template,
                name=f"{OBJECT_TYPE_PLURAL[obj]}_property_history",
                write_disposition="merge",
                primary_key="object_id",
            )(entity=obj, include_custom_props=include_custom_props)

    yield from hubspot_pipelines_for_objects(api_key)
    yield hubspot_properties


def fetch_props(
    object_type: str,
    api_key: str,
    props: Optional[Sequence[str]] = None,
    include_custom_props: bool = True,
) -> str:
    """
    Fetch the list of properties for a HubSpot object type.

    Args:
        object_type (str): Type of HubSpot object (e.g., 'company', 'contact').
        api_key (str): HubSpot API key for authentication.
        props (Optional[Sequence[str]], optional): List of properties to fetch. Defaults to None.
        include_custom_props (bool, optional): Include custom properties in the result. Defaults to True.

    Returns:
        str: Comma-separated list of properties.
    """
    if props == ALL:
        # Fetch all property names
        props_list = list(_get_property_names(api_key, object_type))
    elif isinstance(props, str):
        # If props are passed as a single string, convert it to a list
        props_list = [props]
    else:
        # Ensure it's a list of strings, if not already
        props_list = extract_properties_list(props or [])

    if include_custom_props:
        all_props: List[str] = _get_property_names(api_key, object_type)
        custom_props: List[str] = [prop for prop in all_props if not prop.startswith("hs_")]
        props_list += custom_props

    props_str = ",".join(sorted(set(props_list)))

    if len(props_str) > MAX_PROPS_LENGTH:
        raise ValueError(
            "Your request to Hubspot is too long to process. "
            f"Maximum allowed query length is {MAX_PROPS_LENGTH} symbols, while "
            f"your list of properties `{props_str[:200]}`... is {len(props_str)} "
            "symbols long. Use the `props` argument of the resource to "
            "set the list of properties to extract from the endpoint."
        )
    return props_str


@dlt.resource
def hubspot_events_for_objects(
    object_type: THubspotObjectType,
    object_ids: List[str],
    api_key: str = dlt.secrets.value,
    start_date: pendulum.DateTime = STARTDATE,
) -> DltResource:
    """
    A standalone DLT resource that retrieves web analytics events from the HubSpot API for a particular object type and list of object ids.

    Args:
        object_type (THubspotObjectType): One of the hubspot object types see definition of THubspotObjectType literal.
        object_ids (List[str]): List of object ids to track events.
        api_key (str, optional): The API key used to authenticate with the HubSpot API. Defaults to dlt.secrets.value.
        start_date (pendulum.DateTime, optional): The initial date time from which start getting events, default to STARTDATE.

    Returns:
        DltResource: Incremental DLT resource to track events for objects from the list.
    """

    end_date: str = pendulum.now().isoformat()
    name: str = object_type + "_events"

    def get_web_analytics_events(
        occurred_at: dlt.sources.incremental[str],
    ) -> Iterator[List[Dict[str, Any]]]:
        """
        A helper function that retrieves web analytics events for a given object type from the HubSpot API.

        Args:
            occurred_at (dlt.sources.incremental[str]): Incremental source for event occurrence time.

        Yields:
            Iterator[List[Dict[str, Any]]]: Web analytics event data.
        """
        for object_id in object_ids:
            yield from fetch_data(
                WEB_ANALYTICS_EVENTS_ENDPOINT.format(
                    objectType=object_type,
                    objectId=object_id,
                    occurredAfter=quote(occurred_at.last_value),
                    occurredBefore=quote(end_date),
                ),
                api_key=api_key,
            )

    return dlt.resource(
        get_web_analytics_events,
        name=name,
        primary_key="id",
        write_disposition="append",
        selected=True,
        table_name=lambda e: name + "_" + str(e["eventType"]),
    )(dlt.sources.incremental("occurredAt", initial_value=start_date.isoformat()))

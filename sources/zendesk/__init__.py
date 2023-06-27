"""
Defines all the sources and resources needed for ZendeskSupport, ZendeskChat and ZendeskTalk
"""

from typing import Iterator, Optional, Iterable, Tuple, List, Union, Dict, Any
from itertools import chain

import dlt
from dlt.common import pendulum
from dlt.common.time import parse_iso_like_datetime
from dlt.common.typing import TDataItem, TDataItems
from dlt.extract.source import DltResource

from .helpers.api_helpers import process_ticket, process_ticket_field
from .helpers.talk_api import ZendeskAPIClient
from .helpers.credentials import TZendeskCredentials, ZendeskCredentialsOAuth

from .settings import (
    DEFAULT_START_DATE,
    CUSTOM_FIELDS_STATE_KEY,
    TALK_ENDPOINTS,
    INCREMENTAL_ENDPOINTS,
    SUPPORT_EXTRA_ENDPOINTS,
)
from .helpers.backloading import SliceConfig


TSlice = Tuple[pendulum.DateTime, pendulum.DateTime]


@dlt.source(max_table_nesting=2)
def zendesk_talk(
    credentials: TZendeskCredentials = dlt.secrets.value,
    incremental_start_time: Optional[pendulum.DateTime] = DEFAULT_START_DATE,
    backload_range: Optional[TSlice] = None,
) -> Iterable[DltResource]:
    """
    Retrieves data from Zendesk Talk for phone calls and voicemails.

    Args:
        credentials (TZendeskCredentials): The credentials for authentication. Defaults to the value in the `dlt.secrets` object.
        incremental_start_time (Optional[pendulum.DateTime]): The start time for incremental loading. Defaults to DEFAULT_START_DATE.
        backload_range: Optional tuple of start/end timestamps to load any results from a given time range.

    Yields:
        DltResource: Data resources from Zendesk Talk.
    """

    # use the credentials to authenticate with the ZendeskClient
    zendesk_client = ZendeskAPIClient(credentials)

    # regular endpoints
    for key, talk_endpoint in TALK_ENDPOINTS.items():
        yield dlt.resource(
            talk_resource(
                zendesk_client=zendesk_client,
                talk_endpoint_name=key,
                talk_endpoint=talk_endpoint,
            ),
            name=key,
            write_disposition="replace",
        )

    # adding incremental endpoints
    kwargs: Dict[str, Any]
    if backload_range:
        kwargs = {
            "backload_range": SliceConfig(
                start=backload_range[0], end=backload_range[1], cursor_path="updated_at"
            )
        }
    else:
        kwargs = {
            "updated_at": dlt.sources.incremental(
                "updated_at", initial_value=incremental_start_time.isoformat()
            ),
        }

    for key, talk_incremental_endpoint in INCREMENTAL_ENDPOINTS.items():
        yield dlt.resource(
            talk_incremental_resource,
            name=f"{key}_incremental",
            primary_key="id",
            write_disposition="merge",
        )(
            zendesk_client=zendesk_client,
            talk_endpoint_name=key,
            talk_endpoint=talk_incremental_endpoint,
            **kwargs,
        )


def talk_resource(
    zendesk_client: ZendeskAPIClient, talk_endpoint_name: str, talk_endpoint: str
) -> Iterator[TDataItem]:
    """
    Loads data from a Zendesk Talk endpoint.

    Args:
        zendesk_client (ZendeskAPIClient): An instance of ZendeskAPIClient for making API calls to Zendesk Talk.
        talk_endpoint_name (str): The name of the talk_endpoint.
        talk_endpoint (str): The actual URL ending of the endpoint.

    Yields:
        TDataItem: Dictionary containing the data from the endpoint.
    """
    # send query and process it
    yield from zendesk_client.get_pages(
        endpoint=talk_endpoint, data_point_name=talk_endpoint_name
    )


def talk_incremental_resource(
    zendesk_client: ZendeskAPIClient,
    talk_endpoint_name: str,
    talk_endpoint: str,
    updated_at: Optional[dlt.sources.incremental[str]] = None,
    backload_range: Optional[SliceConfig] = None,
) -> Iterator[TDataItem]:
    """
    Loads data from a Zendesk Talk endpoint with incremental loading.

    Args:
        zendesk_client (ZendeskAPIClient): An instance of ZendeskAPIClient for making API calls to Zendesk Talk.
        talk_endpoint_name (str): The name of the talk_endpoint.
        talk_endpoint (str): The actual URL ending of the endpoint.
        updated_at (dlt.sources.incremental[str]): Source for the last updated timestamp.

    Yields:
        TDataItem: Dictionary containing the data from the endpoint.
    """
    kwargs: Dict[str, Any] = {}
    if backload_range:
        kwargs = backload_range.to_endpoint_kwargs()
    elif updated_at:
        kwargs["start_time"] = parse_iso_like_datetime(updated_at.last_value)

    # send the request and process it
    yield from zendesk_client.get_pages_incremental(
        endpoint=talk_endpoint, data_point_name=talk_endpoint_name, **kwargs
    )


@dlt.source(max_table_nesting=2)
def zendesk_chat(
    credentials: ZendeskCredentialsOAuth = dlt.secrets.value,
    incremental_start_time: Optional[pendulum.DateTime] = DEFAULT_START_DATE,
    backload_range: Optional[TSlice] = None,
) -> Iterable[DltResource]:
    """
    Retrieves data from Zendesk Chat for chat interactions.

    Args:
        credentials (ZendeskCredentialsOAuth): The credentials for authentication. Defaults to the value in the `dlt.secrets` object.
        incremental_start_time (Optional[pendulum.DateTime]): The start time for incremental loading. Defaults to DEFAULT_START_DATE.

    Yields:
        DltResource: Data resources from Zendesk Chat.
    """

    if backload_range:
        kwargs: Dict[str, Any] = {
            "backload_range": SliceConfig(
                start=backload_range[0],
                end=backload_range[1],
                cursor_path="update_timestamp",
            )
        }
    else:
        kwargs = {
            "update_timestamp": dlt.sources.incremental(
                "update_timestamp",
                initial_value=incremental_start_time.isoformat(),
            ),
        }

    # Authenticate
    zendesk_client = ZendeskAPIClient(credentials, url_prefix="https://www.zopim.com")
    yield dlt.resource(chats_table_resource, name="chats", write_disposition="append")(
        zendesk_client, **kwargs
    )


def chats_table_resource(
    zendesk_client: ZendeskAPIClient,
    update_timestamp: Optional[dlt.sources.incremental[str]] = None,
    backload_range: Optional[SliceConfig] = None,
) -> Iterator[TDataItems]:
    """
    Resource for Chats

    Args:
        zendesk_client (Zenpy): Zenpy type object, used to make calls to Zendesk API through the Zenpy module.
        update_timestamp (dlt.sources.incremental[str]): Incremental source specifying the timestamp for incremental loading.

    Yields:
        dict: A dictionary representing each row of data.
    """
    kwargs: Dict[str, Any] = {}
    if backload_range:
        kwargs = backload_range.to_endpoint_kwargs()
    elif update_timestamp:
        kwargs = dict(start_time=parse_iso_like_datetime(update_timestamp.last_value))

    chat_pages = zendesk_client.get_pages_incremental(
        "/api/v2/incremental/chats", "chats", params={"fields": "chats(*)"}, **kwargs
    )
    yield from chat_pages


@dlt.source(max_table_nesting=2)
def zendesk_support(
    credentials: TZendeskCredentials = dlt.secrets.value,
    load_all: bool = True,
    pivot_ticket_fields: bool = True,
    incremental_start_time: Optional[pendulum.DateTime] = DEFAULT_START_DATE,
    backload_range: Optional[TSlice] = None,
) -> Iterable[DltResource]:
    """
    Retrieves data from Zendesk Support for tickets, users, brands, organizations, and groups.

    Args:
        credentials (TZendeskCredentials): The credentials for authentication. Defaults to the value in the `dlt.secrets` object.
        load_all (bool): Whether to load extra resources for the API. Defaults to True.
        pivot_ticket_fields (bool): Whether to pivot the custom fields in tickets. Defaults to True.
        incremental_start_time (Optional[pendulum.DateTime]): The start time for incremental loading. Defaults to DEFAULT_START_DATE.

    Returns:
        Sequence[DltResource]: Multiple dlt resources.
    """

    incremental_start_time_ts = incremental_start_time.int_timestamp
    incremental_start_time_iso_str = incremental_start_time.isoformat()

    @dlt.resource(primary_key="id", write_disposition="append")
    def ticket_events(
        zendesk_client: ZendeskAPIClient,
        timestamp: Optional[dlt.sources.incremental[int]] = None,
        backload_range: Optional[SliceConfig] = None,
    ) -> Iterator[TDataItem]:
        kwargs: Dict[str, Any] = {}
        if backload_range:
            kwargs = backload_range.to_endpoint_kwargs()
        elif timestamp:
            kwargs = {
                "start_time": pendulum.from_timestamp(timestamp.last_value),
            }

        event_pages = zendesk_client.get_pages_incremental(
            "/api/v2/incremental/ticket_events.json", "ticket_events", **kwargs
        )
        yield from event_pages

    @dlt.resource(name="ticket_fields", write_disposition="replace")
    def ticket_fields_table(zendesk_client: ZendeskAPIClient) -> Iterator[TDataItem]:
        """
        Loads ticket fields data from Zendesk API.

        Args:
            zendesk_client (Zenpy): An instance of Zenpy object used to make API calls to Zendesk.

        Yields:
            TDataItem: Dictionary containing the ticket fields data.
        """
        # get dlt state
        ticket_custom_fields = dlt.current.source_state().setdefault(
            CUSTOM_FIELDS_STATE_KEY, {}
        )
        # get all custom fields and update state if needed, otherwise just load dicts into tables
        all_fields = list(
            chain.from_iterable(
                zendesk_client.get_pages("/api/v2/ticket_fields.json", "ticket_fields")
            )
        )
        # all_fields = zendesk_client.ticket_fields()
        for field in all_fields:
            yield process_ticket_field(field, ticket_custom_fields)

    @dlt.resource(
        name="tickets",
        primary_key="id",
        write_disposition="append",
        columns={
            "tags": {"data_type": "complex"},
            "custom_fields": {"data_type": "complex"},
        },
    )
    def ticket_table(
        zendesk_client: ZendeskAPIClient,
        pivot_fields: bool = True,
        per_page: int = 1000,
        updated_at: Optional[dlt.sources.incremental[pendulum.DateTime]] = None,
        backload_range: Optional[SliceConfig] = None,
    ) -> Iterator[TDataItem]:
        """
        Resource for tickets table. Uses DLT state to handle column renaming of custom fields to prevent changing the names of said columns.
        This resource uses pagination, loading and side loading to make API calls more efficient.

        Args:
            zendesk_client (Zenpy): An instance of Zenpy object used to make API calls to Zendesk.
            pivot_fields (bool): Indicates whether to pivot the custom fields in tickets. Defaults to True.
            per_page (int): The number of Ticket objects to load per page. Defaults to 1000.
            updated_at (dlt.sources.incremental[pendulum.DateTime]): Incremental source for the 'updated_at' column.
                Defaults to dlt.sources.incremental("updated_at", initial_value=incremental_start_time).

        Yields:
            TDataItem: Dictionary containing the ticket data.
        """
        kwargs: Dict[str, Any] = {}
        if backload_range:
            kwargs = backload_range.to_endpoint_kwargs()
        elif updated_at:
            kwargs = {"start_time": updated_at.last_value}

        # grab the custom fields from dlt state if any
        fields_dict = dlt.current.source_state().setdefault(CUSTOM_FIELDS_STATE_KEY, {})
        include_objects = ["users", "groups", "organisation", "brands"]
        ticket_pages = zendesk_client.get_pages_incremental(
            "/api/v2/incremental/tickets",
            "tickets",
            params={"include": ",".join(include_objects), "per_page": str(per_page)},
            **kwargs,
        )
        for page in ticket_pages:
            yield [
                process_ticket(ticket, fields_dict, pivot_custom_fields=pivot_fields)
                for ticket in page
            ]

    @dlt.resource(
        name="ticket_metric_events", primary_key="id", write_disposition="append"
    )
    def ticket_metric_table(
        zendesk_client: ZendeskAPIClient,
        time: Optional[dlt.sources.incremental[str]] = None,
        backload_range: Optional[SliceConfig] = None,
    ) -> Iterator[TDataItem]:
        """
        Resource for ticket metric events table. Returns all the ticket metric events from the starting date,
        with the default starting date being January 1st of the current year.

        Args:
            zendesk_client (Zenpy): An instance of Zenpy object used to make API calls to Zendesk.
            time (dlt.sources.incremental[str]): Incremental source for the 'time' column,
                indicating the starting date for retrieving ticket metric events.
                Defaults to dlt.sources.incremental("time", initial_value=incremental_start_time_iso_str).

        Yields:
            TDataItem: Dictionary containing the ticket metric event data.
        """
        kwargs: Dict[str, Any] = {}
        if backload_range:
            kwargs = backload_range.to_endpoint_kwargs()
        elif time:
            kwargs = {"start_time": parse_iso_like_datetime(time.last_value)}

        metric_event_pages = zendesk_client.get_pages_incremental(
            "/api/v2/incremental/ticket_metric_events", "ticket_metric_events", **kwargs
        )
        yield from metric_event_pages

    # Authenticate
    zendesk_client = ZendeskAPIClient(credentials)

    if backload_range:
        ticket_kwargs: Dict[str, Any] = {
            "backload_range": SliceConfig(
                start=backload_range[0], end=backload_range[1], cursor_path="updated_at"
            )
        }
        ticket_event_kwargs: Dict[str, Any] = {
            "backload_range": SliceConfig(
                start=backload_range[0], end=backload_range[1], cursor_path="timestamp"
            )
        }
        ticket_metric_kwargs: Dict[str, Any] = {
            "backload_range": SliceConfig(
                start=backload_range[0], end=backload_range[1], cursor_path="time"
            )
        }
    else:
        ticket_kwargs = {
            "updated_at": dlt.sources.incremental(
                "updated_at", initial_value=incremental_start_time
            ),
        }
        ticket_event_kwargs = {
            "timestamp": dlt.sources.incremental(
                "timestamp", initial_value=incremental_start_time_ts
            )
        }
        ticket_metric_kwargs = {
            "time": dlt.sources.incremental(
                "time", initial_value=incremental_start_time_iso_str
            )
        }

    # loading base tables
    resource_list = [
        ticket_fields_table(zendesk_client=zendesk_client),
        ticket_events(zendesk_client=zendesk_client, **ticket_event_kwargs),
        ticket_table(
            zendesk_client=zendesk_client,
            pivot_fields=pivot_ticket_fields,
            **ticket_kwargs,
        ),
        ticket_metric_table(zendesk_client=zendesk_client, **ticket_metric_kwargs),
    ]

    # other tables to be loaded
    # Tuple of resource_name, endpoint url, Optional[data_key]
    resources_to_be_loaded: List[Tuple[str, str, Optional[str]]] = [
        ("users", "/api/v2/users.json", None),
        ("sla_policies", "/api/v2/slas/policies.json", None),
        ("groups", "/api/v2/groups.json", None),
        ("organizations", "/api/v2/organizations.json", None),
        ("brands", "/api/v2/brands.json", None),
    ]
    if load_all:
        resources_to_be_loaded.extend(SUPPORT_EXTRA_ENDPOINTS)
    for resource, endpoint_url, data_key in resources_to_be_loaded:
        resource_list.append(
            dlt.resource(
                basic_resource(
                    zendesk_client=zendesk_client,
                    endpoint_url=endpoint_url,
                    data_key=data_key or resource,
                ),
                name=resource,
                write_disposition="replace",
            )
        )
    return resource_list


def basic_resource(
    zendesk_client: ZendeskAPIClient,
    endpoint_url: str,
    data_key: str,
    per_page: int = 1000,
) -> Iterator[TDataItem]:
    """
    Basic loader for most endpoints offered by Zenpy. Supports pagination. Expects to be called as a DLT Resource.

    Args:
        zendesk_client (Zenpy): An instance of Zenpy object used to make API calls to Zendesk.
        resource (str): The Zenpy endpoint to retrieve data from, usually directly linked to a Zendesk API endpoint.
        per_page (int, optional): The number of resources to retrieve per page. Defaults to 1000.

    Yields:
        TDataItem: Dictionary containing the resource data.
    """

    params = {"per_page": per_page}
    pages = zendesk_client.get_pages(endpoint_url, data_key, params)
    yield from pages

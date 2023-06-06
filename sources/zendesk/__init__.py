"""
Defines all the sources and resources needed for ZendeskSupport, ZendeskChat and ZendeskTalk
"""

from typing import Iterator, Optional, Sequence, Iterable

import dlt
from dlt.common import pendulum
from dlt.common.time import parse_iso_like_datetime
from dlt.common.typing import TDataItem
from dlt.extract.source import DltResource

from .helpers.api_helpers import (
    auth_zenpy,
    basic_load,
    process_ticket,
    Zenpy,
    process_ticket_field,
)
from .helpers.talk_api import ZendeskAPIClient
from .helpers.credentials import TZendeskCredentials, ZendeskCredentialsOAuth

from .settings import (
    DEFAULT_START_DATE,
    EXTRA_RESOURCES_SUPPORT,
    CUSTOM_FIELDS_STATE_KEY,
    TALK_ENDPOINTS,
    INCREMENTAL_ENDPOINTS,
)


@dlt.source(max_table_nesting=2)
def zendesk_talk(
    credentials: TZendeskCredentials = dlt.secrets.value,
    incremental_start_time: Optional[pendulum.DateTime] = DEFAULT_START_DATE,
) -> Iterable[DltResource]:
    """
    Retrieves data from Zendesk Talk for phone calls and voicemails.

    Args:
        credentials (TZendeskCredentials): The credentials for authentication. Defaults to the value in the `dlt.secrets` object.
        incremental_start_time (Optional[pendulum.DateTime]): The start time for incremental loading. Defaults to DEFAULT_START_DATE.

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
            updated_at=dlt.sources.incremental(
                "updated_at", initial_value=incremental_start_time.isoformat()
            ),
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
    yield from zendesk_client.make_request(
        endpoint=talk_endpoint, data_point_name=talk_endpoint_name
    )


def talk_incremental_resource(
    zendesk_client: ZendeskAPIClient,
    talk_endpoint_name: str,
    talk_endpoint: str,
    updated_at: dlt.sources.incremental[str],
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
    # send the request and process it
    yield from zendesk_client.make_request_incremental(
        endpoint=talk_endpoint,
        data_point_name=talk_endpoint_name,
        start_date=parse_iso_like_datetime(updated_at.last_value).int_timestamp,
    )


@dlt.source(max_table_nesting=2)
def zendesk_chat(
    credentials: ZendeskCredentialsOAuth = dlt.secrets.value,
    incremental_start_time: Optional[pendulum.DateTime] = DEFAULT_START_DATE,
) -> Iterable[DltResource]:
    """
    Retrieves data from Zendesk Chat for chat interactions.

    Args:
        credentials (ZendeskCredentialsOAuth): The credentials for authentication. Defaults to the value in the `dlt.secrets` object.
        incremental_start_time (Optional[pendulum.DateTime]): The start time for incremental loading. Defaults to DEFAULT_START_DATE.

    Yields:
        DltResource: Data resources from Zendesk Chat.
    """

    # Authenticate
    zendesk_client = auth_zenpy(credentials=credentials)
    yield dlt.resource(chats_table_resource, name="chats", write_disposition="append")(
        zendesk_client,
        dlt.sources.incremental(
            "update_timestamp|updated_timestamp",
            initial_value=incremental_start_time.isoformat(),
        ),
    )


def chats_table_resource(
    zendesk_client: Zenpy, update_timestamp: dlt.sources.incremental[str]
) -> Iterator[TDataItem]:
    """
    Resource for Chats

    Args:
        zendesk_client (Zenpy): Zenpy type object, used to make calls to Zendesk API through the Zenpy module.
        update_timestamp (dlt.sources.incremental[str]): Incremental source specifying the timestamp for incremental loading.

    Yields:
        dict: A dictionary representing each row of data.
    """
    # Send query and process it
    all_chats = zendesk_client.chats.incremental(
        start_time=parse_iso_like_datetime(update_timestamp.last_value).int_timestamp
    )
    yield [chat.to_dict() for chat in all_chats]


@dlt.source(max_table_nesting=2)
def zendesk_support(
    credentials: TZendeskCredentials = dlt.secrets.value,
    load_all: bool = True,
    pivot_ticket_fields: bool = True,
    incremental_start_time: Optional[pendulum.DateTime] = DEFAULT_START_DATE,
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

    incremental_start_time_ts = incremental_start_time.timestamp()
    incremental_start_time_iso_str = incremental_start_time.isoformat()

    @dlt.resource(primary_key="id", write_disposition="append")
    def ticket_events(
        zendesk_client: Zenpy,
        timestamp: dlt.sources.incremental[int] = dlt.sources.incremental(
            "timestamp", initial_value=incremental_start_time_ts
        ),
    ) -> Iterator[TDataItem]:
        result_generator = zendesk_client.tickets.events(
            start_time=int(timestamp.last_value)
        )
        yield [ticket.to_dict() for ticket in result_generator]

    @dlt.resource(name="ticket_fields", write_disposition="replace")
    def ticket_fields_table(zendesk_client: Zenpy) -> Iterator[TDataItem]:
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
        all_fields = zendesk_client.ticket_fields()
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
        zendesk_client: Zenpy,
        pivot_fields: bool = True,
        per_page: int = 1000,
        updated_at: dlt.sources.incremental[
            pendulum.DateTime
        ] = dlt.sources.incremental("updated_at", initial_value=incremental_start_time),
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

        # grab the custom fields from dlt state if any
        fields_dict = dlt.current.source_state().setdefault(CUSTOM_FIELDS_STATE_KEY, {})
        all_tickets = zendesk_client.tickets.incremental(
            paginate_by_time=False,
            per_page=per_page,
            start_time=int(updated_at.last_value.timestamp()),
            include=["users", "groups", "organisation", "brands"],
        )
        yield [
            process_ticket(
                ticket=ticket,
                custom_fields=fields_dict,
                pivot_custom_fields=pivot_fields,
            )
            for ticket in all_tickets
        ]

    @dlt.resource(
        name="ticket_metric_events", primary_key="id", write_disposition="append"
    )
    def ticket_metric_table(
        zendesk_client: Zenpy,
        time: dlt.sources.incremental[str] = dlt.sources.incremental(
            "time", initial_value=incremental_start_time_iso_str
        ),
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
        all_metric_events = zendesk_client.ticket_metric_events(
            start_time=parse_iso_like_datetime(time.last_value).int_timestamp
        )
        yield [metric_event.to_dict() for metric_event in all_metric_events]

    # Authenticate
    zendesk_client = auth_zenpy(credentials=credentials)

    # loading base tables
    resource_list = [
        ticket_fields_table(zendesk_client=zendesk_client),
        ticket_events(zendesk_client=zendesk_client),
        ticket_table(zendesk_client=zendesk_client, pivot_fields=pivot_ticket_fields),
        ticket_metric_table(zendesk_client=zendesk_client),
    ]

    # other tables to be loaded
    resources_to_be_loaded = [
        "users",
        "sla_policies",
        "groups",
        "organizations",
        "brands",
    ]
    if load_all:
        resources_to_be_loaded.extend(EXTRA_RESOURCES_SUPPORT)
    for resource in resources_to_be_loaded:
        resource_list.append(
            dlt.resource(
                basic_resource(zendesk_client=zendesk_client, resource=resource),
                name=resource,
                write_disposition="replace",
            )
        )
    return resource_list


def basic_resource(
    zendesk_client: Zenpy, resource: str, per_page: int = 1000
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
    try:
        # use the zenpy endpoint with methods to yield the data
        resource_api_method = getattr(zendesk_client, f"{resource}")
        resource_api = resource_api_method()[::per_page]
    except TypeError:
        # this resource doesn't support slicing for pagination
        resource_api_method = getattr(zendesk_client, f"{resource}")
        resource_api = resource_api_method()
    yield [d for d in basic_load(resource_api)]

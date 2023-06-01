"""
Defines all the sources and resources needed for ZendeskSupport, ZendeskChat and ZendeskTalk
"""

from typing import Iterator, Optional, Sequence, Union

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
from .helpers.talk_api import INCREMENTAL_ENDPOINTS, TALK_ENDPOINTS, ZendeskAPIClient
from .helpers.credentials import (
    ZendeskCredentialsEmailPass,
    ZendeskCredentialsOAuth,
    ZendeskCredentialsToken,
)


EXTRA_RESOURCES_SUPPORT = [
    "activities",
    "automations",
    "custom_agent_roles",
    "dynamic_content",
    "group_memberships",
    "job_status",
    "macros",
    "organization_fields",
    "organization_memberships",
    "recipient_addresses",
    "requests",
    "satisfaction_ratings",
    "sharing_agreements",
    "skips",
    "suspended_tickets",
    "targets",
    "ticket_forms",
    "ticket_metrics",
    "triggers",
    "user_fields",
    "views",
    "tags",
]
FIRST_DAY_OF_CURRENT_YEAR = pendulum.datetime(year=pendulum.now().year, month=1, day=1)
FIRST_DAY_OF_MILLENNIUM = pendulum.datetime(year=2000, month=1, day=1)
FIRST_DAY_OF_MILLENNIUM_STRING = "2000-01-01T00:00:00Z"

CUSTOM_FIELDS_STATE_KEY = "ticket_custom_fields_v2"


@dlt.source(max_table_nesting=2)
def zendesk_talk(
    credentials: Union[
        ZendeskCredentialsEmailPass, ZendeskCredentialsToken, ZendeskCredentialsOAuth
    ] = dlt.secrets.value,
    incremental_start_time: Optional[pendulum.DateTime] = FIRST_DAY_OF_MILLENNIUM,
) -> Sequence[DltResource]:
    def talk_resource(
        zendesk_client: ZendeskAPIClient, talk_endpoint_name: str, talk_endpoint: str
    ) -> Iterator[TDataItem]:
        """
        Called as a dlt resource. Generic loader for a ZendeskTalk endpoint.
        @:param zendesk_client: ZendeskAPIClient object that makes api calls to Zendesk Talk API
        @:param talk_endpoint_name: The name of the talk_endpoint
        @:param talk_endpoint: The actual url ending of this endpoint
        @:returns: Generator of dicts
        """
        # send query and process it
        client_response = zendesk_client.make_request(
            endpoint=talk_endpoint, data_point_name=talk_endpoint_name
        )
        yield from client_response

    def talk_incremental_resource(
        zendesk_client: ZendeskAPIClient,
        talk_endpoint_name: str,
        talk_endpoint: str,
        updated_at: dlt.sources.incremental[str],
    ) -> Iterator[TDataItem]:
        """
        Called as a dlt resource. Generic loader for a ZendeskTalk endpoint.
        @:param zendesk_client: ZendeskAPIClient object that makes api calls to Zendesk Talk API
        @:param talk_endpoint_name: The name of the talk_endpoint
        @:param talk_endpoint: The actual url ending of this endpoint
        @:returns: Generator of dicts
        """
        # send the request and process it
        client_response = zendesk_client.make_request_incremental(
            endpoint=talk_endpoint,
            data_point_name=talk_endpoint_name,
            start_date=parse_iso_like_datetime(updated_at.last_value).int_timestamp,
        )
        yield from client_response

    # use the credentials to authenticate with the ZendeskClient
    zendesk_client = ZendeskAPIClient(credentials)
    talk_resources = []
    # regular endpoints
    for key, talk_endpoint in TALK_ENDPOINTS.items():
        talk_resources.append(
            dlt.resource(
                talk_resource(
                    zendesk_client=zendesk_client,
                    talk_endpoint_name=key,
                    talk_endpoint=talk_endpoint,
                ),
                name=key,
                write_disposition="replace",
            )
        )
    # adding incremental endpoints
    for key, talk_incremental_endpoint in INCREMENTAL_ENDPOINTS.items():
        talk_resources.append(
            dlt.resource(
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
        )
    return talk_resources


@dlt.source(max_table_nesting=2)
def zendesk_chat(
    credentials: ZendeskCredentialsOAuth = dlt.secrets.value,
    incremental_start_time: Optional[pendulum.DateTime] = FIRST_DAY_OF_MILLENNIUM,
) -> DltResource:
    """
    The source for the dlt pipeline. It returns all the basic information. ZendeskChat accepts only OAuthCredentials.
    @:param credentials: read as a dict, as filled in .dlt.secrets.toml
    @:returns: multiple dlt resources
    """

    def chats_table(
        zendesk_client: Zenpy, update_timestamp: dlt.sources.incremental[str]
    ) -> Iterator[TDataItem]:
        """
        Resource for Chats
        @:param zendesk_client: Zenpy type object, used to make calls to Zendesk API through zenpy module
        @:param start_date: Datetime object that serves as the starting date of when to retrieve objects
        @:returns: Generator returning dicts for every row of data
        """
        # Send query and process it
        all_chats = zendesk_client.chats.incremental(
            start_time=parse_iso_like_datetime(
                update_timestamp.last_value
            ).int_timestamp
        )
        yield [chat.to_dict() for chat in all_chats]

    # Authenticate
    zendesk_client = auth_zenpy(credentials=credentials)
    return dlt.resource(chats_table, name="chats", write_disposition="append")(
        zendesk_client,
        dlt.sources.incremental(
            "update_timestamp|updated_timestamp",
            initial_value=incremental_start_time.isoformat(),
        ),
    )


@dlt.source(max_table_nesting=2)
def zendesk_support(
    credentials: Union[
        ZendeskCredentialsEmailPass, ZendeskCredentialsToken, ZendeskCredentialsOAuth
    ] = dlt.secrets.value,
    load_all: bool = True,
    pivot_ticket_fields: bool = True,
    incremental_start_time: Optional[pendulum.DateTime] = FIRST_DAY_OF_MILLENNIUM,
) -> Sequence[DltResource]:
    """
    The source for the dlt pipeline. It returns all the basic tables for Zendesk Support: tickets, users, brands, organizations, groups and all extra resources if required
    @:param credentials: read as a dict, as filled in .dlt.secrets.toml
    @:param load_all: bool, if true will load extra resources for the api
    @:param pivot_tickets: bool that indicates whether to pivot the custom fields in tickets, default set to true
    @:param incremental_start_time: Datetime that indicates when to start loading objects that use incremental loading
    @:returns: multiple dlt resources
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
        Dlt Resource that also writes to dlt state to update on custom ticket fields
        @:param zendesk_client: Zenpy Object that is used to make API calls to Zendesk API
        @:returns: Generator of dicts
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
        This resource uses pagination, loading and side loading to make API calls more efficient
        @:param zendesk_client: Zenpy type object, used to make calls to Zendesk API through zenpy module
        @:param pivot: bool that indicates whether to pivot the custom fields in tickets, default set to true
        @:param per_page: Describes how many Ticket objects are loaded per page, int
        @:returns: Generator returning dicts for every row of data
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
        Will return all the ticket metric events from the starting date with the default being 1st Jan of the current year
        @:param zendesk_client: Zenpy type object, used to make calls to Zendesk API through zenpy module
        @:returns: Generator returning dicts for every row of data
        """
        all_metric_events = zendesk_client.ticket_metric_events(
            start_time=parse_iso_like_datetime(time.last_value).int_timestamp
        )
        yield [metric_event.to_dict() for metric_event in all_metric_events]

    def basic_resource(
        zendesk_client: Zenpy, resource: str, per_page: int = 1000
    ) -> Iterator[TDataItem]:
        """
        Basic loader for most endpoints offered by Zenpy. Supports pagination. Expects to be called as a DLT Resource
        @:param zendesk_client: Zenpy type object, used to make calls to Zendesk API through zenpy module
        @:param resource: String that references the zenpy endpoint, usually directly linked to a ZendeskAPI endpoint, ex: users
        @:param per_page: Indicated how many resources should be retrieved per page with the default being 1000
        @:returns: Generator of dicts
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

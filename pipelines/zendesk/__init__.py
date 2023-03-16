"""
Defines all the sources and resources needed for ZendeskSupport, ZendeskChat and ZendeskTalk
"""

from typing import Iterator, Optional, Sequence, Union
import dlt
from dlt.common.exceptions import MissingDependencyException
from dlt.common.typing import TDataItem
from dlt.extract.source import DltResource
try:
    from pendulum import DateTime, datetime, from_timestamp, now
except ImportError:
    raise MissingDependencyException("Pendulum", ["pendulum"])
try:
    from zenpy import Zenpy
except ImportError:
    raise MissingDependencyException("Zenpy", ["zenpy~=2.0.25"])
from .helpers.api_helpers import auth_zenpy, basic_load, process_ticket
from .helpers.talk_api import INCREMENTAL_ENDPOINTS, TALK_ENDPOINTS, ZendeskAPIClient
from .helpers.credentials import ZendeskCredentialsEmailPass, ZendeskCredentialsOAuth, ZendeskCredentialsToken

EXTRA_RESOURCES = [
    "activities", "automations", "custom_agent_roles", "dynamic_content", "group_memberships", "job_status", "macros", "organization_fields", "organization_memberships", "recipient_addresses",
    "requests", "satisfaction_ratings", "sharing_agreements", "skips", "suspended_tickets", "targets", "ticket_forms", "ticket_metrics", "triggers", "user_fields", "views", "tags"
]
resources_to_be_loaded = ["users", "sla_policies", "groups", "organizations", "brands"]
FIRST_DAY_OF_CURRENT_YEAR = datetime(year=now().year, month=1, day=1)
FIRST_DAY_OF_MILLENNIUM = datetime(year=2000, month=1, day=1)


@dlt.source(max_table_nesting=2)
def zendesk_talk(credentials: Union[ZendeskCredentialsOAuth, ZendeskCredentialsToken, ZendeskCredentialsEmailPass] = dlt.secrets.value,
                 incremental_start_time: Optional[DateTime] = None) -> Sequence[DltResource]:
    # use the credentials to authenticate with the ZendeskClient
    zendesk_client = ZendeskAPIClient(credentials)
    talk_resources = []
    # regular endpoints
    for key, talk_endpoint in TALK_ENDPOINTS.items():
        talk_resources.append(dlt.resource(talk_resource(zendesk_client=zendesk_client, talk_endpoint_name=key, talk_endpoint=talk_endpoint),
                                           name=key,
                                           write_disposition="replace"))
    # adding incremental endpoints
    for key, talk_incremental_endpoint in INCREMENTAL_ENDPOINTS.items():
        talk_resources.append(dlt.resource(talk_incremental_resource(zendesk_client=zendesk_client, talk_endpoint_name=key, talk_endpoint=talk_incremental_endpoint, start_date=incremental_start_time),
                                           name=f"{key}_incremental",
                                           write_disposition="append"))
    return talk_resources


@dlt.source(max_table_nesting=2)
def zendesk_chat(credentials: Union[ZendeskCredentialsOAuth, ZendeskCredentialsToken, ZendeskCredentialsEmailPass] = dlt.secrets.value,
                 incremental_start_time: Optional[DateTime] = None) -> DltResource:
    """
    The source for the dlt pipeline. It returns all the basic information.
    @:param credentials: read as a dict, as filled in .dlt.secrets.toml
    @:returns: multiple dlt resources
    """
    # Authenticate
    zendesk_client = auth_zenpy(credentials=credentials)
    return chats_table(zendesk_client=zendesk_client, start_time=incremental_start_time)


@dlt.source(max_table_nesting=2)
def zendesk_support(credentials: Union[ZendeskCredentialsOAuth, ZendeskCredentialsToken, ZendeskCredentialsEmailPass] = dlt.secrets.value, load_all: bool = True, pivot_ticket_fields: bool = True,
                    incremental_start_time: Optional[DateTime] = None) -> Sequence[DltResource]:
    """
    The source for the dlt pipeline. It returns all the basic tables for Zendesk Support: tickets, users, brands, organizations, groups and all extra resources if required
    @:param credentials: read as a dict, as filled in .dlt.secrets.toml
    @:param load_all: bool, if true will load extra resources for the api
    @:param pivot_tickets: bool that indicates whether to pivot the custom fields in tickets, default set to true
    @:param incremental_start_time: Datetime that indicates when to start loading objects that use incremental loading
    @:returns: multiple dlt resources
    """

    # TODO: Make caching manageable and editable by users
    # Authenticate
    zendesk_client = auth_zenpy(credentials=credentials)
    resource_list = [ticket_fields_table(zendesk_client=zendesk_client),
                     ticket_table(zendesk_client=zendesk_client, pivot_fields=pivot_ticket_fields, start_time=incremental_start_time),
                     ticket_metric_table(zendesk_client=zendesk_client, start_time=incremental_start_time)]
    # load other api endpoints into resources if the option is to do so
    if load_all:
        resources_to_be_loaded.extend(EXTRA_RESOURCES)
    for resource in resources_to_be_loaded:
        resource_list.append(dlt.resource(basic_resource(zendesk_client=zendesk_client, resource=resource),
                                          name=resource,
                                          write_disposition="replace"))
    return resource_list


# Zendesk Support resources
@dlt.resource(name="ticket_fields", write_disposition="replace")
def ticket_fields_table(zendesk_client: Zenpy) -> Iterator[TDataItem]:
    """
    Dlt Resource that also writes to dlt state to update on custom ticket fields
    @:param zendesk_client: Zenpy Object that is used to make API calls to Zendesk API
    @:returns: Generator of dicts
    """
    # get dlt state
    ticket_custom_fields = dlt.state().setdefault("ticket_custom_fields", {})
    # get all custom fields and update state if needed, otherwise just load dicts into tables
    all_fields = zendesk_client.ticket_fields()
    for field in all_fields:
        return_dict = field.to_dict()
        field_id = str(field.id)
        # grab id and update state dict if the id is new, add a new key to indicate that this is the initial value for title
        if not (field_id in ticket_custom_fields):
            ticket_custom_fields[field_id] = field.title
            return_dict["initial_title"] = field.title
        yield return_dict


@dlt.resource(name="tickets", write_disposition="append")
def ticket_table(zendesk_client: Zenpy, pivot_fields: bool = True, per_page: int = 1000, start_time: Optional[DateTime] = None) -> Iterator[TDataItem]:
    """
    Resource for tickets table. Uses DLT state to handle column renaming of custom fields to prevent changing the names of said columns.
    This resource uses pagination, loading and side loading to make API calls more efficient
    @:param zendesk_client: Zenpy type object, used to make calls to Zendesk API through zenpy module
    @:param pivot: bool that indicates whether to pivot the custom fields in tickets, default set to true
    @:param per_page: Describes how many Ticket objects are loaded per page, int
    @:parm start_time: The datetime for which Tickets start to load, tickets before this date will not be loaded
    @:returns: Generator returning dicts for every row of data
    """

    # grab the start time from last dlt load if not filled, if that is also empty then use the first day of the millennium as the start time instead
    if not start_time:
        start_time = dlt.state().setdefault("last_load_tickets", FIRST_DAY_OF_MILLENNIUM.timestamp())
    else:
        # if start time is given convert to a timestamp as it is required by the zenpy api client
        start_time = start_time.timestamp()
    # grab the custom fields from dlt state if any
    fields_dict = dlt.state().setdefault("ticket_custom_fields", {})
    all_tickets = zendesk_client.tickets.incremental(paginate_by_time=False,
                                                     per_page=per_page,
                                                     start_time=start_time,
                                                     include=["users", "groups", "organisation", "brands"])
    for ticket in all_tickets:
        ticket_dict = process_ticket(ticket=ticket, custom_fields=fields_dict, pivot_fields=pivot_fields)
        yield ticket_dict
    # update the last load time in the dlt state
    dlt.state()["last_load_tickets"] = now().timestamp()


@dlt.resource(name="ticket_metric_events", write_disposition="replace")
def ticket_metric_table(zendesk_client: Zenpy, start_time: Optional[DateTime] = None) -> Iterator[TDataItem]:
    """
    Will return all the ticket metric events from the starting date with the default being 1st Jan of the current year
    @:param zendesk_client: Zenpy type object, used to make calls to Zendesk API through zenpy module
    @:param start_time: Datetime on when to start loading for ticket metric events
    @:returns: Generator returning dicts for every row of data
    """

    # grab the start time from last dlt load if not filled, if that is also empty then use the first day of the millennium as the start time instead
    # because of Zenpy this needs to be a Datetime instead of a timestamp
    if not start_time:
        # convert from timestamp to datetime object since it is required by Zenpy
        start_time = from_timestamp(dlt.state().setdefault("last_load_ticket_metric_events", FIRST_DAY_OF_MILLENNIUM.timestamp()))
    all_metric_events = zendesk_client.ticket_metric_events(start_time=start_time)
    for metric_event in all_metric_events:
        yield metric_event.to_dict()
    # update the last load time in the dlt state, save as timestamp since datetime not supported by dlt state
    dlt.state()["last_load_ticket_metric_events"] = now().timestamp()


def basic_resource(zendesk_client: Zenpy, resource: str, per_page: int = 1000) -> Iterator[TDataItem]:
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
    dict_generator = basic_load(resource_api=resource_api)
    yield from dict_generator


# Chat resources
@dlt.resource(name="chats", write_disposition="append")
def chats_table(zendesk_client: Zenpy, start_time: Optional[DateTime] = None) -> Iterator[TDataItem]:
    """
    Resource for Chats
    @:param zendesk_client: Zenpy type object, used to make calls to Zendesk API through zenpy module
    @:param start_date: Datetime object that serves as the starting date of when to retrieve objects
    @:returns: Generator returning dicts for every row of data
    """
    # grab the start time from last dlt load if not filled, if that is also empty then use the first day of the millennium as the start time instead
    if not start_time:
        # the dlt state will store a timestamp, we need to convert back to datetime for Zenpy
        start_time = from_timestamp(dlt.state().setdefault("last_load_chats", FIRST_DAY_OF_MILLENNIUM.timestamp()))
    all_chats = zendesk_client.chats.incremental(start_time=start_time)
    dict_generator = basic_load(resource_api=all_chats)
    yield from dict_generator
    # update the last load time in the dlt state and save as timestamp since datetime is not supported by dlt state
    dlt.state()["last_load_chats"] = now().timestamp()


# Talk resources
def talk_resource(zendesk_client: ZendeskAPIClient, talk_endpoint_name: str, talk_endpoint: str) -> Iterator[TDataItem]:
    """
    Called as a dlt resource. Generic loader for a ZendeskTalk endpoint.
    @:param zendesk_client: ZendeskAPIClient object that makes api calls to Zendesk Talk API
    @:param talk_endpoint_name: The name of the talk_endpoint
    @:param talk_endpoint: The actual url ending of this endpoint
    @:returns: Generator of dicts
    """
    my_req = zendesk_client.make_request(endpoint=talk_endpoint, data_point_name=talk_endpoint_name)
    if my_req:
        my_pages = [page for page in my_req]
        for page in my_pages:
            # multiple records in page
            if isinstance(page, list):
                for record in page:
                    yield record
            # only 1 record in the page, just yield page
            else:
                yield page


def talk_incremental_resource(zendesk_client: ZendeskAPIClient, talk_endpoint_name: str, talk_endpoint: str, start_date: Optional[DateTime] = None) -> Iterator[TDataItem]:
    """
    Called as a dlt resource. Generic loader for a ZendeskTalk endpoint.
    @:param zendesk_client: ZendeskAPIClient object that makes api calls to Zendesk Talk API
    @:param talk_endpoint_name: The name of the talk_endpoint
    @:param talk_endpoint: The actual url ending of this endpoint
    @:returns: Generator of dicts
    """
    # grab the start time from last dlt load if not filled, if that is also empty then use the first day of the millennium as the start time instead
    if not start_date:
        final_start_date = dlt.state().setdefault(f"last_load_talk_{talk_endpoint_name}", FIRST_DAY_OF_MILLENNIUM.timestamp())
    else:
        final_start_date = start_date.timestamp()
    my_req = zendesk_client.make_request_incremental(endpoint=talk_endpoint, data_point_name=talk_endpoint_name, start_date=final_start_date)
    if my_req:
        my_pages = [page for page in my_req]
        for page in my_pages:
            # multiple records in page
            if isinstance(page, list):
                for record in page:
                    yield record
            # only 1 record in the page, just yield page
            else:
                yield page
    # update the last load time in the dlt state
    dlt.state()[f"last_load_talk_{talk_endpoint_name}"] = now().timestamp()

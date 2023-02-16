from typing import Any, Iterator, cast
from pipelines.zendesk.helpers.api_helpers import auth_zendesk
from pipelines.zendesk.helpers.populate_zendesk_test import add_users_and_tickets
import dlt
from dlt.common.typing import DictStrAny, TDataItem
import logging
from zenpy import Zenpy
from zenpy.lib.api_objects import Ticket
import time

ALL_RESOURCES = [
    'users', 'user_fields', 'groups', 'macros', 'organizations', 'organization_memberships', 'organization_fields', 'tickets', 'suspended_tickets', 'topics', 'attachments',
    'brands', 'job_status', 'jira_links', 'tags', 'satisfaction_ratings', 'sharing_agreements', 'skips', 'activities', 'group_memberships', 'end_user', 'ticket_metrics', 'ticket_metric_events',
    'ticket_fields', 'ticket_forms', 'ticket_import', 'requests', 'chats', 'views', 'help_center', 'recipient_addresses', 'nps', 'triggers', 'automations', 'dynamic_content',
    'targets', 'talk', 'talk_pe', 'custom_agent_roles', 'zis'
]


@dlt.source
def zendesk(credentials: dict = dlt.secrets.value, load_all: bool = True) -> Any:
    """
    The source for the dlt pipeline. It returns the following resources: 1 dlt resource for every range in sheet_names
    @:param: spreadsheet_identifier - the id or url to the spreadsheet
    @:param: sheet_names - A list of ranges in the spreadsheet of the type sheet_name!range_name. These are the ranges to be converted into tables
    @:param: credentials - GCP credentials to the account with Google Sheets api access, defined in dlt.secrets
    @:return: multiple dlt resources
    """

    # TODO: Improve users
    # TODO: Improve tickets
    # TODO: Implement ticket_metric events
    # TODO: Implement Chat
    # TODO: Implement Talk

    # authenticate to the service using the helper function
    zen_client = auth_zendesk(credentials=credentials)
    resource_list = [ticket_table(zen_client=zen_client), organization_table(zen_client=zen_client), user_table(zen_client=zen_client), groups_table(zen_client=zen_client),
                     sla_policy_table(zen_client=zen_client)]
    if load_all:
        for extra_resource in ALL_RESOURCES:
            try:
                resource_api_method = getattr(zen_client, extra_resource)
                resource_api = resource_api_method()
                resource_list.append(dlt.resource(basic_resource(resource=resource_api),
                                                  name=f"{extra_resource}_basic",
                                                  write_disposition="replace"
                                                  )
                                     )
            except:
                print(f"Skipped resource: {extra_resource}")
                continue
    return resource_list


@dlt.resource(name="sla_policies", write_disposition="replace")
def sla_policy_table(zen_client: Zenpy) -> Iterator[TDataItem]:
    """
    Resource for sla policies table
    @:param: zen_client - Zenpy type object, used to make calls to Zendesk API through zenpy module
    @:returns: Generator returning dicts for every row of data
    """

    all_policies = zen_client.sla_policies()
    for policy in all_policies:
        policy_json = policy.to_json()
        policy_dict = policy.to_dict()
        yield policy_dict


@dlt.resource(name="tickets", write_disposition="replace")
def ticket_table(zen_client: Zenpy) -> Iterator[TDataItem]:
    """
    Resource for tickets table
    @:param: zen_client - Zenpy type object, used to make calls to Zendesk API through zenpy module
    @:returns: Generator returning dicts for every row of data
    """

    start = time.time()
    all_tickets = zen_client.tickets()
    for ticket in all_tickets:
        ticket_json = ticket.to_json()
        ticket_dict = ticket.to_dict()
        yield ticket_dict
    end = time.time()
    print(f"Execution time tickets: {end-start}")


@dlt.resource(name="organizations", write_disposition="replace")
def organization_table(zen_client: Zenpy) -> Iterator[TDataItem]:
    """
    Resource for organizations table
    @:param: zen_client - Zenpy type object, used to make calls to Zendesk API through zenpy module
    @:returns: Generator returning dicts for every row of data
    """

    start = time.time()
    all_orgs = zen_client.organizations()
    for org in all_orgs:
        org_json = org.to_json()
        org_dict = org.to_dict()
        yield org_dict
    end = time.time()
    print(f"Execution time for organizations: {end-start}")


@dlt.resource(name="users", write_disposition="replace")
def user_table(zen_client: Zenpy) -> Iterator[TDataItem]:
    """
    Resource for users table.
    @:param: zen_client - Zenpy type object, used to make calls to Zendesk API through zenpy module
    @:returns: Generator returning dicts for every row of data
    """
    start = time.time()
    all_users = zen_client.users()
    for user in all_users:
        usr_json = user.to_json()
        usr_dict = user.to_dict()
        yield usr_dict
    end = time.time()
    print(f"Execution time users: {end-start}")


@dlt.resource(name="groups", write_disposition="replace")
def groups_table(zen_client: Zenpy):

    start = time.time()
    all_groups = zen_client.groups()
    for group in all_groups:
        group_json = group.to_json()
        group_dict = group.to_dict()
        yield group_dict
    end = time.time()
    print(f"Execution time groups: {end-start}")


@dlt.resource(name="ticket_metric_events", write_disposition="replace")
def ticket_metric_events_table(zen_client: Zenpy):
    pass


@dlt.resource(name="chat", write_disposition="replace")
def chat_table(zen_client: Zenpy):
    pass


@dlt.resource(name="talk", write_disposition="replace")
def talk_table(zen_client: Zenpy):
    pass


@dlt.resource(name="brands", write_disposition="replace")
def brands_table(zen_client: Zenpy):
    pass


def basic_resource(resource: Any):
    """
    Basic loader for most endpoints offered by Zenpy
    """
    if isinstance(resource, dict):
        yield dlt_clean_dict(resource)
    else:
        for el in resource:
            if isinstance(el, dict):
                yield dlt_clean_dict(el)
                continue
            try:
                dict_res = el.to_dict()
                yield dlt_clean_dict(dict_res)
            except AttributeError as e:
                print(f"Error! Element {el} of type {type(el)} returned from api {resource} has no attribute to_dict")


def dlt_clean_dict(unclean_dict: dict):
    """
    Helper to make sure a dict will always have atomic values
    """
    clean_dict = {}
    for key in unclean_dict.keys():
        if isinstance(unclean_dict[key], (list, dict, tuple)):
            clean_dict[key] = str(unclean_dict[key])
        else:
            clean_dict[key] = unclean_dict[key]
    return clean_dict

from typing import Any, Iterator, cast
from pipelines.zendesk.helpers.api_helpers import auth_zendesk
import dlt
from dlt.common.typing import DictStrAny, TDataItem
import logging
from zenpy import Zenpy
from zenpy.lib.api_objects import Ticket


@dlt.source
def zendesk(credentials: dict = dlt.secrets.value) -> Any:
    """
    The source for the dlt pipeline. It returns the following resources: 1 dlt resource for every range in sheet_names
    @:param: spreadsheet_identifier - the id or url to the spreadsheet
    @:param: sheet_names - A list of ranges in the spreadsheet of the type sheet_name!range_name. These are the ranges to be converted into tables
    @:param: credentials - GCP credentials to the account with Google Sheets api access, defined in dlt.secrets
    @:return: multiple dlt resources
    """
    # authenticate to the service using the helper function
    zen_client = auth_zendesk(credentials=credentials)
    return ticket_table(zen_client=zen_client), organization_table(zen_client=zen_client), user_table(zen_client=zen_client)


@dlt.resource(name="tickets", write_disposition="replace")
def ticket_table(zen_client: Zenpy) -> Iterator[TDataItem]:
    """
    Resource for tickets table
    @:param: zen_client - Zenpy type object, used to make calls to Zendesk API through zenpy module
    @:returns: Generator returning dicts for every row of data
    """
    for ticket in zen_client.tickets():
        ticket_dict = ticket.to_dict()
        tag_str = ""
        for tag in ticket.tags:
            tag_str += f"{tag}, "
        tag_str = tag_str[:-2]
        yield {
            "ticket_id": ticket.id,
            "description": ticket.description,
            "submitter_id": ticket.submitter_id,
            "status": ticket.status,
            "priority": ticket.priority,
            "due_date": ticket.due,
            "created_at": ticket.created,
            "last_update": ticket.updated,
            "type": ticket.type,
            "tags": tag_str
        }


@dlt.resource(name="organizations", write_disposition="replace")
def organization_table(zen_client: Zenpy) -> Iterator[TDataItem]:
    """
    Resource for organizations table
    @:param: zen_client - Zenpy type object, used to make calls to Zendesk API through zenpy module
    @:returns: Generator returning dicts for every row of data
    """
    for organization in zen_client.organizations():
        yield {
            "id": organization.id,
            "name": organization.name,
            "notes": organization.notes,
            "date_created": organization.created,
            "last_updated": organization.updated
        }


@dlt.resource(name="users", write_disposition="replace")
def user_table(zen_client: Zenpy) -> Iterator[TDataItem]:
    """
    Resource for users table.
    @:param: zen_client - Zenpy type object, used to make calls to Zendesk API through zenpy module
    @:returns: Generator returning dicts for every row of data
    """
    for user in zen_client.users():
        yield {
            "id": user.id,
            "username": user.name,
            "email": user.email,
            "phone_number": user.phone,
            "date_created": user.created,
            "last_updated": user.updated
        }

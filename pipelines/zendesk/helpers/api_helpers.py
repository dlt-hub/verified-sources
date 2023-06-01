import json
from typing import Iterator, Any, Optional, Union, TypedDict, Dict, Iterable
from dlt.common import logger, pendulum
from dlt.common.exceptions import MissingDependencyException
from dlt.common.typing import DictStrAny, DictStrStr, TDataItem, TDataItems

try:
    from zenpy import Zenpy
    from zenpy.lib.api_objects import Ticket, JobStatus, TicketField
except ImportError:
    raise MissingDependencyException("Zenpy", ["zenpy>=2.0.25"])
from .credentials import (
    ZendeskCredentialsToken,
    ZendeskCredentialsEmailPass,
    ZendeskCredentialsOAuth,
    TZendeskCredentials,
)


class TCustomFieldInfo(TypedDict):
    title: str
    options: DictStrStr


def auth_zenpy(
    credentials: TZendeskCredentials,
    domain: str = "zendesk.com",
    timeout: Optional[float] = None,
    ratelimit_budget: Optional[int] = None,
    proactive_ratelimit: Optional[int] = None,
    proactive_ratelimit_request_interval: int = 10,
    disable_cache: bool = True,
) -> Zenpy:
    """
    Helper function that authenticates to the Zendesk API using the provided credentials.

    Args:
        credentials (TZendeskCredentials): The Zendesk credentials object that stores the credentials.
        domain (str, optional): The domain of your Zendesk company account. Defaults to "zendesk.com".
        timeout (float, optional): The global timeout on Zenpy. Defaults to None.
        ratelimit_budget (int, optional): The maximum amount of time that the user can spend being rate limited.
            Defaults to None.
        proactive_ratelimit (int, optional): User-imposed rate limit due to budgeting issues, etc. Defaults to None.
        proactive_ratelimit_request_interval (int, optional): Indicates how many seconds to wait when rate limit is reached.
            Defaults to 10 seconds.
        disable_cache (bool, optional): If True, disables the caching of already retrieved objects. Defaults to True.

    Returns:
        Zenpy: An API client to make requests to the Zendesk API.
    """
    # oauth token is the preferred way to authenticate, followed by api token and then email + password combo
    if isinstance(credentials, ZendeskCredentialsOAuth):
        zendesk_client = Zenpy(
            subdomain=credentials.subdomain,
            oauth_token=credentials.oauth_token,
            domain=domain,
            timeout=timeout,
            ratelimit_budget=ratelimit_budget,
            proactive_ratelimit=proactive_ratelimit,
            proactive_ratelimit_request_interval=proactive_ratelimit_request_interval,
            disable_cache=disable_cache,
        )
        logger.info("Zenpy Received OAuth Credentials.")
    elif isinstance(credentials, ZendeskCredentialsToken):
        zendesk_client = Zenpy(
            token=credentials.token,
            email=credentials.email,
            subdomain=credentials.subdomain,
            domain=domain,
            timeout=timeout,
            ratelimit_budget=ratelimit_budget,
            proactive_ratelimit=proactive_ratelimit,
            proactive_ratelimit_request_interval=proactive_ratelimit_request_interval,
            disable_cache=disable_cache,
        )
        logger.info("Zenpy Received API token Credentials.")
    elif isinstance(credentials, ZendeskCredentialsEmailPass):
        zendesk_client = Zenpy(
            email=credentials.email,
            subdomain=credentials.subdomain,
            password=credentials.password,
            domain=domain,
            timeout=timeout,
            ratelimit_budget=ratelimit_budget,
            proactive_ratelimit=proactive_ratelimit,
            proactive_ratelimit_request_interval=proactive_ratelimit_request_interval,
            disable_cache=disable_cache,
        )
        logger.info("Zenpy Received Email and Password Credentials.")
    return zendesk_client


def process_ticket(
    ticket: Ticket,
    custom_fields: Dict[str, TCustomFieldInfo],
    pivot_custom_fields: bool = True,
) -> DictStrAny:
    """
    Helper function that processes a ticket object and returns a dictionary of ticket data.

    Args:
        ticket (Ticket): The ticket object returned by a Zenpy API call.
        custom_fields (Dict[str, TCustomFieldInfo]): A dictionary containing all the custom fields available for tickets.
        pivot_custom_fields (bool, optional): A boolean indicating whether to pivot all custom fields or not.
            Defaults to True.

    Returns:
        DictStrAny: A dictionary containing cleaned data about a ticket.
    """
    base_dict: DictStrAny = ticket.to_dict()

    # pivot custom field if indicated as such
    # get custom fields
    for custom_field in base_dict["custom_fields"]:
        if pivot_custom_fields:
            cus_field_id = str(custom_field["id"])
            field = custom_fields[cus_field_id]
            field_name = field["title"]
            current_value = custom_field["value"]
            options = field["options"]
            # Map dropdown values to labels
            if not current_value or not options:
                base_dict[field_name] = current_value
            elif isinstance(
                current_value, list
            ):  # Multiple choice field has a list of values
                base_dict[field_name] = [options.get(key, key) for key in current_value]
            else:
                base_dict[field_name] = options.get(current_value)
        else:
            custom_field["ticket_id"] = ticket.id
    # delete fields that are not needed for pivoting
    if pivot_custom_fields:
        del base_dict["custom_fields"]
    del base_dict["fields"]
    base_dict = _make_json_serializable(base_dict)
    # modify dates to return datetime objects instead
    base_dict["updated_at"] = ticket.updated
    base_dict["created_at"] = ticket.created
    base_dict["due_at"] = ticket.due
    return base_dict


def process_ticket_field(
    field: TicketField, custom_fields_state: Dict[str, TCustomFieldInfo]
) -> TDataItem:
    """Update custom field mapping in dlt state for the given field."""
    return_dict = field.to_dict()
    field_id = str(field.id)
    # grab id and update state dict
    # if the id is new, add a new key to indicate that this is the initial value for title
    # New dropdown options are added to existing field but existing options are not changed
    options = getattr(field, "custom_field_options", [])
    new_options = {o.value: o.name for o in options}
    existing_field = custom_fields_state.get(field_id)
    if existing_field:
        existing_options = existing_field["options"]
        if return_options := return_dict.get("custom_field_options"):
            for item in return_options:
                item["name"] = existing_options.get(item["value"], item["name"])
        for key, value in new_options.items():
            if key not in existing_options:
                existing_options[key] = value
    else:
        custom_fields_state[field_id] = dict(title=field.title, options=new_options)
        return_dict["initial_title"] = field.title
    return return_dict


def basic_load(resource_api: Iterator[Any]) -> Iterator[TDataItem]:
    """
    Receives a generator/iterable of Zenpy objects and returns a generator of dictionaries.

    Args:
        resource_api (Iterator[Any]): Generator/iterable of Zenpy objects.

    Yields:
        Iterator[TDataItem]: Generator of dictionaries.
    """
    # sometimes there is a single element which is a dict instead of a generator of objects
    if isinstance(resource_api, dict):
        yield _make_json_serializable(resource_api)
    else:
        # some basic resources return a dict instead of objects
        for element in resource_api:
            if isinstance(element, dict):
                yield _make_json_serializable(element)
            else:
                dict_res = _make_json_serializable(element.to_dict())
                yield dict_res


def _make_json_serializable(the_dict: DictStrAny) -> DictStrAny:
    """
    Helper that makes a dict JSON serializable.

    Args:
        the_dict (DictStrAny): The dictionary that needs to be made JSON serializable.

    Returns:
        DictStrAny: Processed dictionary that no longer contains any non-JSON serializable values.
    """

    for key, value in the_dict.items():
        try:
            json.dumps(the_dict[key])
        except (TypeError, OverflowError):
            the_dict[key] = str(value)
    return the_dict

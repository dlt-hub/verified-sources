from dlt.common import logger
from dlt.common.typing import TDataItem
from typing import Iterator, Union
from zenpy import Zenpy
from zenpy.lib.api_objects import Ticket


class ZendeskCredentials:
    """
    This class is used to store credentials from a dict of Zendesk Credentials
    Subdomain is always required  and there are 3 alternative authentication ways
    1. email + token
    2. email + password
    3. oauth_token
    """
    subdomain: Union[str, None]
    email: Union[str, None]
    token: Union[str, None]
    oauth_token: Union[str, None]
    password: Union[str, None]

    def __init__(self, cred_dict: dict):
        for key, value in cred_dict.items():
            setattr(self, key, value)


def auth_zendesk(credentials: ZendeskCredentials, domain: str = "zendesk.com", timeout: Union[float, None] = None, ratelimit_budget: Union[int, None] = None,
                 proactive_ratelimit: Union[int, None] = None, proactive_ratelimit_request_interval: int = 10, disable_cache: bool = False) -> Zenpy:
    """
    Helper, gets a Zendesk Credentials object and authenticates to the Zendesk API
    @:param: credentials - Zendesk Credentials Object, stores the credentials
    @:param disable_cache: bool, If true disables the caching of already retrieved objects.
    @:param domain: str, domain of your Zendesk company account
    @:param proactive_ratelimit: int, user imposed rate limit due to budgeting issues, etc.
    @:param proactive_ratelimit_request_interval: int, indicates how many seconds to wait when rate limit is reached, default is 10 seconds
    @:param ratelimit_budget: int that sets the maximum amount of time that the user can spend being rate limited
    @:param timeout: float that sets the global timeout on Zenpy
    @:returns: API client to make requests to Zendesk API
    """
    # 3 alternate ways to authenticate, all fields for at least one need to be active to authenticate, can't have tokens and password at the same time
    if credentials.password:
        credentials.token = None
    # zenpy currently will handle most errors
    # fill fields that were filled
    zen_client = Zenpy(
        token=credentials.token,
        email=credentials.email,
        subdomain=credentials.subdomain,
        password=credentials.password,
        oauth_token=credentials.oauth_token,
        domain=domain,
        timeout=timeout,
        ratelimit_budget=ratelimit_budget,
        proactive_ratelimit=proactive_ratelimit,
        proactive_ratelimit_request_interval=proactive_ratelimit_request_interval,
        disable_cache=disable_cache
    )
    return zen_client


def process_ticket(sample_ticket: Ticket, custom_fields: dict, pivot_fields: bool = True) -> dict:
    """
    Helper that returns a dictionary of the ticket class provided as a parameter. This is done since to_dict() method of Ticket class doesn't return all the required information
    @:param sample_ticket: Ticket Object returned by a Zenpy API call, contains info on a single ticket.
    @:param custom_fields: A dict containing all the custom fields available for tickets.
    @:param pivot_fields: Bool that indicates whether to pivot all custom fields or not.
    @:return base_dict: A dict containing 'cleaned' data about a ticket.
    """

    base_dict = sample_ticket.to_dict()
    # get tags as a string eliminating the square brackets from string
    base_dict["tags"] = str(base_dict["tags"])[1:-1]

    # pivot custom field if indicated as such
    # get custom fields
    for custom_field in base_dict["custom_fields"]:
        if pivot_fields:
            cus_field_id = str(custom_field["id"])
            field_name = custom_fields[cus_field_id]
            base_dict[field_name] = custom_field["value"]
        else:
            custom_field["ticket_id"] = sample_ticket.id

    # delete fields that are not needed for pivoting
    if pivot_fields:
        del base_dict["custom_fields"]
    del base_dict["fields"]

    # modify dates to return datetime objects instead
    base_dict["updated_at"] = sample_ticket.updated
    base_dict["created_at"] = sample_ticket.created
    base_dict["due_at"] = sample_ticket.due
    return base_dict


def basic_load(resource_api: Iterator) -> Iterator[TDataItem]:
    """
    Receives a generator/iterable of Zenpy Objects and returns a generator of dicts. Loader helper
    @:param resource_api: generator/iterable of Zenpy Objects
    """
    # sometimes there is a single element which is a dict instead of a generator of objects
    if isinstance(resource_api, dict):
        yield resource_api
    else:
        # some basic resources return a dict instead of objects
        for element in resource_api:
            if isinstance(element, dict):
                yield element
                continue
            try:
                dict_res = element.to_dict()
                yield dict_res
            except AttributeError as e:
                logger.warning(f"Error! Element {element} of type {type(element)} returned from api {resource_api} has no attribute to_dict")
                logger.warning(str(e))

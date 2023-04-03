from typing import Iterator, Any, Optional, Union
from dlt.common import logger, pendulum
from dlt.common.exceptions import MissingDependencyException
from dlt.common.typing import DictStrAny, DictStrStr, TDataItem, TDataItems
try:
    from zenpy import Zenpy
    from zenpy.lib.api_objects import Ticket
except ImportError:
    raise MissingDependencyException("Zenpy", ["zenpy>=2.0.25"])
from .credentials import ZendeskCredentialsToken, ZendeskCredentialsEmailPass, ZendeskCredentialsOAuth


def auth_zenpy(credentials: Union[ZendeskCredentialsOAuth, ZendeskCredentialsToken, ZendeskCredentialsEmailPass], domain: str = "zendesk.com", timeout: Optional[float] = None,
               ratelimit_budget: Optional[int] = None, proactive_ratelimit: Optional[int] = None, proactive_ratelimit_request_interval: int = 10, disable_cache: bool = False) -> Zenpy:
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
    # oauth token is the preferred way to authenticate, followed by api token and then email + password combo
    if isinstance(credentials, ZendeskCredentialsOAuth):
        zendesk_client = Zenpy(
            subdomain=credentials.subdomain, oauth_token=credentials.oauth_token,
            domain=domain, timeout=timeout, ratelimit_budget=ratelimit_budget, proactive_ratelimit=proactive_ratelimit, proactive_ratelimit_request_interval=proactive_ratelimit_request_interval,
            disable_cache=disable_cache
        )
        logger.info("Zenpy Received OAuth Credentials.")
    elif isinstance(credentials, ZendeskCredentialsToken):
        zendesk_client = Zenpy(
            token=credentials.token, email=credentials.email, subdomain=credentials.subdomain,
            domain=domain, timeout=timeout, ratelimit_budget=ratelimit_budget, proactive_ratelimit=proactive_ratelimit, proactive_ratelimit_request_interval=proactive_ratelimit_request_interval,
            disable_cache=disable_cache
        )
        logger.info("Zenpy Received API token Credentials.")
    elif isinstance(credentials, ZendeskCredentialsEmailPass):
        zendesk_client = Zenpy(
            email=credentials.email, subdomain=credentials.subdomain, password=credentials.password,
            domain=domain, timeout=timeout, ratelimit_budget=ratelimit_budget, proactive_ratelimit=proactive_ratelimit, proactive_ratelimit_request_interval=proactive_ratelimit_request_interval,
            disable_cache=disable_cache
        )
        logger.info("Zenpy Received Email and Password Credentials.")
    return zendesk_client


def process_ticket(ticket: Ticket, custom_fields: DictStrStr, pivot_custom_fields: bool = True) -> DictStrAny:
    """
    Helper that returns a dictionary of the ticket class provided as a parameter. This is done since to_dict()
    method of Ticket class doesn't return all the required information
    @:param sample_ticket: Ticket Object returned by a Zenpy API call, contains info on a single ticket.
    @:param custom_fields: A dict containing all the custom fields available for tickets.
    @:param pivot_fields: Bool that indicates whether to pivot all custom fields or not.
    @:return base_dict: A dict containing 'cleaned' data about a ticket.
    """

    base_dict: DictStrAny = ticket.to_dict()
    # get tags as a string eliminating the square brackets from string
    base_dict["tags"] = base_dict["tags"]

    # pivot custom field if indicated as such
    # get custom fields
    for custom_field in base_dict["custom_fields"]:
        if pivot_custom_fields:
            cus_field_id = str(custom_field["id"])
            field_name = custom_fields[cus_field_id]
            base_dict[field_name] = custom_field["value"]
        else:
            custom_field["ticket_id"] = ticket.id

    # delete fields that are not needed for pivoting
    if pivot_custom_fields:
        del base_dict["custom_fields"]

    del base_dict["fields"]

    # modify dates to return datetime objects instead
    base_dict["updated_at"] = ticket.updated
    base_dict["created_at"] = ticket.created
    base_dict["due_at"] = ticket.due
    return base_dict


def basic_load(resource_api: Iterator[Any]) -> Iterator[TDataItem]:
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
            else:
                dict_res = element.to_dict()
                yield dict_res


def process_talk_resource(response: Iterator[TDataItems]) -> Iterator[DictStrAny]:
    """
    Processes the response from a request to the ZendeskTalk API and yields results one line at a time for every page in the response
    :param response: Can contain one or multiple pages of data, if multiple pages this will be a generator of lists containing data (dictionaries), otherwise it will just a list
    of dictionaries
    :returns: A generator of dicts
    """
    if response:
        my_pages = [page for page in response]
        for page in my_pages:
            # multiple records in page
            if isinstance(page, list):
                for record in page:
                    yield record
            # only 1 record in the page, just yield page
            else:
                yield page


def get_latest_timestamp(chosen_col: str, default_col: str, object_dict: DictStrAny) -> Union[float, int, Any]:
    """
    Checks the dictionary returned from a row of an endpoint to deduce when the object was last created/modified and save it to dlt state.
    :param chosen_col: The column which the user wants to save into dlt state as the last load time
    :param default_col: The column in which this value is usually expected, to be used if the user provided column doesn't exist
    :param object_dict: The dict containing a row of data from an endpoint
    :return return_timestamp: The timestamp of the date-type object in the dict
    """
    if chosen_col in object_dict:
        parsed_date = pendulum.parse(object_dict[chosen_col])
        if isinstance(parsed_date, pendulum.DateTime):
            return_timestamp = parsed_date.timestamp()
        else:
            raise ValueError(f"Column passed to save the last load time: {chosen_col} doesn't contain a format which can be converted into a DateTime by the pendulum parser!")
    else:
        parsed_date = pendulum.parse(object_dict[default_col])
        if isinstance(parsed_date, pendulum.DateTime):
            return_timestamp = parsed_date.timestamp()
        else:
            raise ValueError(f"Error! {default_col} doesn't contain a format which can be converted into a DateTime by the pendulum parser!")
    return return_timestamp

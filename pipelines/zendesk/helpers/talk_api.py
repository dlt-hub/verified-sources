"""
This module contains everything related to the API client class made to make requests specifically to ZendeskTalk
"""

from time import sleep
from typing import Dict, Iterator, Optional, Tuple, Union
from dlt.common import logger
from dlt.common.typing import DictStrStr, TDataItems, TSecretValue
from dlt.sources.helpers.requests import client
from .credentials import ZendeskCredentialsEmailPass, ZendeskCredentialsOAuth, ZendeskCredentialsToken

TALK_ENDPOINTS = {
    "calls": "/api/v2/channels/voice/calls",
    "addresses": "/api/v2/channels/voice/addresses",
    "greeting_categories": "/api/v2/channels/voice/greeting_categories",
    "greetings": "/api/v2/channels/voice/greetings",
    "ivrs": "/api/v2/channels/voice/ivr",
    "phone_numbers": "/api/v2/channels/voice/phone_numbers",
    "settings": "/api/v2/channels/voice/settings",
    "lines": "/api/v2/channels/voice/lines",
    "agents_activity": "/api/v2/channels/voice/stats/agents_activity",
    "current_queue_activity": "/api/v2/channels/voice/stats/current_queue_activity"
}
INCREMENTAL_ENDPOINTS = {
    "calls": "/api/v2/channels/voice/stats/incremental/calls.json",
    "legs": "/api/v2/channels/voice/stats/incremental/legs.json"
}

possible_endpoints = {
    "availabilities": "/api/v2/channels/voice/availabilities",
    "recordings": "/api/v2/channels/voice/calls/{call_id}/recordings",
    "digital_lines": "/api/v2/channels/voice/digital_lines",
    "agents_overview": "/api/v2/channels/voice/stats/agents_overview",
    "account_overview": "/api/v2/channels/voice/stats/account_overview",
}

class ZendeskAPIClient:
    """
    API client used to make requests to Zendesk TALK.
    """
    subdomain: str = ""
    url: str = ""
    headers: Optional[DictStrStr]
    auth: Optional[Tuple[str, TSecretValue]]

    def __init__(self, credentials: Union[ZendeskCredentialsOAuth, ZendeskCredentialsToken, ZendeskCredentialsEmailPass]) -> None:
        """
        Initializer for the API client which is then used to make API calls to the ZendeskAPI
        @:param credentials: ZendeskCredentials object which contains the necessary credentials to authenticate to ZendeskAPI
        @:returns: Nothing since it just initializes API parameters
        """
        # oauth token is the preferred way to authenticate, followed by api token and then email + password combo
        # fill headers and auth for every possibility of credentials given, raise error if credentials are of incorrect type
        if isinstance(credentials, ZendeskCredentialsOAuth):
            self.headers = {"Authorization": f"Bearer {credentials.oauth_token}"}
            self.auth = None
        elif isinstance(credentials, ZendeskCredentialsToken):
            self.headers = None
            self.auth = (f"{credentials.email}/token", credentials.token)
        elif isinstance(credentials, ZendeskCredentialsEmailPass):
            self.auth = (credentials.email, credentials.password)
            self.headers = None
        else:
            raise TypeError("Wrong credentials type provided to ZendeskAPIClient. The credentials need to be of type: ZendeskCredentialsOAuth, ZendeskCredentialsToken or ZendeskCredentialsEmailPass")

        # set subdomain, this is always needed to configure endpoints
        self.subdomain = credentials.subdomain
        self.url = f"https://{self.subdomain}.zendesk.com"

    def make_request(self, endpoint: str, data_point_name: str, params: Optional[Dict[str, int]] = None) -> Iterator[TDataItems]:
        """
        Makes a get request on a given endpoint.
        @:param endpoint: the url to the endpoint, i.e. api/v2/calls
        @:param data_point_name: name of the endpoint, i.e. calls
        @:returns: A generator of json responses
        """
        # make request and keep looping until there is no next page
        get_url = f"{self.url}{endpoint}"
        while get_url:
            response = client.get(get_url, headers=self.headers, auth=self.auth, params=params)
            response.raise_for_status()
            response_json = response.json()
            get_url = response_json.get("next_page", None)
            yield response_json[data_point_name]

    def make_request_incremental(self, endpoint: str, data_point_name: str, start_date: int) -> Iterator[TDataItems]:
        """
        Makes a request to an incremental API endpoint
        @:param endpoint: the url to the endpoint, i.e. api/v2/calls
        @:param data_point_name: name of the endpoint, i.e. calls
        @:param start_date: a timestamp of the starting date, i.e. a date in unix epoch time (the number of seconds since January 1, 1970, 00:00:00 UTC)
        @:returns: A generator of json responses
        """
        # start date comes as unix epoch float, need to convert to an integer to make the call to the API
        params = {"start_time": start_date}
        yield from self.make_request(endpoint=endpoint, data_point_name=data_point_name, params=params)

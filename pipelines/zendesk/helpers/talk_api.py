"""
This module contains everything related to the API client class made to make requests specifically to ZendeskTalk
"""
from pipelines.zendesk.helpers.credentials import ZendeskCredentialsToken, ZendeskCredentialsOAuth, ZendeskCredentialsEmailPass
from dlt.common import logger
from dlt.common.typing import DictStrStr, TSecretValue, TDataItems
from typing import Union, Tuple, Iterator
import requests


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
possible_endpoints = {
    "incremental_calls": "/api/v2/channels/voice/stats/incremental/calls.json",
    "incremental_legs": "/api/v2/channels/voice/stats/incremental/legs.json",
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
    headers: DictStrStr = {}
    auth: Tuple[str, TSecretValue]

    def __init__(self, credentials: Union[ZendeskCredentialsOAuth, ZendeskCredentialsToken, ZendeskCredentialsEmailPass]) -> None:
        """
        Initializer for the API client which is then used to make API calls to the ZendeskAPI
        @:param credentials: ZendeskCredentials object which contains the necessary credentials to authenticate to ZendeskAPI
        @:returns: Nothing since it just initializes API parameters
        """
        # set subdomain, this is always needed to configure endpoints
        self.subdomain = credentials.subdomain
        self.url = f"https://{self.subdomain}.zendesk.com"

        # oauth token is the preferred way to authenticate, followed by api token and then email + password combo
        # if none of these are filled, simply return an error
        if isinstance(credentials, ZendeskCredentialsOAuth):
            self.headers["Authorization"] = f"Bearer {credentials.oauth_token}"
        elif isinstance(credentials, ZendeskCredentialsToken):
            self.headers["Authorizations"] = f"Bearer {credentials.token}"
        elif isinstance(credentials, ZendeskCredentialsEmailPass):
            self.auth = (credentials.email, credentials.password)

    def make_request(self, endpoint: str, data_point_name: str) -> Iterator[TDataItems]:
        """
        Makes a get request on a given endpoint.
        @:param endpoint: String that specifies the exact endpoint data is being received from
        @:return response_json:
        """

        # TODO: handle hitting the rate limit
        # TODO: caching
        # TODO: side loading
        # TODO: handle incremental load
        # make request and keep looping until there is no next page
        get_url = f"{self.url}{endpoint}"
        while get_url:
            response = requests.get(get_url, headers=self.headers, auth=self.auth)
            if response.status_code == 200:
                response_json = response.json()
                get_url = response_json.get("next_page", None)
                yield response_json[data_point_name]
            else:
                # retry or print for failure
                logger.warning(f"API call failed on endpoint {endpoint} with error code {response.status_code}")

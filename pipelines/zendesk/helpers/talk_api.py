from api_helpers import ZendeskCredentials
import dlt
from dlt.common import logger
from dlt.common.typing import DictStrStr
import requests



talk_endpoints = {
    "calls": "/api/v2/channels/voice/calls",
    "incremental_calls": "channels/voice/stats/incremental/calls.json",
    "incremental_legs": "channels/voice/stats/incremental/legs.json",
    "availabilities": "/api/v2/channels/voice/availabilities",
    "addresses": "/api/v2/channels/voice/addresses",
    "availabilities": "/api/v2/channels/voice/availabilities",
    "recordings": "/api/v2/channels/voice/calls/{call_id}/recordings",
    "digital_lines": "/api/v2/channels/voice/digital_lines",
    "greeting_categories": "/api/v2/channels/voice/greeting_categories",
    "greetings": "/api/v2/channels/voice/greetings",
    "ivr": "/api/v2/channels/voice/ivr",
    "phone_numbers": "api/v2/channels/voice/phone_numbers",
    "settings": "/api/v2/channels/voice/settings",
    "agents_overview": "/api/v2/channels/voice/stats/agents_overview",
    "account_overview": "/api/v2/channels/voice/stats/account_overview",
    "lines": "/api/v2/channels/voice/lines",
    "agents_activity": "channels/voice/stats/agents_activity",
    "current_queue_activity": "channels/voice/stats/current_queue_activity",
    "tickets": "/api/v2/tickets"
}


class ZendeskAPIClient:
    """
    API client used to make requests to Zendesk TALK
    """
    subdomain: str = ""
    url: str = ""
    headers: DictStrStr = {}
    auth: tuple[str] = ""

    def __init__(self, credentials: ZendeskCredentials) -> None:
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
        if credentials.oauth_token:
            self.headers["Authorization"] = f"Bearer {credentials.oauth_token}"
        elif credentials.token and credentials.email:
            self.headers["Authorizations"] = f"Bearer {credentials.token}"
        elif credentials.email and credentials.password:
            self.auth = (credentials.email, credentials.password)
        else:
            logger.warning("Error when trying to load credentials into ZendeskAPIClient")
            raise ValueError("Incorrect credentials!")

    def make_request(self, endpoint: str) -> str:
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
                get_url = response_json["next_page"]
                yield response_json
            else:
                # retry or print for failure
                logger.warning(f"API call failed on endpoint {endpoint} with error code {response.status_code}")
                print(f"API call failed on endpoint {endpoint} with error code {response.status_code}")

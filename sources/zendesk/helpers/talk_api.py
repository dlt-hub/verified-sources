"""
This module contains everything related to the API client class made to make requests specifically to ZendeskTalk
"""

from time import sleep
from typing import Dict, Iterator, Optional, Tuple, Union, Any
from dlt.common import pendulum
from dlt.common import logger
from dlt.common.typing import DictStrStr, TDataItems, TSecretValue
from dlt.sources.helpers.requests import client
from .credentials import (
    ZendeskCredentialsEmailPass,
    ZendeskCredentialsOAuth,
    ZendeskCredentialsToken,
    TZendeskCredentials,
)
from .api_helpers import parse_iso_or_timestamp


class ZendeskAPIClient:
    """
    API client used to make requests to Zendesk talk, support and chat API
    """

    subdomain: str = ""
    url: str = ""
    headers: Optional[DictStrStr]
    auth: Optional[Tuple[str, TSecretValue]]

    def __init__(
        self, credentials: TZendeskCredentials, url_prefix: Optional[str] = None
    ) -> None:
        """
        Initializer for the API client which is then used to make API calls to the ZendeskAPI

        Args:
            credentials: ZendeskCredentials object which contains the necessary credentials to authenticate to ZendeskAPI
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
            raise TypeError(
                "Wrong credentials type provided to ZendeskAPIClient. The credentials need to be of type: ZendeskCredentialsOAuth, ZendeskCredentialsToken or ZendeskCredentialsEmailPass"
            )

        # If url_prefix is set it overrides the default API URL (e.g. chat api uses zopim.com domain)
        if url_prefix:
            self.url = url_prefix
        else:
            self.subdomain = credentials.subdomain
            self.url = f"https://{self.subdomain}.zendesk.com"

    def get_pages(
        self,
        endpoint: str,
        data_point_name: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> Iterator[TDataItems]:
        """
        Makes a request to a paginated endpoint and returns a generator of data items per page.

        Args:
            endpoint: The url to the endpoint, e.g. /api/v2/calls
            data_point_name: The key which data items are nested under in the response object (e.g. calls)
            params: Optional dict of query params to include in the request

        Returns:
            Generator of pages, each page is a list of dict data items
        """
        # make request and keep looping until there is no next page
        get_url = f"{self.url}{endpoint}"
        has_more_pages = True
        while has_more_pages:
            response = client.get(
                get_url, headers=self.headers, auth=self.auth, params=params
            )
            response.raise_for_status()
            response_json = response.json()
            result = response_json[data_point_name]
            yield result
            get_url = response_json.get("next_page", None)
            # Get URL includes params
            params = {}
            # Ticket API always returns next page URL resulting in infinite loop
            # `end_of_stream` property signals there are no more pages.
            # See https://developer.zendesk.com/api-reference/ticketing/ticket-management/incremental_exports/#json-format
            end_of_stream = response_json.get("end_of_stream", False)
            has_more_pages = bool(get_url and result) and not end_of_stream

    def get_pages_incremental(
        self,
        endpoint: str,
        data_point_name: str,
        start_time: pendulum.DateTime,
        until: Optional[pendulum.DateTime] = None,
        timestamp_field: Optional[str] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> Iterator[TDataItems]:
        """
        Makes a request to an incremental API endpoint

        Args:
            endpoint: The url to the endpoint, e.g. /api/v2/calls
            data_point_name: The key which data items are nested under in the response object (e.g. calls)
            start_time: a timestamp of the starting date, i.e. a date in unix epoch time (the number of seconds since January 1, 1970, 00:00:00 UTC)
            params: Optional dict of query params to include in the request

        Returns:
            Generator of pages, each page is a list of dict data items
        """
        if until and timestamp_field:
            yield from self._get_pages_until(
                endpoint,
                data_point_name,
                timestamp_field,
                until,
                start_time,
                params=params,
            )
            return
        # start date comes as unix epoch float, need to convert to an integer to make the call to the API
        params = params or {}
        params["start_time"] = str(start_time.int_timestamp)
        yield from self.get_pages(
            endpoint=endpoint, data_point_name=data_point_name, params=params
        )

    def _get_pages_until(
        self,
        endpoint: str,
        data_point_name: str,
        timestamp_field: str,
        until: pendulum.DateTime,
        start_time: Optional[pendulum.DateTime],
        params: Optional[Dict[str, Any]] = None,
    ) -> Iterator[TDataItems]:
        params = params or {}
        params["start_time"] = str(start_time.int_timestamp)

        def _page_filtered(page: TDataItems) -> TDataItems:
            for item in page:
                ts = parse_iso_or_timestamp(item[timestamp_field])
                if ts >= until:
                    return
                yield item

        for page in self.get_pages(
            endpoint=endpoint, data_point_name=data_point_name, params=params
        ):
            page = list(_page_filtered(page))
            if not page:
                return
            yield page

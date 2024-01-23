from enum import Enum
from typing import Dict, Iterator, Optional, Tuple, Any
from dlt.common.typing import TDataItems

from api_client import APIClient, BearerTokenAuth, JSONResponsePaginator

from .paginators import CursorPaginator, StreamPaginator, StartTimePaginator

from .credentials import (
    ZendeskCredentialsEmailPass,
    ZendeskCredentialsOAuth,
    ZendeskCredentialsToken,
    TZendeskCredentials,
)


class PaginationType(Enum):
    OFFSET = 0
    CURSOR = 1
    STREAM = 2
    START_TIME = 3


class ZendeskAPIClient(APIClient):
    """
    API client used to make requests to Zendesk talk, support and chat API
    """

    def __init__(
        self, credentials: TZendeskCredentials, url_prefix: Optional[str] = None
    ) -> None:
        """
        Initializer for the API client which is then used to make API calls to the ZendeskAPI

        Args:
            credentials: ZendeskCredentials object which contains the necessary credentials to authenticate to ZendeskAPI
        """
        self.subdomain = credentials.subdomain
        base_url = f"https://{self.subdomain}.zendesk.com"

        # # If url_prefix is set it overrides the default API URL (e.g. chat api uses zopim.com domain)
        if url_prefix:
            base_url = url_prefix

        # Setting up authentication
        if isinstance(credentials, ZendeskCredentialsOAuth):
            auth = BearerTokenAuth(credentials.oauth_token)
        elif isinstance(credentials, ZendeskCredentialsToken):
            auth = (f"{credentials.email}/token", credentials.token)
        elif isinstance(credentials, ZendeskCredentialsEmailPass):
            auth = (credentials.email, credentials.password)
        else:
            raise TypeError("Incorrect credentials type provided to ZendeskAPIClient.")

        super().__init__(base_url=base_url, auth=auth)


    def get_pages(
        self,
        endpoint: str,
        data_point_name: str,
        pagination: PaginationType,
        params: Optional[Dict[str, Any]] = None,
    ) -> Iterator[TDataItems]:
        """
        Makes a request to a paginated endpoint and returns a generator of data items per page.

        Args:
            endpoint: The url to the endpoint, e.g. /api/v2/calls
            data_point_name: The key which data items are nested under in the response object (e.g. calls)
            params: Optional dict of query params to include in the request
            pagination: Type of pagination type used by endpoint

        Returns:
            Generator of pages, each page is a list of dict data items
        """
        params = params or {}
        paginator = None

        if pagination == PaginationType.CURSOR:
            paginator = CursorPaginator(records_key=data_point_name)
        elif pagination == PaginationType.OFFSET:
            paginator = JSONResponsePaginator(
                next_key="next_page", records_key=data_point_name
            )
        elif pagination == PaginationType.STREAM:
            paginator = StreamPaginator(records_key=data_point_name)
        elif pagination == PaginationType.START_TIME:
            paginator = StartTimePaginator(records_key=data_point_name)
        else:
            raise ValueError(f"Invalid pagination type: {pagination}")

        for page in self.paginate(endpoint, params=params, paginator=paginator):
            yield page
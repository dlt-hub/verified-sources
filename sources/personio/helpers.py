"""Personio source helpers"""
from typing import Any, Iterable, Optional
from urllib.parse import urljoin

from dlt.common.typing import Dict, TDataItems
from dlt.sources.helpers import requests

from ..api_client import APIClient, BearerTokenAuth
from .paginator import Paginator


class PersonioAPI(APIClient):
    """A Personio API client."""

    def __init__(self, base_url: str, client_id: str, client_secret: str) -> None:
        """
        Args:
            client_id: The client ID of your app.
            client_secret: The client secret of your app.
        """
        self.access_token = self.get_token(base_url, client_id, client_secret)
        super().__init__(base_url, auth=BearerTokenAuth(self.access_token))

    def get_token(self, base_url: str, client_id: str, client_secret: str) -> str:
        """Get an access token from Personio.

        Returns:
            The access token.
        """
        url = urljoin(base_url, "auth")
        response = requests.post(
            url, json={"client_id": client_id, "client_secret": client_secret}
        )
        return response.json()["data"]["token"]

    def get_pages(
        self,
        resource: str,
        params: Optional[Dict[str, Any]] = None,
        offset_by_page: bool = False,
    ) -> Iterable[TDataItems]:
        """Get all pages from Personio using requests.

        Args:
            resource: The resource to get pages for (e.g. employees, absences, attendances).
            params: The parameters for the resource.
            offset_by_page (bool): If True, offset increases by 1 per page; else, increases by page_size.

        Yields:
            List of data items from the page
        """
        params = params or {}
        for page_content in self.paginate(
            path=resource,
            params=params,
            paginator=Paginator(offset_by_page=offset_by_page),
        ):
            yield page_content

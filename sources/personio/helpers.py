"""Personio source helpers"""
from typing import Any, Iterable, Optional
from urllib.parse import urljoin

import backoff
import inflection
from dlt.common.time import ensure_pendulum_datetime
from dlt.common.typing import Dict, TDataItems
from dlt.sources.helpers import requests
from requests.exceptions import RequestException


def backoff_giveup(ex: RequestException) -> bool:
    status_code = ex.response.status_code
    if status_code >= 500 or status_code in [429]:
        return False
    return True


class PersonioApi:
    """A Personio API client."""

    base_url = "https://api.personio.de/v1/"

    def __init__(self, client_id: str, client_secret: str) -> None:
        """
        Args:
            client_id: The client ID of your app.
            client_secret: The client secret of your app.
        """
        self.client_id = client_id
        self.client_secret = client_secret
        self.access_token = self.get_token()

    @backoff.on_exception(
        backoff.expo,
        RequestException,
        max_tries=5,
        giveup=backoff_giveup,  # type: ignore
    )
    def _request(
        self,
        method: str,
        resource: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, Any]] = None,
        json: Optional[Dict[str, Any]] = None,
    ) -> Any:
        """Make a request to Personio.

        Args:
            method: The HTTP method to use.
            resource: The resource to get pages for (e.g. employees, absences, attendances).
            params: Query params to include in the request.
            headers: Headers to include in the request.

        Returns:
            The response data.
        """
        url = urljoin(self.base_url, resource)
        headers = headers or {}
        response = requests.request(
            method, url, params=params, headers=headers, json=json
        )
        response.raise_for_status()
        return response.json()

    def get_token(self) -> str:
        """Get an access token from Personio.

        Returns:
            The access token.
        """
        headers = {"Content-Type": "application/json", "Accept": "application/json"}
        data = {"client_id": self.client_id, "client_secret": self.client_secret}
        response = self._request("POST", "auth", headers=headers, json=data)
        token: str = response["data"]["token"]
        return token

    def get_pages(
        self,
        resource: str,
        params: Dict[str, Any] = {},
        page_size: int = 200,
        convert_response: bool = True,
    ) -> Iterable[TDataItems]:
        """Get all pages from Personio using requests.

        Args:
            resource: The resource to get pages for (e.g. employees, absences, attendances).
            params: The parameters for the resource.
            page_size: The max number of items to fetch per page. Defaults to 200.

        Yields:
            List of data items from the page
        """
        headers = {"Authorization": f"Bearer {self.access_token}"}
        params.update({"limit": page_size, "offset": 0})
        while True:
            response = self._request("GET", resource, headers=headers, params=params)
            # Get item list from the page
            if convert_response:
                yield [self._convert_response(item) for item in response["data"]]
            else:
                yield [item.get("attributes", {}) for item in response["data"]]
            metadata = response.get("metadata")
            if not metadata:
                break

            current_page = metadata.get("current_page")
            total_pages = metadata.get("total_pages")

            if current_page >= total_pages or not response["data"]:
                break
            params["offset"] += page_size

    def _convert_response(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """Convert personio response for a cleaned data item and handle datetime fields
        Args:
            item: The item to convert

        Returns:
            The same data item (for convenience)
        """
        attributes = item.get("attributes", {})
        output = {}
        for value in attributes.values():
            name = value["universal_id"]
            if not name:
                label = value["label"].replace(" ", "_")
                name = inflection.underscore(label)

            if value["type"] == "date" and value["value"]:
                output[name] = ensure_pendulum_datetime(value["value"])
            else:
                output[name] = value["value"]
        return output

import logging
import time
from typing import Any, Dict, Iterable, Optional

import pendulum
from dlt.common.typing import TDataItem
from dlt.sources.helpers import requests


class WorkableClient:
    def __init__(
        self,
        access_token: str,
        subdomain: str,
        start_date: Optional[pendulum.DateTime] = None,
    ):
        self.access_token = access_token
        self.subdomain = subdomain
        # Authorization headers. Content-Type is not necessary,
        # but should workable start providing alternate
        # content types such as XML this won't break
        self.headers = {
            "accept": "application/json",
            "Authorization": "Bearer " + access_token,
        }
        # Base URL Endpoint for all API requests
        self.base_url = f"https://{subdomain}.workable.com/spi/v3"
        # Increase the default limit for downloading lists from 50 to 100,
        # so we need to make fewer requests
        self.default_limit = 100
        self.params = {"limit": self.default_limit}
        # Initialize an ISO-formatted start date.
        # If the `start_date` argument is not provided, it defaults to January 1, 2000.
        self.default_start_date = pendulum.datetime(2000, 1, 1).isoformat()
        self.start_date_iso = (
            start_date.isoformat()
            if start_date is not None
            else self.default_start_date
        )

    def _request_with_rate_limit(self, url: str, **kwargs: Any) -> requests.Response:
        """
        Handling rate limits in HTTP requests and ensuring
        that the client doesn't exceed the limit set by the server
        """
        while True:
            try:
                response = requests.get(url, **kwargs)
                return response
            except requests.HTTPError as e:
                logging.warning("Rate limited. Waiting to retry...")
                seconds_to_wait = (
                    int(e.response.headers["X-Rate-Limit-Reset"])
                    - pendulum.now().timestamp()
                )
                time.sleep(seconds_to_wait)

    def pagination(
        self,
        endpoint: str,
        custom_url: Optional[str] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> Iterable[TDataItem]:
        """
        Queries an API endpoint using pagination and returns the results as a generator.
        Args:
            endpoint (str): The API endpoint to query.
            custom_url (str, optional): A custom URL to use instead of the default base URL.
            params (dict, optional): A dictionary of query parameters to include in the API request.

        """
        base_url = self.base_url if custom_url is None else custom_url
        url = f"{base_url}/{endpoint}"

        if params is not None:
            self.params.update(params)

        has_more = True
        while has_more:
            response = self._request_with_rate_limit(
                url, headers=self.headers, params=self.params
            )
            response_json = response.json()
            paging = response_json.get("paging")
            if paging is not None:
                url = paging["next"]
            else:
                has_more = False

            yield response_json.get(endpoint)

    def details_from_endpoint(
        self, main_endpoint: str, code: str, dependent_endpoint: str
    ) -> Iterable[TDataItem]:
        custom_url = f"{self.base_url}/{main_endpoint}/{code}"
        return self.pagination(dependent_endpoint, custom_url=custom_url)

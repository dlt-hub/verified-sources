"""Freshdesk source helpers"""

import logging
import time
from typing import Any, Iterable, Optional

import pendulum
from dlt.common.typing import TDataItem
from dlt.sources.helpers import requests


class FreshdeskClient:
    """
    Client for making authenticated requests to the Freshdesk API. It incorporates API requests with
    rate limit and pagination.

    Attributes:
        api_key (str): The API key used for authenticating requests to the Freshdesk API.
        domain (str): The Freshdesk domain specific to the user, used in constructing the base URL.
        base_url (str): The base URL constructed from the domain, targeting the Freshdesk API v2.
    """
    def __init__(self, api_key: str, domain: str):
        # Initialize the FreshdeskClient instance with API key and domain.
        # The API key is used for authentication with the Freshdesk API.
        # The domain specifies the unique Freshdesk domain of the user.

        # Store the API key provided during initialization.
        self.api_key = api_key
        # Store the Freshdesk domain provided during initialization.
        self.domain = domain

        # Construct the base URL for the API requests.
        # This URL is formed by appending the domain to the standard Freshdesk API base URL format.
        # All API requests will use this base URL as their starting point.
        self.base_url = f"https://{domain}.freshdesk.com/api/v2"

    def _request_with_rate_limit(self, url: str, **kwargs: Any) -> requests.Response:
        """
        Handles rate limits in HTTP requests and ensures
        that the client doesn't exceed the limit set by the server.
        """
        while True:
            try:
                response = requests.get(url, **kwargs, auth=(self.api_key, "X"))
                response.raise_for_status()
                return response
            except requests.HTTPError as e:
                if e.response.status_code == 429:
                    logging.warning("Rate limited. Waiting to retry...")
                    seconds_to_wait = (
                        int(e.response.headers["X-Rate-Limit-Reset"])
                        - pendulum.now().timestamp()
                    )
                    time.sleep(seconds_to_wait)
                else:
                    raise

    def paginated_response(
        self,
        endpoint: str,
        page: int = 1,
        per_page: int = 100,
        updated_at: Optional[Any] = None,
    ) -> Iterable[TDataItem]:
        """
        Retrieves data from an endpoint with pagination.

        Args:
            endpoint (str): The endpoint to retrieve data from (e.g., 'tickets', 'contacts').
            page (int): The starting page number for the API request.
            per_page (int): The number of items requested per page.
            updated_at (Optional[Any]): An optional 'updated_at' to limit the data retrieved.
                                        Defaults to None.

        Yields:
            Iterable[TDataItem]: Data items retrieved from the endpoint.

        """
        while True:
            # Construct the URL for the specific endpoint
            url = f"{self.base_url}/{endpoint}"

            params = {"per_page": per_page, "page": page}

            # Adjust parameters based on the endpoint
            if endpoint in ["tickets", "contacts"]:
                param_key = (
                    "updated_since" if endpoint == "tickets" else "_updated_since"
                )
                if updated_at:
                    params[param_key] = updated_at

            # To handle requests, use the method provided by the class,
            # which includes rate-limiting.
            response = self._request_with_rate_limit(url, params=params)

            data = response.json()

            if not data:
                break

            yield data

            # Assuming the API does not return a full page of data if it's the last page
            if len(data) < per_page:
                break  # Last page reached

            page += 1  # Prepare to fetch the next page

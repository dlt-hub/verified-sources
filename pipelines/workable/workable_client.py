import logging
import time
from typing import Optional

import pendulum
import requests


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
        # content types such as XML, this won't break
        self.headers = {
            "accept": "application/json",
            "Authorization": "Bearer " + access_token,
        }
        # Base URL Endpoint for all API requests
        self.base_url = f"https://{subdomain}.workable.com/spi/v3"
        # Increase default limit for downloading lists from 50 to 100,
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

    def _request_with_rate_limit(self, url, **kwargs):
        """
        Handling rate limits in HTTP requests and ensuring
        that the client doesn't exceed the limit set by the server
        """
        response = requests.get(url, **kwargs)
        if response.status_code == 429:
            logging.warning("Rate limited. Waiting to retry...")
            seconds_to_wait = (
                int(response.headers["X-Rate-Limit-Reset"]) - pendulum.now().timestamp()
            )
            time.sleep(seconds_to_wait)
            return self._request_with_rate_limit(url, **kwargs)
        return response

    def pagination(
        self,
        endpoint: str,
        custom_url: Optional[str] = None,
        params: Optional[dict] = None,
    ) -> list:
        """
        Queries an API endpoint using pagination and returns the results as a generator.
        Parameters:
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

    def _get_dependent_endpoint(self, code: str, from_endpoint: str, to_endpoint: str):
        custom_url = f"{self.base_url}/{from_endpoint}/{code}"
        return self.pagination(to_endpoint, custom_url=custom_url)

    def from_jobs_with_shortcode(self, shortcode: str, endpoint: str):
        yield self._get_dependent_endpoint(shortcode, "jobs", endpoint)

    def from_candidates_with_id(self, id: str, endpoint: str):
        yield self._get_dependent_endpoint(id, "candidates", endpoint)

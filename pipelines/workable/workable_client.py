import logging
import time
from typing import Optional

import pendulum
import requests


class WorkableClient:
    def __init__(self, access_token: str, subdomain: str, start_date=None):
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
        self.default_start_date = pendulum.datetime(2000, 1, 1).isoformat()
        self.start_date_iso = (
            start_date.isoformat()
            if start_date is not None
            else self.default_start_date
        )
        self.params = {"limit": self.default_limit}

    def _request_with_rate_limit(self, url, **kwargs):
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
        self, endpoint: str, custom_url: str = None, params: Optional[dict] = None
    ) -> list:
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

            yield response_json[endpoint]

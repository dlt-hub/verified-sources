"""This module contains an implementation of a Matomo API client for python."""
from typing import Iterator, List
from dlt.common.typing import DictStrAny, TDataItems
from dlt.sources.helpers.requests import client


class MatomoAPIClient:
    """
    API client used to make requests to Matomo API.
    """

    def __init__(self, base_url: str, auth_token: str) -> None:
        """
        Initializes the client which is then used to make api requests.
        :param auth_token: Token that provides access to the api.
        :param base_url: The url of the domain which is being analyzed.
        """

        self.base_url = base_url
        self.auth_token = auth_token

    def _request(self, params: dict):
        """
        Helper that retrieves the data and returns the json response
        :param params:
        :return:
        """

        # loop through all the pages
        # the total number of rows is received after the first request, for the first request to be sent through, initializing the row_count to 1 would suffice
        headers = {'Content-type': 'application/json'}
        url = f"{self.base_url}/index.php"
        response = client.get(url=url, headers=headers, params=params)
        response.raise_for_status()
        json_response = response.json()
        yield json_response

    def get_query(self, date: str, extra_params: DictStrAny, methods: List[str], period: str, site_id: int):
        """

        :param date:
        :param extra_params:
        :param methods:
        :param period:
        :param site_id:
        :return:
        """
        # Set up the API URL and parameters
        if extra_params is None:
            extra_parameters = {}
        params = {
            "module": "API",
            "method": "API.getBulkRequest",
            "format": "json",
            "token_auth": self.auth_token,
        }
        for i, method in enumerate(methods):
            params[f"urls[{i}]"] = f"method={method}&idSite={site_id}&period={period}&date={date}"
        # Merge the additional parameters into the request parameters
        params.update(extra_params)
        # Send the API request
        yield from self._request(params=params)


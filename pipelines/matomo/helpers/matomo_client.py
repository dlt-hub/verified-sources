"""This module contains an implementation of a Matomo API client for python."""
from typing import List, Union
from dlt.common.typing import DictStrAny, DictStrStr, TDataItem
from dlt.sources.helpers.requests import client


class MatomoAPIClient:
    """
    API client used to make requests to Matomo API.
    """

    def __init__(self, credentials: DictStrStr) -> None:
        """
        Initializes the client which is then used to make api requests.
        :param credentials: Contains a token and a base url used to authenticate to the Matomo API
        """

        self.base_url = credentials["url"]
        self.auth_token = credentials["api_token"]

    def _request(self, params: DictStrAny) -> TDataItem:
        """
        Helper that retrieves the data and returns the json response from the API
        :param params:
        :returns: Json returned from API
        """

        # loop through all the pages
        # the total number of rows is received after the first request, for the first request to be sent through, initializing the row_count to 1 would suffice
        headers = {'Content-type': 'application/json'}
        url = f"{self.base_url}/index.php"
        response = client.get(url=url, headers=headers, params=params)
        response.raise_for_status()
        json_response = response.json()
        return json_response

    def get_query(self, date: str, extra_params: DictStrAny, methods: List[str], period: str, site_id: int) -> TDataItem:
        """
        Helper that gets data in a batch from Matomo.
        :param date: Can be a single date or a date range in the form start_date,end_date
        :param extra_params: Extra parameters as a dict
        :param methods: List of methods we want data for
        :param period: Period can be day, month, year
        :param site_id: Unique id of the Matomo site
        :returns: JSON data from the response.
        """
        # Set up the API URL and parameters
        if not extra_params:
            extra_params = {}
        params = {
            "module": "API",
            "method": "API.getBulkRequest",
            "format": "json",
            "token_auth": self.auth_token
        }
        for i, method in enumerate(methods):
            params[f"urls[{i}]"] = f"method={method}&idSite={site_id}&period={period}&date={date}"
        # Merge the additional parameters into the request parameters
        params.update(extra_params)
        # Send the API request
        return self._request(params=params)

    def get_method(self, extra_params: DictStrAny, method: str, site_id: int) -> TDataItem:
        """
        Helper that gets data using a Matomo API method
        :param extra_params: Extra parameters as a dict
        :param method: Unique report from Matomo API
        :param site_id: Unique id of the Matomo site
        :returns: JSON data from the response.
        """

        # Set up the API URL and parameters
        if not extra_params:
            extra_params = {}
        params = {
            "module": "API",
            "idSite": site_id,
            "method": method,
            "format": "json",
            "token_auth": self.auth_token,
        }
        # Merge the additional parameters into the request parameters
        params.update(extra_params)
        # Send the API request
        return self._request(params=params)


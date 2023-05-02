"""This module contains an implementation of a Matomo API client for python."""
from typing import List, Union
from dlt.common.typing import DictStrAny, DictStrStr, TDataItem
from dlt.sources.helpers.requests import client


class MatomoAPIClient:
    """
    API client used to make requests to Matomo API.
    """

    def __init__(self, api_token: str, url: str) -> None:
        """
        Initializes the client which is then used to make api requests.
        :param api_token: Token used to authenticate for Matomo API
        :param url: Url of the website
        """

        self.base_url = url
        self.auth_token = api_token

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

    def get_method(self, extra_params: DictStrAny, method: str, site_id: int, rows_per_page: int = 10000) -> TDataItem:
        """
        Helper that gets data using a Matomo API method
        :param extra_params: Extra parameters as a dict
        :param method: Unique report from Matomo API
        :param site_id: Unique id of the Matomo site
        :param rows_per_page: How many rows are returned per page from the request.
        :returns: JSON data from the response.
        """

        # Set up the API URL and parameters
        if not extra_params:
            extra_params = {}
        filter_offset = 0
        params = {
            "module": "API",
            "method": "API.getBulkRequest",
            "format": "json",
            "token_auth": self.auth_token,
            "urls[0]": f"method={method}&idSite={site_id}&filter_limit={rows_per_page}&filter_offset={filter_offset}"
        }
        # Merge the additional parameters into the request parameters
        params.update(extra_params)
        # Send the API request
        method_data = self._request(params=params)[0]
        while len(method_data):
            yield method_data
            filter_offset += len(method_data)
            params["urls[0]"] = f"method={method}&idSite={site_id}&filter_limit={rows_per_page}&filter_offset={filter_offset}"
            method_data = self._request(params=params)[0]

    def get_visitors_batch(self, visitor_list: List[str], site_id: int, extra_params: DictStrAny = None) -> TDataItem:
        """
        Gets visitors for Matomo.
        :param visitor_list:
        :param site_id:
        :param extra_params:
        :return:
        """
        if not extra_params:
            extra_params = {}
        params = {
            "module": "API",
            "method": "API.getBulkRequest",
            "format": "json",
            "site_id": site_id,
            "token_auth": self.auth_token
        }
        params.update({f"urls[{i}]": f"method=Live.getVisitorProfile&idSite={site_id}&visitorId={visitor_list[i]}" for i in range(len(visitor_list))})
        params.update(extra_params)
        method_data = self._request(params=params)
        return method_data


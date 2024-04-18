"""This module contains an implementation of a Matomo API client for python."""
from typing import Iterator, List, Union
from dlt.common.typing import DictStrAny, DictStrStr, TDataItem, TDataItems
from dlt.sources.helpers.requests import client


class MatomoResponseError(Exception):
    """Raised when matomo response contains an error"""

    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(f"Matomo error response: {self.message}")


class MatomoAPIClient:
    """
    API client used to make requests to Matomo API.
    """

    def __init__(self, api_token: str, url: str) -> None:
        """
        Initializes the client.

        Args:
            api_token (str): Token used to authenticate for Matomo API.
            url (str): URL of the Matomo website.
        """

        self.base_url = url
        self.auth_token = api_token

    def _request(self, params: DictStrAny) -> TDataItem:
        """
        Helper method that retrieves data and returns the JSON response from the API.

        Args:
            params (DictStrAny): Parameters for the API request.

        Returns:
            TDataItem: JSON response from the API.
        """

        # loop through all the pages
        # the total number of rows is received after the first request, for the first request to be sent through, initializing the row_count to 1 would suffice
        headers = {"Content-type": "application/json"}
        url = f"{self.base_url}/index.php"
        response = client.get(url=url, headers=headers, params=params)
        response.raise_for_status()
        json_response = response.json()
        # matomo returns error with HTTP 200
        if isinstance(json_response, list):
            for part in json_response:
                if isinstance(part, dict) and part.get("result") == "error":
                    raise MatomoResponseError(part["message"])
        return json_response

    def get_query(
        self,
        date: str,
        extra_params: DictStrAny,
        methods: List[str],
        period: str,
        site_id: int,
    ) -> TDataItem:
        """
        Helper method that gets data in a batch from Matomo.

        Args:
            date (str): Date or date range for the data retrieval.
            extra_params (DictStrAny): Extra parameters for the API request.
            methods (List[str]): List of methods to retrieve data for.
            period (str): Period for the data retrieval (e.g., "day", "month", "year").
            site_id (int): Unique ID of the Matomo site.

        Returns:
            TDataItem: JSON data from the response.
        """
        # Set up the API URL and parameters
        if not extra_params:
            extra_params = {}
        params = {
            "module": "API",
            "method": "API.getBulkRequest",
            "format": "json",
            "token_auth": self.auth_token,
        }
        for i, method in enumerate(methods):
            params[
                f"urls[{i}]"
            ] = f"method={method}&idSite={site_id}&period={period}&date={date}"
        # Merge the additional parameters into the request parameters
        params.update(extra_params)
        # Send the API request
        return self._request(params=params)

    def get_method(
        self,
        extra_params: DictStrAny,
        method: str,
        site_id: int,
        rows_per_page: int = 10000,
    ) -> Iterator[TDataItems]:
        """
        Helper method that gets data using a Matomo API method.

        Args:
            extra_params (DictStrAny): Extra parameters for the API request.
            method (str): Unique report from the Matomo API.
            site_id (int): Unique ID of the Matomo site.
            rows_per_page (int): Number of rows returned per page from the request.

        Yields:
            Iterator[TDataItems]: JSON data from the response.
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
            "urls[0]": f"method={method}&idSite={site_id}&filter_limit={rows_per_page}&filter_offset={filter_offset}",
        }
        # Merge the additional parameters into the request parameters
        params.update(extra_params)
        # Send the API request
        method_data = self._request(params=params)[0]
        while len(method_data):
            yield method_data
            filter_offset += len(method_data)
            params[
                "urls[0]"
            ] = f"method={method}&idSite={site_id}&filter_limit={rows_per_page}&filter_offset={filter_offset}"
            method_data = self._request(params=params)[0]

    def get_visitors_batch(
        self, visitor_list: List[str], site_id: int, extra_params: DictStrAny = None
    ) -> TDataItems:
        """
        Gets visitors for Matomo.

        Args:
            visitor_list (List[str]): List of visitor IDs.
            site_id (int): Unique ID of the Matomo site.
            extra_params (DictStrAny): Extra parameters for the API request.

        Returns:
            TDataItems: JSON data from the response.
        """
        if not extra_params:
            extra_params = {}
        params = {
            "module": "API",
            "method": "API.getBulkRequest",
            "format": "json",
            "site_id": site_id,
            "token_auth": self.auth_token,
        }
        params.update(
            {
                f"urls[{i}]": f"method=Live.getVisitorProfile&idSite={site_id}&visitorId={visitor_list[i]}"
                for i in range(len(visitor_list))
            }
        )
        params.update(extra_params)
        method_data = self._request(params=params)
        return method_data

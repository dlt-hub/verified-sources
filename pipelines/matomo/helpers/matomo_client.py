"""This module contains an implementation of a Matomo API client for python."""
from typing import Iterator
from dlt.common.typing import TDataItems
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
        headers = {'Content-type': 'application/json'}
        url = f"{self.base_url}/index.php"
        response = client.get(url=url, headers=headers, params=params)
        response.raise_for_status()
        yield response.json()

    def get_matomo_data(self, api_method: str, site_id: int, period: str, date: str, extra_parameters: dict = None) -> Iterator[TDataItems]:
        """
        Retrieves data from the Matomo API for a given site using multiple Matomo API methods.
        :param extra_parameters:
        :param date:
        :param period:
        :param api_method:
        :param site_id:
        :return:
        """
        # Set up the API URL and parameters
        if extra_parameters is None:
            extra_parameters = {}
        params = {
            "module": "API",
            "method": api_method,
            "idSite": site_id,
            "period": period,
            "date": date,
            "format": "json",
            "token_auth": self.auth_token
        }
        # Merge the additional parameters into the request parameters
        params.update(extra_parameters)
        # Send the API request
        yield from self._request(params=params)


# if __name__ == "__main__":
#     api_client = MatomoAPIClient(base_url="", auth_token="")
#     meta = api_client.get_matomo_data(site_id=2, api_method="API.getMatomoVersion", period="month", date="2022-03-01", extra_parameters=None)
#     print(meta)

"""This module contains an implementation of a Matomo API client for python."""
from typing import Dict, Iterator, List, Optional, Tuple, Union
from dlt.sources.helpers.requests import client
from dlt.common.typing import DictStrStr, TDataItems, TSecretValue


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

    def get_visits(self, id_site: int, period: str, date: str):
        params = {
            'module': 'API',
            'method': 'VisitsSummary.get',
            'idSite': id_site,
            'format': 'json',
            'token_auth': self.auth_token,
            'period': period,
            'date': date
        }
        return self._request(params=params)

    def get_metadata(self, id_site: int, period: str, date: str):
        params = {
            'module': 'API',
            'method': 'API.getReportMetadata',
            'idSite': id_site,
            'format': 'json',
            'token_auth': self.auth_token,
            'period': period,
            'date': date
        }
        return self._request(params=params)

    # def get_matomo_data(self, api_methods: List[str], site_id: int, parameters: Dict[str, str]) -> Iterator[TDataItems]:
    #     """
    #     Retrieves data from the Matomo API for a given site using multiple Matomo API methods.
    #
    #     Parameters:
    #         api_methods (list): A list of Matomo API methods to use (e.g. ['API.getSiteMetadata', 'VisitsSummary.get']).
    #         site_id (int): The ID of the site to retrieve data for.
    #         parameters (dict): A dictionary of additional parameters to include in the API request.
    #
    #     Returns:
    #         dict: A dictionary containing the data retrieved from the Matomo API for each of the requested methods
    #     :param api_methods:
    #     :param site_id:
    #     :param parameters:
    #     :return:
    #     """
    #     # Set up the API URL and parameters
    #     params = {
    #         "module": "API",
    #         "method": api_methods,
    #         "idSite": site_id,
    #         "format": "json",
    #         "token_auth": self.auth_token
    #     }
    #     # Merge the additional parameters into the request parameters
    #     params.update(parameters)
    #     # Send the API request
    #     yield from self._request(params=params)

# if __name__ == "__main__":
#     api_client = MatomoAPIClient(base_url="", auth_token="")
#     # visits = api_client.get_visits(id_site=2, period="month", date="2022-03-01")
#     meta = api_client.get_metadata(id_site=2, period="month", date="2022-03-01")
#     print(meta)

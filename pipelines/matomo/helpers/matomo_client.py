"""This module contains an implementation of a Matomo API client for python."""
import requests
from typing import Dict, Iterator, Optional, Tuple, Union
from dlt.common import logger
from dlt.common.typing import DictStrStr, TDataItems, TSecretValue

MATOMO_STATUS_CODES = {
    "ok": 200,
    "rate_limit": 429
}


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

    def _request(self, endpoint: str, id_site: int, period: str, date: str):
        headers = {'Content-type': 'application/json'}
        url = f"{self.base_url}/index.php?module=API&method={endpoint}&idSite={id_site}&period={period}&date={date}&format=json&token_auth={self.auth_token}"
        response = requests.get(url=url, headers=headers)
        response.raise_for_status()
        return response.json()

    def get_visits(self, id_site, period, date):
        params = {
            "idSite": id_site,
            "period": period,
            "date": date,
            "method": "VisitsSummary.get"
        }
        return self._request(endpoint=params["method"], id_site=params["idSite"], period=params["period"], date=params["date"])


if __name__ == "__main__":
    api_client = MatomoAPIClient(base_url="", auth_token="")
    visits = api_client.get_visits(id_site=2, period="month", date="2022-03-01")
    print(visits)

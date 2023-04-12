"""This module contains an implementation of a Matomo API client for python."""
import logging
from time import sleep
from typing import Dict, Iterator, Optional, Tuple, Union
from dlt.common import logger
from dlt.common.typing import DictStrStr, TDataItems, TSecretValue
from dlt.sources.helpers.requests import client

MATOMO_STATUS_CODES = {
    "ok": 200,
    "rate_limit": 429
}


class MatomoAPIClient:
    """
    API client used to make requests to Matomo API.
    """
    base_url: str = ""
    headers: Optional[DictStrStr]
    auth: Optional[Tuple[str, TSecretValue]]
    auth_token: str = ""

    def __init__(self, base_url: str, auth_token: str) -> None:
        """
        Initializes the client which is then used to make api requests.
        :param auth_token: Token that provides access to the api.
        :param base_url: The url of the domain which is being analyzed.
        """

        self.base_url = base_url
        self.auth_token = auth_token

    def make_request(self, endpoint: str, data_point_name: str, params: Optional[Dict[str, int]] = None) -> Iterator[TDataItems]:
        """

        :param endpoint:
        :param data_point_name:
        :param params:
        :return:
        """

        # get_url = f"{self.base_url}/index.php?module=API&method={endpoint}&format=json"
        get_url = f"{self.base_url}/index.php?module=API&method={endpoint}&idSite=2&period=month&date=last30&format=json&token_auth={self.auth_token}"

        while get_url:
            try:
                response = client.get(get_url, headers=self.headers, auth=self.auth, params=params)
                logging.warning(response)
                if response.status_code == MATOMO_STATUS_CODES["ok"]:
                    # check if there is a next page and yield the response,
                    # usually all relevant data is stored in a key with same name as endpoint
                    response_json = response.json()
                    get_url = response_json.get("next_page", None)
                    yield response_json[data_point_name]
                else:
                    get_url = None
                    logger.warning(f"API call failed on endpoint {endpoint} with error code {response.status_code}")
            except Exception as e:
                logger.warning(f"Encountered an error on url: {get_url}")
                logger.warning(str(e))
                get_url = None


if __name__ == "__main__":
    matomo_client = MatomoAPIClient(base_url="", auth_token="")
    matomo_client.make_request(endpoint="VisitsSummary.get", data_point_name="visits")

"""This module contains an implementation of a Matomo API client for python."""
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
    API client used to make requests to MATOMO API.
    """
    url: str = ""
    headers: Optional[DictStrStr]
    auth: Optional[Tuple[str, TSecretValue]]

    def __init__(self, api_token: str, domain_url: str) -> None:
        """
        Initializes the client which is then used to make api requests.
        :param api_token: Token that provides access to the api.
        :param domain_url: The url of the domain which is being analyzed.
        """

        self.url = domain_url

        pass

    def make_request(self, endpoint: str, data_point_name: str, params: Optional[Dict[str, int]] = None) -> Iterator[TDataItems]:
        """

        :param endpoint:
        :param data_point_name:
        :param params:
        :return:
        """
        pass

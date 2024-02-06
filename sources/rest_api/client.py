from typing import Optional, Dict, Any, Generator, Literal
import copy

from requests.auth import AuthBase

from dlt.common import logger
from dlt.sources.helpers import requests

from .paginators import BasePaginator, UnspecifiedPaginator
from .detector import create_paginator

from .utils import join_url

class RESTClient:
    """A generic REST client for making requests to an API.

    Attributes:
        base_url (str): The base URL of the API.
        headers (Optional[Dict[str, str]]): Headers to include in all requests.
        auth (Optional[AuthBase]): An authentication object to use for all requests.
        paginator (Optional[BasePaginator]): A paginator object for handling API pagination.
            Note that this object will be deepcopied for each request to ensure that the
            paginator state is not shared between requests.
    """

    def __init__(
        self,
        base_url: str,
        headers: Optional[Dict[str, str]] = None,
        auth: Optional[AuthBase] = None,
        paginator: Optional[BasePaginator] = None,
    ) -> None:
        self.base_url = base_url
        self.headers = headers
        self.auth = auth
        self.paginator = paginator if paginator else UnspecifiedPaginator()

    def make_request(self, path="", method="get", params=None, json=None):
        if path.startswith("http"):
            url = path
        else:
            url = join_url(self.base_url, path)

        logger.info(
            f"Making {method.upper()} request to {url} with params={params}, "
            f"json={json}"
        )

        response = requests.request(
            method=method,
            url=url,
            headers=self.headers,
            params=params if method.lower() == "get" else None,
            json=json if method.lower() in ["post", "put"] else None,
            auth=self.auth,
        )
        response.raise_for_status()
        return response

    def get(self, path="", params=None):
        return self.make_request(path, method="get", params=params)

    def post(self, path="", json=None):
        return self.make_request(path, method="post", json=json)

    def paginate(
        self,
        path: str = "",
        method: Literal["get", "post"] = "get",
        params: Optional[Dict[str, Any]] = None,
        json: Optional[Dict[str, Any]] = None,
        paginator: Optional[BasePaginator] = None,
    ) -> Generator[Any, None, None]:
        """Paginate over an API endpoint.

        Example:
            >>> client = APIClient(...)
            >>> for page in client.paginate("/search", method="post", json={"query": "foo"}):
            >>>     print(page)
        """
        paginator = copy.deepcopy(paginator if paginator else self.paginator)
        while paginator.has_next_page:
            response = self.make_request(
                path=path, method=method, params=params, json=json
            )

            if isinstance(paginator, UnspecifiedPaginator):
                # Detect suitable paginator and it's params
                paginator = create_paginator(response)

                # If no paginator is found, raise an error
                if paginator is None:
                    raise ValueError(
                        "No suitable paginator found for the API response."
                    )
                else:
                    logger.info(f"Detected paginator: {paginator.__class__.__name__}")

            yield paginator.extract_records(response)

            paginator.update_state(response)
            path, params, json = paginator.prepare_next_request_args(path, params, json)

    def __iter__(self):
        return self.paginate()

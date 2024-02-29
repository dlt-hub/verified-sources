from typing import Optional, List, Dict, Any, Union, Generator, Literal
import copy

from requests.auth import AuthBase
from requests import Session as BaseSession
from requests import Response
from requests.exceptions import HTTPError

from dlt.common import logger
from dlt.sources.helpers.requests.retry import Client

from .paginators import (
    BasePaginator,
    UnspecifiedPaginator,
    SinglePagePaginator,
    JSONResponsePaginator,
    HeaderLinkPaginator,
)
from .detector import create_paginator, find_records_key

from .utils import join_url, create_nested_accessor


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
        session: BaseSession = None,
        request_client: Client = None,
    ) -> None:
        self.base_url = base_url
        self.headers = headers
        self.auth = auth
        if session:
            self.session = session
        elif request_client:
            self.session = request_client.session
        else:
            self.session = Client().session

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

        response = self.session.request(
            method=method,
            url=url,
            headers=self.headers,
            params=params if method.lower() == "get" else None,
            json=json if method.lower() in ["post", "put"] else None,
            auth=self.auth,
        )
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
        records_path: Optional[Union[str, List[str]]] = None,
        response_actions: Optional[List[Dict[str, Any]]] = None,
    ) -> Generator[Any, None, None]:
        """Paginate over an API endpoint.

        Example:
            >>> client = APIClient(...)
            >>> for page in client.paginate("/search", method="post", json={"query": "foo"}):
            >>>     print(page)
        """
        paginator = copy.deepcopy(paginator if paginator else self.paginator)

        extract_records = (
            self.create_records_extractor(records_path) if records_path else None
        )

        while paginator.has_next_page:
            try:
                response = self.make_request(
                    path=path, method=method, params=params, json=json
                )
            except HTTPError as e:
                if not response_actions:
                    raise e
                else:
                    response = e.response

            if response_actions:
                action_type = self.handle_response_actions(response, response_actions)
                if action_type == "ignore":
                    logger.info(
                        f"Error {response.status_code}. Ignoring response '{response.json()}' and stopping pagination."
                    )
                    break
                elif action_type == "retry":
                    logger.info("Retrying request.")
                    continue

            if isinstance(paginator, UnspecifiedPaginator):
                # Detect suitable paginator and its params
                paginator = create_paginator(response)

                # If no paginator is found, raise an error
                if paginator is None:
                    raise ValueError(
                        f"No suitable paginator found for the response at {response.url}"
                    )
                else:
                    logger.info(f"Detected paginator: {paginator.__class__.__name__}")

            # If extract_records is None, try to detect records key
            # based on the paginator type
            if extract_records is None:
                if isinstance(paginator, (SinglePagePaginator, HeaderLinkPaginator)):
                    extract_records = lambda response: response.json()  # noqa
                elif isinstance(paginator, JSONResponsePaginator):
                    _records_path = find_records_key(response.json())
                    if _records_path:
                        extract_records = self.create_records_extractor(_records_path)

            yield extract_records(response)

            paginator.update_state(response)
            path, params, json = paginator.prepare_next_request_args(path, params, json)

    def create_records_extractor(self, records_path: Optional[Union[str, List[str]]]):
        nested_accessor = create_nested_accessor(records_path)

        return lambda response: nested_accessor(response.json())

    def handle_response_actions(
        self, response: Response, actions: List[Dict[str, Any]]
    ):
        """Handle response actions based on the response and the provided actions.

        Example:
        response_actions = [
            {"status_code": 404, "action": "ignore"},
            {"content": "Not found", "action": "ignore"},
            {"status_code": 429, "action": "retry"},
            {"status_code": 200, "content": "some text", "action": "retry"},
        ]
        action_type = client.handle_response_actions(response, response_actions)
        """
        content = response.text

        for action in actions:
            status_code = action.get("status_code")
            content_substr = action.get("content")
            action_type = action.get("action")

            if status_code is not None and content_substr is not None:
                if response.status_code == status_code and content_substr in content:
                    return action_type

            elif status_code is not None:
                if response.status_code == status_code:
                    return action_type

            elif content_substr is not None:
                if content_substr in content:
                    return action_type

        return None

    def __iter__(self):
        return self.paginate()

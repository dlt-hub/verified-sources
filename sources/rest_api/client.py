from typing import Optional, List, Dict, Any, Union, Generator, Literal
import copy
from urllib.parse import urlparse

from requests.auth import AuthBase
from requests import Session as BaseSession
from requests import Response, Request

from dlt.common import logger
from dlt.common import jsonpath
from dlt.sources.helpers.requests.retry import Client

from .paginators import BasePaginator
from .detector import create_paginator, find_records

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
        session: BaseSession = None,
    ) -> None:
        self.base_url = base_url
        self.headers = headers
        self.auth = auth
        if session:
            self.session = session
        else:
            self.session = Client(raise_for_status=False).session

        self.paginator = paginator

    def _create_request(
        self,
        path: str,
        method: str,
        params: Dict[str, Any],
        json: Optional[Dict[str, Any]] = None,
        auth: Optional[AuthBase] = None,
        hooks: Optional[Dict[str, Any]] = None,
    ) -> Request:
        parsed_url = urlparse(path)
        if parsed_url.scheme in ("http", "https"):
            url = path
        else:
            url = join_url(self.base_url, path)

        auth = auth or self.auth

        return Request(
            method=method,
            url=url,
            headers=self.headers,
            params=params if method.lower() == "get" else None,
            json=json if method.lower() in ["post", "put"] else None,
            auth=auth,
            hooks=hooks,
        )

    def _send_request(self, request: Request) -> Response:
        logger.info(
            f"Making {request.method.upper()} request to {request.url}"
            f" with params={request.params}, json={request.json}"
        )

        prepared_request = self.session.prepare_request(request)

        return self.session.send(prepared_request)

    def request(
        self, path: str = "", method: Literal["get", "post"] = "get", **kwargs: Any
    ) -> Response:
        prepared_request = self._create_request(
            path=path,
            method=method,
            **kwargs,
        )
        return self._send_request(prepared_request)

    def get(
        self, path: str = "", params: Optional[Dict[str, Any]] = None, **kwargs: Any
    ) -> Response:
        return self.request(path, method="get", params=params, **kwargs)

    def post(
        self, path: str = "", json: Optional[Dict[str, Any]] = None, **kwargs: Any
    ) -> Response:
        return self.request(path, method="post", json=json, **kwargs)

    def paginate(
        self,
        path: str = "",
        method: Literal["get", "post"] = "get",
        params: Optional[Dict[str, Any]] = None,
        json: Optional[Dict[str, Any]] = None,
        auth: Optional[AuthBase] = None,
        paginator: Optional[BasePaginator] = None,
        data_selector: Optional[Union[str, List[str]]] = None,
        response_actions: Optional[List[Dict[str, Any]]] = None,
        hooks: Optional[Dict[str, Any]] = None,
    ) -> Generator[Any, None, None]:
        """Paginate over an API endpoint.

        Example:
            >>> client = APIClient(...)
            >>> for page in client.paginate("/search", method="post", json={"query": "foo"}):
            >>>     print(page)
        """
        paginator = copy.deepcopy(paginator if paginator else self.paginator)

        # extract_records = (
        #     self.create_records_extractor(data_selector) if data_selector else None
        # )

        request = self._create_request(
            path=path, method=method, params=params, json=json, auth=auth, hooks=hooks
        )

        while True:
            response = self._send_request(request)

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

            if paginator is None:
                paginator = self.detect_paginator(response)

            if data_selector:
                # we should compile data_selector
                data = jsonpath.find_values(data_selector, response.json())
                # extract if single item selected
                yield data[0] if len(data) == 1 else data
            else:
                yield find_records(response.json())

            paginator.update_state(response)
            paginator.update_request(request)

            if not paginator.has_next_page:
                break

    def detect_paginator(self, response: Response) -> BasePaginator:
        paginator = create_paginator(response)
        if paginator is None:
            raise ValueError(
                f"No suitable paginator found for the response at {response.url}"
            )
        logger.info(f"Detected paginator: {paginator.__class__.__name__}")
        return paginator

    def handle_response_actions(
        self, response: Response, actions: List[Dict[str, Any]]
    ) -> Optional[str]:
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
            content_substr: str = action.get("content")
            action_type: str = action.get("action")

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

    def __iter__(self) -> Generator[Any, None, None]:
        return self.paginate()

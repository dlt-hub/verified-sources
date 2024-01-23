from abc import ABC, abstractmethod
from typing import Optional, Dict, Any, Generator, Tuple
import copy
from requests.auth import AuthBase

from dlt.sources.helpers import requests
from dlt.sources.helpers.requests import Response


class BasePaginator(ABC):
    def __init__(self) -> None:
        self._has_next_page = True

    @property
    def has_next_page(self) -> bool:
        """
        Check if there is a next page available.

        Returns:
            bool: True if there is a next page available, False otherwise.
        """
        return self._has_next_page

    @abstractmethod
    def update_state(self, response: Response) -> None:
        """Update the paginator state based on the response.

        Args:
            response (Response): The response object from the API.
        """
        ...

    @abstractmethod
    def prepare_next_request_args(
        self, url: str, params: Optional[Dict[str, Any]], json: Optional[Dict[str, Any]]
    ) -> Tuple[Optional[str], Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
        """
        Prepare the arguments for the next API request based on the current state of pagination.

        Subclasses must implement this method to update the request arguments appropriately.

        Args:
            url (str): The original URL used in the current API request.
            params (Optional[Dict[str, Any]]): The original query parameters used in the current API request.
            json (Optional[Dict[str, Any]]): The original JSON body of the current API request.

        Returns:
            tuple: A tuple containing the updated URL, query parameters, and JSON body to be used
                for the next API request. These values are used to progress through the paginated data.
        """
        ...

    @abstractmethod
    def extract_records(self, response: Response) -> Any:
        """
        Extract the records data from the response.

        Args:
            response (Response): The response object from the API.

        Returns:
            Any: The extracted records data.
        """
        ...


class HeaderLinkPaginator(BasePaginator):
    """A paginator that uses the 'Link' header in HTTP responses
    for pagination.

    A good example of this is the GitHub API:
        https://docs.github.com/en/rest/guides/traversing-with-pagination
    """

    def __init__(self, links_next_key: str = "next") -> None:
        super().__init__()
        self.links_next_key = links_next_key
        self.next_url: Optional[str] = None

    def update_state(self, response: Response) -> None:
        self.next_url = response.links.get(self.links_next_key, {}).get("url")
        self._has_next_page = self.next_url is not None

    def prepare_next_request_args(self, url, params, json):
        return self.next_url, params, json

    def extract_records(self, response: Response) -> Any:
        return response.json()


class JSONResponsePaginator(BasePaginator):
    """A paginator that uses a specific key in the JSON response to find
    the next page URL.
    """

    def __init__(self, next_key: str = "next", records_key: str = "results"):
        """
        Args:
            next_key (str, optional): The key in the JSON response that
                contains the next page URL. Defaults to 'next'.
            records_key (str, optional): The key in the JSON response that
                contains the page's records. Defaults to 'results'.
        """
        super().__init__()
        self.next_url: Optional[str] = None
        self.next_key = next_key
        self.records_key = records_key

    def update_state(self, response: Response):
        self.next_url = response.json().get(self.next_key)
        self._has_next_page = self.next_url is not None

    def prepare_next_request_args(self, url, params, json):
        return self.next_url, params, json

    def extract_records(self, response: Response) -> Any:
        return response.json().get(self.records_key, [])


class BearerTokenAuth(AuthBase):
    def __init__(self, token: str):
        self.token = token

    def __call__(self, request):
        request.headers["Authorization"] = f"Bearer {self.token}"
        return request


def join_url(base_url: str, path: str) -> str:
    if not base_url.endswith("/"):
        base_url += "/"
    return base_url + path.lstrip("/")


class APIClient:
    """A generic API client for making requests to an API.

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
        self.paginator = paginator if paginator else HeaderLinkPaginator()

    def make_request(self, path="", method="get", params=None, json=None):
        if path.startswith("http"):
            url = path
        else:
            url = join_url(self.base_url, path)

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
        method: str = "get",
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

            yield paginator.extract_records(response)

            paginator.update_state(response)
            path, params, json = paginator.prepare_next_request_args(path, params, json)

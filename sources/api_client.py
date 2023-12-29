from abc import ABC, abstractmethod
from typing import Optional, Dict, Any, Generator
from requests.auth import AuthBase

from dlt.sources.helpers import requests
from dlt.sources.helpers.requests import Response


class BasePaginator(ABC):
    @abstractmethod
    def paginate(
        self,
        client: "APIClient",
        url: str,
        method: str,
        params: Optional[Dict[str, Any]],
        json: Optional[Dict[str, Any]],
    ) -> Generator[Any, None, None]:
        pass


class HeaderLinkPaginator(BasePaginator):
    """A paginator that uses the 'Link' header in HTTP responses
    for pagination.

    A good example of this is the GitHub API:
        https://docs.github.com/en/rest/guides/traversing-with-pagination
    """
    def get_next_url(self, response: Response) -> Optional[str]:
        return response.links.get("next", {}).get("url")

    def paginate(
        self,
        client: "APIClient",
        url: str,
        method: str,
        params: Optional[Dict[str, Any]],
        json: Optional[Dict[str, Any]],
    ) -> Generator[Dict[str, Any], None, None]:
        while url:
            response = client.make_request(url, method, params, json)

            yield response.json()

            url = self.get_next_url(response)


class JSONResponsePaginator(BasePaginator):
    """A paginator that uses a specific key in the JSON response to find
    the next page URL.
    """
    def __init__(self, next_key: str = "next", content_key: str = "results"):
        """
        Args:
            next_key (str, optional): The key in the JSON response that
                contains the next page URL. Defaults to 'next'.
            content_key (str, optional): The key in the JSON response that
                contains the page content. Defaults to 'results'.
        """
        self.next_key = next_key
        self.content_key = content_key

    def get_next_url(self, response: Response) -> Optional[str]:
        return response.json().get(self.next_key)

    def extract_page_content(self, response: Response) -> Any:
        return response.json().get(self.content_key)

    def paginate(
        self,
        client: "APIClient",
        url: str,
        method: str,
        params: Optional[Dict[str, Any]],
        json: Optional[Dict[str, Any]],
    ) -> Generator[Any, None, None]:
        while url:
            response = client.make_request(url, method, params, json)
            yield self.extract_page_content(response)
            url = self.get_next_url(response)


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

        paginator = paginator if paginator else self.paginator

        return paginator.paginate(self, path, method, params, json)

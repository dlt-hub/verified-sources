from abc import ABC, abstractmethod
from typing import Optional, Sequence, Union

from dlt.sources.helpers.requests import Response, Request

from .utils import create_nested_accessor
from .detector import PaginatorDetectorMixin, NEXT_PAGE_KEY_PATTERNS


class BasePaginator(ABC):
    def __init__(self) -> None:
        self._has_next_page = True
        self._next_reference: Optional[str] = None

    @property
    def has_next_page(self) -> bool:
        """
        Check if there is a next page available.

        Returns:
            bool: True if there is a next page available, False otherwise.
        """
        return self._has_next_page

    @property
    def next_reference(self) -> Optional[str]:
        return self._next_reference

    @next_reference.setter
    def next_reference(self, value: Optional[str]):
        self._next_reference = value
        self._has_next_page = value is not None

    @abstractmethod
    def update_state(self, response: Response) -> None:
        """Update the paginator state based on the response.

        Args:
            response (Response): The response object from the API.
        """
        ...

    @abstractmethod
    def update_request(self, request: Request) -> None:
        """
        Update the request object with the next arguments for the API request.

        Args:
            request (Request): The request object to be updated.
        """
        ...


class SinglePagePaginator(BasePaginator, PaginatorDetectorMixin):
    """A paginator for single-page API responses."""

    def update_state(self, response: Response) -> None:
        self._has_next_page = False

    def update_request(self, request: Request) -> None:
        return

    @classmethod
    def detect(cls, response: Response):
        links_next_key = "next"
        if response.links.get(links_next_key):
            return cls()
        return None


class OffsetPaginator(BasePaginator):
    """A paginator that uses the 'offset' parameter for pagination."""

    def __init__(
        self,
        initial_offset,
        initial_limit,
        offset_key: str = "offset",
        limit_key: str = "limit",
        total_key: str = "total",
    ):
        super().__init__()
        self.offset_key = offset_key
        self.limit_key = limit_key
        self._total_accessor = create_nested_accessor(total_key)

        self.offset = initial_offset
        self.limit = initial_limit

    def update_state(self, response: Response) -> None:
        total = self._total_accessor(response.json())

        if total is None:
            raise ValueError(
                f"Total count not found in response for {self.__class__.__name__}"
            )

        self.offset += self.limit

        if self.offset >= total:
            self._has_next_page = False

    def update_request(self, request: Request) -> None:
        if request.params is None:
            request.params = {}

        request.params[self.offset_key] = self.offset
        request.params[self.limit_key] = self.limit


class BaseNextUrlPaginator(BasePaginator):
    def update_request(self, request: Request) -> None:
        request.url = self._next_reference


class HeaderLinkPaginator(BaseNextUrlPaginator, PaginatorDetectorMixin):
    """A paginator that uses the 'Link' header in HTTP responses
    for pagination.

    A good example of this is the GitHub API:
        https://docs.github.com/en/rest/guides/traversing-with-pagination
    """

    def __init__(self, links_next_key: str = "next") -> None:
        """
        Args:
            links_next_key (str, optional): The key (rel ) in the 'Link' header
                that contains the next page URL. Defaults to 'next'.
        """
        super().__init__()
        self.links_next_key = links_next_key

    def update_state(self, response: Response) -> None:
        self.next_reference = response.links.get(self.links_next_key, {}).get("url")

    @classmethod
    def detect(cls, response: Response):
        links_next_key = "next"
        if response.links.get(links_next_key):
            return cls()
        return None


class JSONResponsePaginator(BaseNextUrlPaginator, PaginatorDetectorMixin):
    """A paginator that uses a specific key in the JSON response to find
    the next page URL.
    """

    def __init__(
        self,
        next_key: Union[str, Sequence[str]] = "next",
    ):
        """
        Args:
            next_key (str, optional): The key in the JSON response that
                contains the next page URL. Defaults to 'next'.
        """
        super().__init__()
        self.next_key = next_key
        self._next_key_accessor = create_nested_accessor(next_key)

    def update_state(self, response: Response):
        try:
            self.next_reference = self._next_key_accessor(response.json())
        except KeyError:
            self.next_reference = None

    @classmethod
    def detect(cls, response: Response):
        dictionary = response.json()
        next_key = cls.find_next_page_key(dictionary)
        if not next_key:
            return None
        return cls(next_key=next_key)

    @staticmethod
    def find_next_page_key(dictionary, path=None):
        if not isinstance(dictionary, dict):
            return None

        if path is None:
            path = []

        for key, value in dictionary.items():
            normalized_key = key.lower()
            if any(pattern in normalized_key for pattern in NEXT_PAGE_KEY_PATTERNS):
                return [*path, key]

            if isinstance(value, dict):
                result = JSONResponsePaginator.find_next_page_key(value, [*path, key])
                if result:
                    return result

        return None


class UnspecifiedPaginator(BasePaginator):
    def update_state(self, response: Response) -> None:
        return Exception("Can't update state with this paginator")

    def update_request(self, request: Request) -> None:
        return

    def autodetect(self, response: Response):
        paginator_classes = [
            HeaderLinkPaginator,
            JSONResponsePaginator,
            SinglePagePaginator,
        ]
        for PaginatorClass in paginator_classes:
            paginator = PaginatorClass.detect(response)
            if paginator:
                return paginator
        return None

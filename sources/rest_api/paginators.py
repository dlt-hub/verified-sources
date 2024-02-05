from abc import ABC, abstractmethod
from typing import Optional, Dict, Any, Tuple, Sequence, Union

from dlt.sources.helpers.requests import Response

from .utils import create_nested_accessor


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


class SinglePagePaginator(BasePaginator):
    """A paginator for single-page API responses."""

    def update_state(self, response: Response) -> None:
        self._has_next_page = False

    def prepare_next_request_args(self, url, params, json):
        return None, None, None

    def extract_records(self, response: Response) -> Any:
        return response.json()


class OffsetPaginator(BasePaginator):
    """A paginator that uses the 'offset' parameter for pagination."""

    def __init__(
        self,
        initial_offset,
        initial_limit,
        records_key: Union[str, Sequence[str]] = "results",
        offset_key: str = "offset",
        limit_key: str = "limit",
        total_key: str = "total",
    ):
        self.offset_key = offset_key
        self.limit_key = limit_key
        self._records_accessor = create_nested_accessor(records_key)
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

    def prepare_next_request_args(self, url, params, json):
        if params is None:
            params = {}

        params[self.offset_key] = self.offset
        params[self.limit_key] = self.limit

        return url, params, json


class BaseNextUrlPaginator(BasePaginator):
    def prepare_next_request_args(self, url, params, json):
        return self._next_reference, params, json


class HeaderLinkPaginator(BaseNextUrlPaginator):
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

    def extract_records(self, response: Response) -> Any:
        return response.json()


class JSONResponsePaginator(BaseNextUrlPaginator):
    """A paginator that uses a specific key in the JSON response to find
    the next page URL.
    """

    def __init__(
        self,
        next_key: Union[str, Sequence[str]] = "next",
        records_key: Union[str, Sequence[str]] = "results",
    ):
        """
        Args:
            next_key (str, optional): The key in the JSON response that
                contains the next page URL. Defaults to 'next'.
            records_key (str, optional): The key in the JSON response that
                contains the page's records. Defaults to 'results'.
        """
        super().__init__()
        self.next_key = next_key
        self.records_key = records_key
        self._next_key_accessor = create_nested_accessor(next_key)
        self._records_accessor = create_nested_accessor(records_key)

    def update_state(self, response: Response):
        self.next_reference = self._next_key_accessor(response.json())

    def extract_records(self, response: Response) -> Any:
        return self._records_accessor(response.json())


class UnspecifiedPaginator(BasePaginator):
    def extract_records(self, response: Response) -> Any:
        raise Exception("Can't extract records with this paginator")

    def update_state(self, response: Response) -> None:
        return Exception("Can't update state with this paginator")

    def prepare_next_request_args(self, url: str, params, json):
        return Exception("Can't prepare next request with this paginator")

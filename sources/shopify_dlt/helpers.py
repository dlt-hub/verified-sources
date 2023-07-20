"""Shopify source helpers"""

from dlt.sources.helpers import requests
from dlt.common.typing import TDataItem, TDataItems, Dict
from typing import Any, Iterable, Optional, Literal

from .settings import DEFAULT_API_VERSION
from .date_helper import ensure_pendulum_datetime

TOrderStatus = Literal["open", "closed", "cancelled", "any"]


class ShopifyApi:
    """
    A Shopify API client that can be used to get pages of data from Shopify.
    """

    def __init__(
        self,
        shop_url: str,
        private_app_password: str,
        api_version: str = DEFAULT_API_VERSION,
    ) -> None:
        """
        Args:
            shop_url: The URL of your shop (e.g. https://my-shop.myshopify.com).
            private_app_password: The private app password to the app on your shop.
            api_version: The API version to use (e.g. 2023-01)
        """
        self.shop_url = shop_url
        self.private_app_password = private_app_password
        self.api_version = api_version

    def get_pages(
        self, resource: str, params: Optional[Dict[str, Any]] = None
    ) -> Iterable[TDataItems]:
        """Get all pages from shopify using requests.
        Iterates through all pages and yield each page items.

        Args:
            resource: The resource to get pages for (e.g. products, orders, customers).
            params: Query params to include in the request.

        Yields:
            List of data items from the page
        """
        url = f"{self.shop_url}/admin/api/{self.api_version}/{resource}.json"
        headers = {"X-Shopify-Access-Token": self.private_app_password}
        while url:
            response = requests.get(url, params=params, headers=headers)
            response.raise_for_status()
            json = response.json()
            # Get item list from the page
            yield [self._convert_datetime_fields(item) for item in json[resource]]
            link = response.headers.get("Link")
            url = self._get_next_page_url(link)
            # Query params are included in subsequent page URLs
            params = None

    def _get_next_page_url(self, link_header: Optional[str]) -> Optional[str]:
        """Get the next page URL from the shopify link header

        Args:
            link_header: The link header from the response
                e.g. <https://my-shop.myshopify.com/admin/api/2021-07/products.json?page_info=eyJkaXJlY3Rpb24iOiJwcm9kdWN0X2lkIn0>; rel="next"

        Returns:
            The next page URL or None if there is no next page
        """
        if link_header:
            for link in link_header.split(","):
                parts = link.split(";")
                if len(parts) == 2 and parts[1].strip() == 'rel="next"':
                    url = parts[0].strip(" <>")
                    return url
        return None

    def _convert_datetime_fields(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """Convert timestamp fields in the item to pendulum datetime objects

        The item is modified in place.

        Args:
            item: The item to convert

        Returns:
            The same data item (for convenience)
        """
        fields = ["created_at", "updated_at"]
        for field in fields:
            if field in item:
                item[field] = ensure_pendulum_datetime(item[field])
        return item

"""Fetches Shopify Orders and Products."""

from typing import Any, Dict, Iterator, Iterator, Optional, Iterable

import shopify

import dlt

from dlt.extract.source import DltResource
from dlt.common.typing import TDataItem

from .settings import DEFAULT_API_VERSION, FIRST_DAY_OF_MILLENNIUM
from .helpers import iterate_page


@dlt.source(name="shopify")
def shopify_source(
    private_app_password: str = dlt.secrets.value,
    api_version: str = DEFAULT_API_VERSION,
    shop_url: str = dlt.config.value,
    start_date: Optional[str] = FIRST_DAY_OF_MILLENNIUM,
) -> Iterable[DltResource]:
    """
    The source for the Shopify pipeline. Available resources are products, orders, and customers.

    Args:
        private_app_password (str): The app password to the app on your shop.
        api_version (str): The API version to use.
        shop_url (str): The URL of your shop.
        start_date (Optional[str]): The date from which to import items. It will import items initially created on or after this date.

    Returns:
        Iterable[DltResource]: A list of DltResource objects representing the data resources.
    """

    # build client
    session = shopify.Session(shop_url, api_version, private_app_password)
    shopify.ShopifyResource.activate_session(session)

    # define resources
    @dlt.resource(primary_key="id", write_disposition="merge")
    def products(
        updated_at: dlt.sources.incremental[str] = dlt.sources.incremental(
            "updated_at", initial_value=FIRST_DAY_OF_MILLENNIUM
        )
    ) -> Iterable[TDataItem]:
        """
        The resource for products on your shop, supports incremental loading and pagination.

        Args:
            updated_at (dlt.sources.incremental[str]): The saved state of the last 'updated_at' value.

        Returns:
            Iterable[TDataItem]: A generator of products.
        """
        page = shopify.Product.find(
            updated_at_min=updated_at.last_value, created_at_min=start_date
        )
        yield iterate_page(page)

    @dlt.resource(primary_key="id", write_disposition="merge")
    def orders(
        updated_at: dlt.sources.incremental[str] = dlt.sources.incremental(
            "updated_at", initial_value=FIRST_DAY_OF_MILLENNIUM
        )
    ) -> Iterable[TDataItem]:
        """
        The resource for orders on your shop, supports incremental loading and pagination.

        Args:
            updated_at (dlt.sources.incremental[str]): The saved state of the last 'updated_at' value.

        Returns:
            Iterable[TDataItem]: A generator of orders.
        """
        page = shopify.Order.find(
            updated_at_min=updated_at.last_value, created_at_min=start_date
        )
        yield iterate_page(page)

    @dlt.resource(primary_key="id", write_disposition="merge")
    def customers(
        updated_at: dlt.sources.incremental[str] = dlt.sources.incremental(
            "updated_at", initial_value=FIRST_DAY_OF_MILLENNIUM
        )
    ) -> Iterable[TDataItem]:
        """
        The resource for customers on your shop, supports incremental loading and pagination.

        Args:
            updated_at (dlt.sources.incremental[str]): The saved state of the last 'updated_at' value.

        Returns:
            Iterable[TDataItem]: A generator of customers.
        """
        page = shopify.Customer.find(
            updated_at_min=updated_at.last_value, created_at_min=start_date
        )
        yield iterate_page(page)

    return (products, orders, customers)

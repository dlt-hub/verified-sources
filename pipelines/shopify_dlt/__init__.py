
"""Fetches Shopify Orders and Products."""

from typing import Any, Dict, Iterator, Iterator, Optional

import shopify

import dlt

from dlt.extract.source import DltResource

FIRST_DAY_OF_MILLENNIUM = "2000-01-01"
DEFAULT_API_VERSION = "2023-01"

@dlt.source(name="shopify")
def shopify_source(
    private_app_password: str = dlt.secrets.value,
    api_version: str = DEFAULT_API_VERSION,
    shop_url: str = dlt.config.value,
    start_date: Optional[str] = FIRST_DAY_OF_MILLENNIUM,
) -> Iterator[DltResource]:
    """
    The source for the shopify pipeline. Available resources are products, orders and customers.
    @:param: private_app_password - The app password to the app on your shop
    @:param: api_version - The api version to use
    @:param: shop_url - The url of your shop
    @:param: start_date - The date from which to import items, will import items initially created on or after this date
    @:return: data_resources_list - list of dlt resources
    """

    # build client
    session = shopify.Session(shop_url, api_version, private_app_password)
    shopify.ShopifyResource.activate_session(session)

    # define resources
    @dlt.resource(primary_key="id", write_disposition="merge")
    def products(updated_at = dlt.sources.incremental("updated_at", initial_value=FIRST_DAY_OF_MILLENNIUM)) -> Iterator[Dict[str, Any]]:
        """
        The resource for products on your shop, supports incremental loading and pagination
        @:param: updated_at - The saved state of the last updated_at value
        @:return: generator of products
        """
        page = shopify.Product.find(updated_at_min=updated_at.last_value, created_at_min=start_date)
        while page:
            for product in page:
                yield product.to_dict()
            page = page.next_page() if page.has_next_page() else None

    @dlt.resource(primary_key="id", write_disposition="merge")
    def orders(updated_at = dlt.sources.incremental("updated_at", initial_value=FIRST_DAY_OF_MILLENNIUM)) -> Iterator[Dict[str, Any]]:
        """
        The resource for orders on your shop, supports incremental loading and pagination
        @:param: updated_at - The saved state of the last updated_at value
        @:return: generator of orders
        """
        page = shopify.Order.find(updated_at_min=updated_at.last_value, created_at_min=start_date)
        while page:
            for order in page:
                yield order.to_dict()
            page = page.next_page() if page.has_next_page() else None

    @dlt.resource(primary_key="id", write_disposition="merge")
    def customers(updated_at = dlt.sources.incremental("updated_at", initial_value=FIRST_DAY_OF_MILLENNIUM)) -> Iterator[Dict[str, Any]]:
        """
        The resource for customers on your shop, supports incremental loading and pagination
        @:param: updated_at - The saved state of the last updated_at value
        @:return: generator of customers
        """
        page = shopify.Customer.find(updated_at_min=updated_at.last_value, created_at_min=start_date)
        while page:
            for order in page:
                yield order.to_dict()
            page = page.next_page() if page.has_next_page() else None

    return (products,orders,customers)


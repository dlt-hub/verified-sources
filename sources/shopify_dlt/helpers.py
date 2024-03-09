"""Shopify source helpers"""
from functools import cached_property
from urllib.parse import urljoin

import dlt
from dlt.sources import DltResource
from dlt.common.time import ensure_pendulum_datetime
from dlt.sources.helpers import requests
from dlt.common.typing import TDataItem, TDataItems, Dict, DictStrAny
from dlt.common import jsonpath as jp
from typing import Any, Iterable, Optional, Literal, List

from .settings import DEFAULT_API_VERSION, DEFAULT_PARTNER_API_VERSION
from .exceptions import ShopifyPartnerApiError
from .gql_queries import schema_query, queries_query, bulk_query

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
        url = urljoin(self.shop_url, f"/admin/api/{self.api_version}/{resource}.json")

        headers = {"X-Shopify-Access-Token": self.private_app_password}
        while url:
            response = requests.get(url, params=params, headers=headers)
            response.raise_for_status()
            json = response.json()
            # Get item list from the page
            yield [self._convert_datetime_fields(item) for item in json[resource]]
            url = response.links.get("next", {}).get("url")
            # Query params are included in subsequent page URLs
            params = None

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


class ShopifyPartnerApi:
    """Client for Shopify Partner grapql API"""

    def __init__(
        self,
        access_token: str,
        organization_id: str,
        api_version: str = DEFAULT_PARTNER_API_VERSION,
    ) -> None:
        """
        Args:
            access_token: The access token to use
            organization_id: The organization id to query
            api_version: The API version to use (e.g. 2023-01)
        """
        self.access_token = access_token
        self.organization_id = organization_id
        self.api_version = api_version

    @property
    def graphql_url(self) -> str:
        return f"https://partners.shopify.com/{self.organization_id}/api/{self.api_version}/graphql.json"

    def run_graphql_query(
        self, query: str, variables: Optional[DictStrAny] = None
    ) -> DictStrAny:
        """Run a graphql query against the Shopify Partner API

        Args:
            query: The query to run
            variables: The variables to include in the query

        Returns:
            The response JSON
        """
        headers = {"X-Shopify-Access-Token": self.access_token}
        response = requests.post(
            self.graphql_url,
            json={"query": query, "variables": variables},
            headers=headers,
        )
        data = response.json()
        if data.get("errors"):
            raise ShopifyPartnerApiError(response.text)
        return data  # type: ignore[no-any-return]

    def get_graphql_pages(
        self,
        query: str,
        data_items_path: jp.TJsonPath,
        pagination_cursor_path: jp.TJsonPath,
        pagination_variable_name: str,
        variables: Optional[DictStrAny] = None,
    ) -> Iterable[TDataItems]:
        variables = dict(variables or {})
        while True:
            data = self.run_graphql_query(query, variables)
            print(data)
            data_items = jp.find_values(data_items_path, data)
            if not data_items:
                break
            yield data_items
            cursors = jp.find_values(pagination_cursor_path, data)
            if not cursors:
                break
            variables[pagination_variable_name] = cursors[-1]


class ShopifyAdminApi:
    def __init__(
        self,
        access_token: str,
        store_name: str,
        api_version: str = DEFAULT_API_VERSION,
    ) -> None:
        self.access_token = access_token
        self.store_name = store_name
        self.api_version = api_version

    @property
    def graphql_url(self) -> str:
        return f"https://{self.store_name}.myshopify.com/admin/api/{self.api_version}/graphql.json"

    def run_graphql_query(
        self, query: str, variables: Optional[DictStrAny] = None
    ) -> DictStrAny:
        """Run a graphql query against the Shopify Partner API

        Args:
            query: The query to run
            variables: The variables to include in the query

        Returns:
            The response JSON
        """
        headers = {
            "Content-Type": "application/json",
            "X-Shopify-Access-Token": self.access_token,
        }
        request_data = {"query": query, "variables": variables}

        resp = requests.post(
            url=self.graphql_url,
            headers=headers,
            json=request_data,
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()


def shopify_resource(type_def: dict, schema: dict, query: dict, client: ShopifyAdminApi) -> Iterable[TDataItem]:
    breakpoint()
    print(query["name"])
    #print(_get_fields_schema(type_def["fields"]))
    return ()


class ShopifySchemaBuilder:
    def __init__(self, shopify_admin_api_client: ShopifyAdminApi) -> None:
        self.shopify_admin_api_client = shopify_admin_api_client

    @cached_property
    def schema_gql(self) -> dict:
        """Return the schema for the stream."""
        json_resp = self.shopify_admin_api_client.run_graphql_query(schema_query)
        jsonpath = "$.data.__schema.types[*]"
        return list(iter(jp.find_values(jsonpath, json_resp)))

    def filter_queries(self, query):
        args = [a["name"] for a in query["args"]]
        return "first" in args and "query" in args

    @cached_property
    def queries_gql(self) -> dict:
        """Return the schema for the stream."""
        json_resp = self.shopify_admin_api_client.run_graphql_query(queries_query)
        items_path = "$.data.__schema.queryType.fields[*]"
        queries = jp.find_values(items_path, json_resp)
        return [q for q in queries if self.filter_queries(q)]

    def extract_gql_node(self, query: dict) -> dict:
        jsonpath = "$.type.ofType.fields[*]"
        query_fields = jp.find_values(jsonpath, query)
        return next((f for f in query_fields if f["name"] == "nodes"), None)

    def get_gql_query_type(self, node: dict) -> str:
        jsonpath = "$.type.ofType.ofType.ofType.name"
        return next(iter(jp.find_values(jsonpath, node)), None)

    def get_type_fields(self, gql_type: str) -> list[dict]:
        type_def = next(s for s in self.schema_gql if s["name"] == gql_type)

        filtered_fields = []
        for field in type_def["fields"]:
            type_kind = next(iter(jp.find_values("type.kind", field)), None)
            field_kind = next(iter(jp.find_values("type.ofType.kind", field)), None)
            if type_kind == "NON_NULL" and field_kind == "SCALAR":
                filtered_fields.append(field)
            elif type_kind == "NON_NULL" and field_kind == "OBJECT":
                filtered_fields.append(field)

        return {f["name"]: f["type"]["ofType"] for f in filtered_fields}

    def get_resources(self) -> Iterable[DltResource]:
        queries = self.queries_gql

        incremental_fields = [
            "updatedAt",
            "editedAt",
            "lastEditDate",
            "occurredAt",
            "createdAt",
            "startedAt",
        ]

        for query in queries:
            node = self.extract_gql_node(query)
            if not node:
                continue

            gql_type = self.get_gql_query_type(node)
            fields_def = self.get_type_fields(gql_type)
            type_def = next(s for s in self.schema_gql if s["name"] == gql_type)

            # Get the primary key
            pk = [k for k, v in fields_def.items() if v["name"] == "ID"]
            if not pk:
                continue

            # Get the replication key
            date_fields = [k for k, v in fields_def.items() if v["name"] == "DateTime"]
            rk = next((i for i in incremental_fields if i in date_fields), None)

            # type_def = dict(
            #     name=inflection.underscore(query["name"]),
            #     query_name=query["name"],
            #     gql_type=gql_type,
            #     primary_keys=pk,
            #     replication_key=rk,
            # )
            yield dlt.resource(
                shopify_resource(type_def=type_def, schema=self.schema_gql, query=query, client=self.shopify_admin_api_client),
                name=query["name"],
                primary_key=pk,
            )

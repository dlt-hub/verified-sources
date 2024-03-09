"""Fetches Shopify Orders and Products."""

from typing import Any, Dict, Iterator, Optional, Iterable

import dlt

from dlt.sources import DltResource
from dlt.common.typing import TDataItem, TAnyDateTime
from dlt.common.time import ensure_pendulum_datetime
from dlt.common import pendulum
from dlt.common import jsonpath as jp

from .settings import (
    DEFAULT_API_VERSION,
    FIRST_DAY_OF_MILLENNIUM,
    DEFAULT_ITEMS_PER_PAGE,
    DEFAULT_PARTNER_API_VERSION,
)
from .helpers import ShopifyPartnerApi, ShopifyAdminApi, ShopifySchemaBuilder


@dlt.source(name="shopify")
def shopify_source(
    private_app_password: str = dlt.secrets.value,
    api_version: str = DEFAULT_API_VERSION,
    shop_name: str = dlt.config.value,
    start_date: TAnyDateTime = FIRST_DAY_OF_MILLENNIUM,
    end_date: Optional[TAnyDateTime] = None,
    created_at_min: TAnyDateTime = FIRST_DAY_OF_MILLENNIUM,
    items_per_page: int = DEFAULT_ITEMS_PER_PAGE,
) -> Iterable[DltResource]:
    """
    The source for the Shopify pipeline. Available resources are products, orders, and customers.

    `start_time` argument can be used on its own or together with `end_time`. When both are provided
    data is limited to items updated in that time range.
    The range is "half-open", meaning elements equal and newer than `start_time` and elements older than `end_time` are included.
    All resources opt-in to use Airflow scheduler if run as Airflow task

    Args:
        private_app_password: The app password to the app on your shop.
        api_version: The API version to use (e.g. 2023-01).
        shop_url: The URL of your shop (e.g. https://my-shop.myshopify.com).
        items_per_page: The max number of items to fetch per page. Defaults to 250.
        start_date: Items updated on or after this date are imported. Defaults to 2000-01-01.
            If end date is not provided, this is used as the initial value for incremental loading and after the initial run, only new data will be retrieved.
            Accepts any `date`/`datetime` object or a date/datetime string in ISO 8601 format.
        end_time: The end time of the range for which to load data.
            Should be used together with `start_date` to limit the data to items updated in that time range.
            If end time is not provided, the incremental loading will be enabled and after initial run, only new data will be retrieved
        created_at_min: The minimum creation date of items to import. Items created on or after this date are loaded. Defaults to 2000-01-01.
        order_status: The order status to filter by. Can be 'open', 'closed', 'cancelled', or 'any'. Defaults to 'any'.

    Returns:
        Iterable[DltResource]: A list of DltResource objects representing the data resources.
    """
    from graphql import GraphQLObjectType, GraphQLScalarType, GraphQLList, GraphQLNonNull, GraphQLEnumType, build_client_schema
    #admin_client = ShopifyAdminApi(private_app_password, shop_name, api_version)
    #from .gql_queries import schema_query, queries_query
    #res = admin_client.run_graphql_query(schema_query)
    #data = res["data"]
    ignore_fields = [
        "image",
        "metafield",
        "metafields",
        "metafieldconnection",
        "privateMetafield",
        "privateMetafields",
    ]
    import json
    with open('shopify_schema.json') as f:
        data = json.load(f)
    schema = build_client_schema(data)

    def is_queryable(object_type) -> bool:
        query_type = schema.query_type
        for query_field in query_type.fields.values():
            if query_field.type == object_type:
                return True
        return False


    def construct_fields_query(field_type, level: int, indent="", field_name: Optional[str] = None) -> str:
        field_queries = []
        if isinstance(field_type, (GraphQLNonNull, GraphQLList)):
            field_type = field_type.of_type
            return construct_fields_query(field_type, level, indent, field_name)
        elif isinstance(field_type, GraphQLList):
            field_type = field_type.of_type
            field_queries.append(f"{indent}{field_name} {{")
        elif isinstance(field_type, GraphQLObjectType):
            if is_queryable(field_type) and field_name not in ["nodes"]:
                field_queries.append(f"{indent}{field_name} {{ id }}")
            #if level == 0:
            #    field_queries.append(f"{indent}{field_name} {{ id }}")
            elif level > 0:
                #field_queries.append(f"{indent}{field_name} {{")
                for field_name_internal, field in field_type.fields.items():
                    if field.deprecation_reason:
                        continue
                    if field_name_internal in ignore_fields:
                        continue
                    if field_name_internal in ["edges", "pageInfo"]:
                        continue
                    field_queries.append(construct_fields_query(field.type, level - 1, indent + " ", field_name_internal))
                if "Connection" not in field_type.name:
                    field_queries.insert(0, f"{indent}{field_name} {{")
                    field_queries.append(f"{indent}}}")
        elif isinstance(field_type, (GraphQLScalarType, GraphQLEnumType)):
            field_queries.append(f"{indent}{field_name}")
        return "\n".join(field_queries)

    def build_query(query_name):
        query_type = schema.query_type
        query_field = query_type.fields.get(query_name)

        fields_query = construct_fields_query(query_field.type, level = 3)
        query = f"{{\n{query_name} {{\n{fields_query}\n}}\n}}"
        return query

    #print(is_queryable(schema.get_type("MoneyV2")))
    print(build_query('customers'))
    return ()
    # schema_builder = ShopifySchemaBuilder(admin_client)

    # start_date_obj = ensure_pendulum_datetime(start_date)
    # end_date_obj = ensure_pendulum_datetime(end_date) if end_date else None
    # created_at_min_obj = ensure_pendulum_datetime(created_at_min)

    # return schema_builder.get_resources()


@dlt.resource
def shopify_partner_query(
    query: str,
    data_items_path: jp.TJsonPath,
    pagination_cursor_path: jp.TJsonPath,
    pagination_variable_name: str = "after",
    variables: Optional[Dict[str, Any]] = None,
    access_token: str = dlt.secrets.value,
    organization_id: str = dlt.config.value,
    api_version: str = DEFAULT_PARTNER_API_VERSION,
) -> Iterable[TDataItem]:
    """
    Resource for getting paginated results from the Shopify Partner GraphQL API.

    This resource will run the given GraphQL query and extract a list of data items from the result.
    It will then run the query again with a pagination cursor to get the next page of results.

    Example:
        query = '''query Transactions($after: String) {
            transactions(after: $after, first: 100) {
                edges {
                    cursor
                    node {
                        id
                    }
                }
            }
        }'''

        partner_query_pages(
            query,
            data_items_path="data.transactions.edges[*].node",
            pagination_cursor_path="data.transactions.edges[-1].cursor",
            pagination_variable_name="after",
        )

    Args:
        query: The GraphQL query to run.
        data_items_path: The JSONPath to the data items in the query result. Should resolve to array items.
        pagination_cursor_path: The JSONPath to the pagination cursor in the query result, will be piped to the next query via variables.
        pagination_variable_name: The name of the variable to pass the pagination cursor to.
        variables: Mapping of extra variables used in the query.
        access_token: The Partner API Client access token, created in the Partner Dashboard.
        organization_id: Your Organization ID, found in the Partner Dashboard.
        api_version: The API version to use (e.g. 2024-01). Use `unstable` for the latest version.
    Returns:
        Iterable[TDataItem]: A generator of the query results.
    """
    client = ShopifyPartnerApi(
        access_token=access_token,
        organization_id=organization_id,
        api_version=api_version,
    )

    yield from client.get_graphql_pages(
        query,
        data_items_path=data_items_path,
        pagination_cursor_path=pagination_cursor_path,
        pagination_variable_name=pagination_variable_name,
        variables=variables,
    )

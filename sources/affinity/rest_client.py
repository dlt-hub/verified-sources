from typing import Any

from dlt.sources.helpers.rest_client.auth import BearerTokenAuth, HttpBasicAuth
from dlt.sources.helpers.rest_client.client import RESTClient, Response
from dlt.sources.helpers.rest_client.paginators import (
    JSONLinkPaginator,
    JSONResponseCursorPaginator,
)

from .type_adapters import error_adapter
from .settings import API_BASE, V2_PREFIX


def get_v2_rest_client(api_key: str):
    return RESTClient(
        base_url=f"{API_BASE}{V2_PREFIX}",
        auth=BearerTokenAuth(api_key),
        data_selector="data",
        paginator=JSONLinkPaginator("pagination.nextUrl"),
    )


def get_v1_rest_client(api_key: str):
    return RESTClient(
        base_url=API_BASE,
        auth=HttpBasicAuth("", api_key),
        paginator=JSONResponseCursorPaginator(
            cursor_path="next_page_token", cursor_param="page_token"
        ),
    )


def raise_if_error(response: Response, *args: Any, **kwargs: Any) -> None:
    if response.status_code < 200 or response.status_code >= 300:
        error = error_adapter.validate_json(response.text)
        response.reason = "\n".join([e.message for e in error.errors])
        response.raise_for_status()


hooks = {"response": [raise_if_error]}
MAX_PAGE_LIMIT_V1 = 500
MAX_PAGE_LIMIT_V2 = 100

"""Paginated Jira endpoints loaded as tables replaced on each run: users, workflows, workflow
statuses and projects
"""

from typing import Any, Dict, Iterable, List, Optional, Set

from dlt.common import logger
from dlt.common.schema.typing import TColumnSchema
from dlt.common.typing import DictStrAny, TDataItem
from dlt.sources.helpers.requests import Response

from .client import JiraClient
from .paginators import JiraOffsetPaginator, JiraResponseError, response_body


def nullable_key_columns(primary_key: str) -> Dict[str, TColumnSchema]:
    """Hints keeping `primary_key` nullable, so a row without one lands instead of
    failing the load package."""
    return {primary_key: {"nullable": True}}


def unseen_rows(
    rows: List[TDataItem], key_field: str, seen: Set[Any]
) -> List[TDataItem]:
    """Rows whose key has not been yielded yet, adding the keys it returns to `seen`.

    A row without the field cannot be matched and is kept.
    """
    unseen = []
    for row in rows:
        key = row.get(key_field) if isinstance(row, dict) else None
        if key is None:
            unseen.append(row)
            continue
        if key in seen:
            continue
        seen.add(key)
        unseen.append(row)
    return unseen


def _any_row_without_key(rows: List[TDataItem], key_field: str) -> bool:
    return any(not isinstance(row, dict) or row.get(key_field) is None for row in rows)


def _check_data_path(api_path: str, data_path: str, response: Response) -> None:
    body = response_body(response)
    if isinstance(body, dict) and data_path in body:
        return
    got = sorted(body) if isinstance(body, dict) else type(body).__name__
    raise JiraResponseError(
        f"Jira endpoint {api_path} returned no '{data_path}' key. Got: {got}.",
        response=response,
    )


def get_paginated_data(
    subdomain: str,
    email: str,
    api_token: str,
    page_size: int,
    api_path: str,
    data_path: Optional[str] = None,
    offset_path: Optional[str] = None,
    params: Optional[DictStrAny] = None,
    key_field: Optional[str] = None,
    deduplicate: bool = False,
) -> Iterable[TDataItem]:
    """
    Function to fetch paginated data from a Jira API endpoint with `startAt` pagination.

    Args:
        subdomain: The subdomain for the Jira instance.
        email: The email to authenticate with.
        api_token: The API token to authenticate with.
        page_size: Maximum number of results per page
        api_path: The API path for the Jira endpoint.
        data_path: Optional data path to extract from the response.
        offset_path: Optional response key whose row count advances `startAt`. Defaults to
            `data_path`, and differs when the rows taken sit under a sibling key.
        params: Optional parameters for the API request.
        key_field: Optional response field holding the key. Rows missing it are logged once.
        deduplicate: If True, drops rows whose `key_field` was yielded before.
    Yields:
        Iterable[TDataItem]: Yields pages of data from the API.
    """
    if deduplicate and not key_field:
        raise ValueError("deduplicate needs a key_field to deduplicate on")
    client = JiraClient(subdomain, email, api_token)
    paginator = JiraOffsetPaginator(page_size, offset_path)
    seen: Optional[Set[Any]] = set() if deduplicate else None
    warned_on_missing_key = False
    # "$" selects the whole body for the endpoints that return a bare array
    for page in client.paginate(
        api_path, paginator, data_path or "$", params=dict(params or {})
    ):
        if data_path:
            _check_data_path(api_path, data_path, page.response)
        rows = list(page)
        if (
            key_field
            and not warned_on_missing_key
            and _any_row_without_key(rows, key_field)
        ):
            logger.warning(
                f"Jira endpoint {api_path} returned rows without '{key_field}'. They load with"
                " a null primary key and are reported once per run."
            )
            warned_on_missing_key = True
        if seen is not None and key_field:
            rows = unseen_rows(rows, key_field, seen)
        if rows:
            yield rows

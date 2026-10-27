"""Reads comments of issues fetched by the `issues` transformer"""

from typing import Dict, Iterator, List, Optional

import dlt
from dlt.common import logger
from dlt.common.schema.typing import TTableSchemaColumns
from dlt.common.time import ensure_datetime_in_tz
from dlt.common.typing import DictStrAny, TDataItem
from dlt.sources.helpers.requests import HTTPError

from .client import JiraClient
from .issues import IssuesPage, page_ids, rows_to_items
from .schema_types import (
    USER,
    TBackend,
    TShape,
    shape_to_column_schema,
    trim_to_shape,
)

COMMENT: Dict[str, TShape] = {
    "id": "text",
    "author": USER,
    "updateAuthor": USER,
    "body": "text",
    "created": "timestamp",
    "updated": "timestamp",
    "jsdPublic": "bool",
    "visibility": {"type": "text", "value": "text", "identifier": "text"},
}
COMMENT_COLUMNS: TTableSchemaColumns = {
    "id": {"name": "id", "data_type": "text", "nullable": False},
    "issueId": {"name": "issueId", "data_type": "text"},
    "created": {"name": "created", "data_type": "timestamp"},
    # the latest version of a comment read more than once in a run wins
    "updated": {"name": "updated", "data_type": "timestamp", "dedup_sort": "desc"},
    "deleted": {"name": "deleted", "data_type": "bool", "hard_delete": True},
}


def comment_columns(backend: TBackend, nested_types: bool) -> TTableSchemaColumns:
    """Returns columns of comments in the order of values in Arrow rows"""
    columns: TTableSchemaColumns = {}
    for name, shape in {**COMMENT, "issueId": "text", "deleted": "bool"}.items():
        col = shape_to_column_schema(name, shape, backend, nested_types)
        if col is not None:
            columns[name] = {**col, **COMMENT_COLUMNS.get(name, {})}
    return columns


def comment_rows(issues: TDataItem, page: IssuesPage) -> Iterator[TDataItem]:
    """Yields all comments of a page of `issues`.

    Each issue without comments gets a row with the `deleted` flag that deletes its comments
    loaded before, except on the first run of an incremental load.
    """
    reader = page.reader
    ids = page_ids(issues)
    comments = page.comments
    if comments is None:
        # `issue_comments` was renamed or there is no pipeline context
        comments = reader.fetch_comments(ids)
    rows: List[DictStrAny] = []
    for issue_id in ids:
        embedded = comments.get(issue_id)
        if embedded is None:
            continue
        issue_comments = read_comments(reader.client, issue_id, embedded)
        if issue_comments is None:
            continue
        for comment in issue_comments:
            row: DictStrAny = trim_to_shape(comment, COMMENT)
            row["issueId"] = issue_id
            for name in ("created", "updated"):
                if row.get(name):
                    row[name] = ensure_datetime_in_tz(row[name])
            rows.append(row)
        if not issue_comments and not reader.initial_load:
            rows.append(
                {"id": f"issue-{issue_id}", "issueId": issue_id, "deleted": True}
            )
    if rows:
        columns = comment_columns(reader.backend, reader.nested_types)
        yield dlt.mark.with_hints([], dlt.mark.make_hints(columns=columns))
        yield rows_to_items(rows, columns, reader.backend)


def read_comments(
    client: JiraClient, issue_id: str, embedded: DictStrAny
) -> Optional[List[DictStrAny]]:
    """Returns all comments of an issue. Jira embeds the latest 20 comments in an issue, all
    comments are read again when there are more. Returns None if the issue was deleted.
    """
    comments: List[DictStrAny] = embedded.get("comments") or []
    if len(comments) >= (embedded.get("total") or 0):
        return comments
    try:
        return client.comments(issue_id)
    except HTTPError as http_ex:
        if http_ex.response is None or http_ex.response.status_code != 404:
            raise
        logger.warning(f"Skipping comments of deleted Jira issue `{issue_id}`")
        return None

"""Reads Jira worklogs changed and deleted since the last run"""

from datetime import datetime, timezone
from typing import Any, Iterator, List, Optional

from dlt.common.schema.typing import TTableSchemaColumns
from dlt.common.time import datetime_to_timestamp_ms, ensure_datetime_in_tz
from dlt.common.typing import DictStrAny, TDataItem
from dlt.extract.incremental import Incremental

from .client import JiraClient
from .issues import batched
from .schema_types import TShape, trim_to_shape
from ..settings import (
    MAX_WORKLOG_IDS,
    WORKLOG_DELETED_PATH,
    WORKLOG_LIST_PATH,
    WORKLOG_UPDATED_PATH,
)

AUTHOR: TShape = {"accountId": "text", "displayName": "text"}
WORKLOG: TShape = {
    "id": "text",
    "issueId": "text",
    "author": AUTHOR,
    "updateAuthor": AUTHOR,
    "comment": "text",
    "created": "text",
    "updated": "text",
    "started": "text",
    "timeSpentSeconds": "bigint",
}
WORKLOG_DATETIMES = ("created", "updated", "started")
WORKLOG_COLUMNS: TTableSchemaColumns = {
    "id": {"name": "id", "data_type": "text", "nullable": False},
    "issueId": {"name": "issueId", "data_type": "text"},
    **{name: {"name": name, "data_type": "timestamp"} for name in WORKLOG_DATETIMES},
    "timeSpentSeconds": {"name": "timeSpentSeconds", "data_type": "bigint"},
    # set on deleted worklogs, which are removed from the destination on merge
    "deleted": {"name": "deleted", "data_type": "bool", "hard_delete": True},
}
# the latest change of a worklog read more than once in a run wins
WORKLOG_COLUMNS["updated"]["dedup_sort"] = "desc"


def changed_worklogs(
    client: JiraClient, path: str, since: int
) -> Iterator[List[DictStrAny]]:
    """Pages through ids of worklogs changed after `since` (UNIX timestamp in milliseconds)"""
    while True:
        page = client.get_json(path, params={"since": since})
        values: List[DictStrAny] = page.get("values") or []
        if values:
            yield values
        if page.get("lastPage", True):
            return
        until: int = page["until"]
        if until <= since:
            # Jira pages by the youngest timestamp of a page
            raise RuntimeError(
                f"Cannot page Jira worklogs changed at {since}: more than {MAX_WORKLOG_IDS}"
                " worklogs share the same timestamp"
            )
        since = until


def to_worklog_row(worklog: DictStrAny) -> DictStrAny:
    row: DictStrAny = trim_to_shape(worklog, WORKLOG)
    for name in WORKLOG_DATETIMES:
        if row.get(name):
            row[name] = ensure_datetime_in_tz(row[name])
    return row


def worklog_rows(
    client: JiraClient, incremental: Optional[Incremental[Any]] = None
) -> Iterator[TDataItem]:
    """Yields worklogs changed since the start of `incremental`, then the deleted worklogs"""
    start_value = incremental.get_current_range()[0] if incremental else None
    since = (
        0
        if start_value is None
        else datetime_to_timestamp_ms(ensure_datetime_in_tz(start_value))
    )
    for changed in changed_worklogs(client, WORKLOG_UPDATED_PATH, since):
        for ids in batched([item["worklogId"] for item in changed], MAX_WORKLOG_IDS):
            worklogs = client.post_json(WORKLOG_LIST_PATH, {"ids": ids})
            yield [to_worklog_row(worklog) for worklog in worklogs]
    for deleted in changed_worklogs(client, WORKLOG_DELETED_PATH, since):
        yield [
            {
                "id": str(item["worklogId"]),
                "updated": datetime.fromtimestamp(
                    item["updatedTime"] / 1000, timezone.utc
                ),
                "deleted": True,
            }
            for item in deleted
        ]

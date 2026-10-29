"""Reads Jira worklogs changed and deleted since the last run"""

from datetime import datetime, timezone
from typing import Any, Iterator, Optional

from dlt.common.schema.typing import TTableSchemaColumns
from dlt.common.time import datetime_to_timestamp_ms, ensure_datetime_in_tz
from dlt.common.typing import TDataItem
from dlt.extract.incremental import Incremental

from .client import JiraClient, batched
from .schema_types import convert_record
from ..settings import MAX_WORKLOG_IDS, WORKLOG_DELETED_PATH, WORKLOG_UPDATED_PATH

WORKLOG_TYPES = {
    "created": "timestamp",
    "updated": "timestamp",
    "started": "timestamp",
    "timeSpentSeconds": "bigint",
}
WORKLOG_COLUMNS: TTableSchemaColumns = {
    # the latest change of a worklog read more than once in a run wins
    "updated": {"dedup_sort": "desc"},
    # set on deleted worklogs, which are removed from the destination on merge
    "deleted": {"data_type": "bool", "hard_delete": True},
}


def worklog_rows(
    client: JiraClient,
    incremental: Optional[Incremental[Any]] = None,
    read_deleted: bool = True,
) -> Iterator[TDataItem]:
    """Yields worklogs of all issues of the site changed since the start of `incremental`, then
    the deleted worklogs if `read_deleted`
    """
    start_value = incremental.get_current_range()[0] if incremental else None
    since = (
        0
        if start_value is None
        else datetime_to_timestamp_ms(ensure_datetime_in_tz(start_value))
    )
    for changed in client.changed_worklogs(WORKLOG_UPDATED_PATH, since):
        for ids in batched([item["worklogId"] for item in changed], MAX_WORKLOG_IDS):
            yield [
                convert_record(worklog, WORKLOG_TYPES)
                for worklog in client.worklogs(ids)
            ]
    if not read_deleted:
        return
    for deleted in client.changed_worklogs(WORKLOG_DELETED_PATH, since):
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

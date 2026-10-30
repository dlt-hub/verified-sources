"""Reads comments of issues fetched by the `issues` transformer"""

from typing import Iterator, List, Optional

from dlt.common import logger
from dlt.common.schema.typing import TTableSchemaColumns
from dlt.common.typing import DictStrAny, TDataItem
from dlt.sources.helpers.requests import HTTPError

from .client import JiraClient, batched
from .issues import IssuesPage, IssuesReader, is_replace
from .schema_types import convert_record

COMMENT_TYPES = {"created": "timestamp", "updated": "timestamp"}
COMMENT_COLUMNS: TTableSchemaColumns = {
    # the latest version of a comment read more than once in a run wins
    "updated": {"dedup_sort": "desc"},
    # typed, since the first run writes no rows that delete comments
    "deleted": {"data_type": "bool", "hard_delete": True},
}


def comment_rows(issues: List[DictStrAny], page: IssuesPage) -> Iterator[TDataItem]:
    """Yields all comments of a page of `issues` in pages of `chunk_size` comments.

    The `comment` field of the issues is bulk fetched, an issue with more comments than Jira
    embeds is read again with the comment endpoint. Each issue without comments gets a row with
    the `deleted` flag that deletes its comments loaded before, except on the first run of an
    incremental load and when the issues replace their table.
    """
    yield from batched(_comment_rows(issues, page.reader), page.reader.chunk_size)


def _comment_rows(
    issues: List[DictStrAny], reader: IssuesReader
) -> Iterator[DictStrAny]:
    ids = [issue["id"] for issue in issues]
    comments = reader.fetch_comments(ids)
    for issue_id in ids:
        embedded = comments.get(issue_id)
        if embedded is None:
            continue
        issue_comments = read_comments(reader.client, issue_id, embedded)
        if issue_comments is None:
            continue
        for comment in issue_comments:
            row = convert_record(comment, COMMENT_TYPES)
            row["issueId"] = issue_id
            yield row
        if (
            not issue_comments
            and not reader.initial_load
            and not is_replace(reader.write_disposition)
        ):
            yield {"id": f"issue-{issue_id}", "issueId": issue_id, "deleted": True}


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

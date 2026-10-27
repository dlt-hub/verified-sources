import re
from datetime import datetime, timezone
from typing import Dict, Optional

import requests_mock

from sources.jira import issue_comments, jira_issues

from tests.jira.utils import (
    CREDENTIALS,
    DEFAULT_FIELD_IDS,
    JiraMock,
    UPDATED_1,
    UPDATED_2,
    duckdb_pipeline,
    issues_updated_at,
    load,
    make_comment,
    rows_by_id,
)

VISIBILITY = {"type": "role", "value": "Administrators", "identifier": "10002"}


def test_issue_comments(jira_mock: JiraMock, http_mock: requests_mock.Mocker) -> None:
    jira_mock.add_issues(*issues_updated_at((1, 2, 3)))
    jira_mock.comments = {
        "1": [
            make_comment(10, UPDATED_1),
            make_comment(11, UPDATED_2, visibility=VISIBILITY),
        ],
        "3": [make_comment(comment_id, UPDATED_1) for comment_id in range(30, 35)],
    }
    # issue 3 has more comments than embedded in the issue
    jira_mock.embedded_comments = 2
    jira_mock.comment_page_size = 2
    issues = jira_issues(**CREDENTIALS)
    pipeline = duckdb_pipeline("jira_comments")
    load(pipeline, [issues, issues | issue_comments])

    # the comment field of a page of issues is fetched after the issues
    assert [fetch["fields"] for fetch in jira_mock.bulk_fetches] == [
        DEFAULT_FIELD_IDS,
        ["comment"],
    ]
    assert "comment" not in pipeline.default_schema.get_table_columns("issues")
    # comments of issue 3 are paged
    assert [
        (fetch["issueId"], fetch["startAt"]) for fetch in jira_mock.comment_fetches
    ] == [("3", "0"), ("3", "2"), ("3", "4")]
    table = pipeline.default_schema.get_table("issue_comments")
    assert table["write_disposition"] == "merge"
    columns = table["columns"]
    assert columns["id"]["primary_key"] is True
    assert columns["issue_id"]["merge_key"] is True
    assert columns["created"]["data_type"] == "timestamp"
    assert columns["updated"]["dedup_sort"] == "desc"
    assert columns["body"]["data_type"] == "text"
    assert columns["jsd_public"]["data_type"] == "bool"
    assert columns["deleted"]["hard_delete"] is True
    rows = rows_by_id(pipeline, "issue_comments")
    expected_rows = [
        ("10", "1"),
        ("11", "1"),
        *((str(comment_id), "3") for comment_id in range(30, 35)),
    ]
    assert [(row["id"], row["issue_id"]) for row in rows] == expected_rows
    # the first run writes no row deleting the comments of issue 2
    assert pipeline.last_trace.last_normalize_info.row_counts["issue_comments"] == 7
    assert rows[1]["updated"] == datetime(2024, 1, 16, 16, 0, tzinfo=timezone.utc)
    assert rows[1]["body"] == "comment 11 with *wiki markup*"
    assert rows[1]["jsd_public"] is True
    assert rows[1]["author__account_id"] == "a1"
    assert rows[1]["visibility__value"] == "Administrators"
    assert rows[0]["visibility__value"] is None

    # the next run writes the row deleting the comments of issue 2 and skips the comments of
    # issue 3, deleted since its page of issues was fetched
    http_mock.get(
        re.compile(r"/rest/api/2/issue/3/comment"),
        status_code=404,
        json={"errorMessages": ["Issue does not exist"]},
    )
    issues = jira_issues(**CREDENTIALS)
    load(pipeline, [issues, issues | issue_comments])
    assert pipeline.last_trace.last_normalize_info.row_counts["issue_comments"] == 3
    assert [
        (row["id"], row["issue_id"]) for row in rows_by_id(pipeline, "issue_comments")
    ] == expected_rows


def test_issue_comments_edits_and_deletes(jira_mock: JiraMock) -> None:
    jira_mock.add_issues(*issues_updated_at((1, 2, 3)))
    jira_mock.comments = {
        "1": [make_comment(10, UPDATED_1), make_comment(11, UPDATED_1)],
        "2": [make_comment(20, UPDATED_1)],
    }
    pipeline = duckdb_pipeline("jira_comments_changed")

    def load_bodies() -> Dict[str, Optional[str]]:
        issues = jira_issues(**CREDENTIALS)
        load(pipeline, issues | issue_comments)
        return {
            row["id"]: row["body"] for row in rows_by_id(pipeline, "issue_comments")
        }

    assert load_bodies() == {
        "10": "comment 10 with *wiki markup*",
        "11": "comment 11 with *wiki markup*",
        "20": "comment 20 with *wiki markup*",
    }
    assert pipeline.last_trace.last_normalize_info.row_counts["issue_comments"] == 3

    # comment 10 is edited, comment 11 is deleted, issue 2 has no comments left and issue 3 gets
    # a comment, issues are updated within lag and read again
    jira_mock.comments = {
        "1": [make_comment(10, UPDATED_2, body="edited")],
        "3": [make_comment(30, UPDATED_2)],
    }
    assert load_bodies() == {"10": "edited", "30": "comment 30 with *wiki markup*"}
    # issue 2 without comments deletes its comments
    assert pipeline.last_trace.last_normalize_info.row_counts["issue_comments"] == 3

    # nothing changed and no lag: the issues updated last are read once more, the incremental
    # records them and the next run extracts nothing
    expected = {"10": "edited", "30": "comment 30 with *wiki markup*"}
    pipeline.run(jira_issues(**CREDENTIALS, lag=0) | issue_comments)
    assert pipeline.last_trace.last_normalize_info.row_counts["issue_comments"] == 3
    pipeline.run(jira_issues(**CREDENTIALS, lag=0) | issue_comments)
    assert "issue_comments" not in pipeline.last_trace.last_normalize_info.row_counts
    assert {
        row["id"]: row["body"] for row in rows_by_id(pipeline, "issue_comments")
    } == expected

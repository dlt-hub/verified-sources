import json
from datetime import datetime, timezone
from typing import Dict, Optional

import dlt
import pytest
import requests_mock

from sources.jira import issue_comments, jira_issues

from tests.jira.utils import (
    CREDENTIALS,
    DEFAULT_FIELD_IDS,
    JiraMock,
    UPDATED_1,
    UPDATED_2,
    load_captured,
    make_comment,
    make_issue,
    run_to_parquet,
    to_rows,
)
from tests.utils import assert_load_info, load_tables_to_dicts


VISIBILITY = {"type": "role", "value": "Administrators", "identifier": "10002"}


@pytest.mark.parametrize("backend", ["python", "pyarrow"])
def test_issue_comments(jira_mock: JiraMock, backend: str) -> None:
    jira_mock.add_issues(*(make_issue(issue_id, UPDATED_1) for issue_id in (1, 2, 3)))
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
    issues = jira_issues(**CREDENTIALS, backend=backend)
    pipeline = dlt.pipeline(
        f"jira_comments_{backend}", destination="duckdb", dev_mode=True
    )
    assert_load_info(pipeline.run([issues, issues | issue_comments]))

    # comments are fetched with the issues
    assert [fetch["fields"] for fetch in jira_mock.bulk_fetches] == [
        [*DEFAULT_FIELD_IDS, "comment"]
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
    assert columns["deleted"]["hard_delete"] is True
    rows = sorted(
        load_tables_to_dicts(pipeline, "issue_comments")["issue_comments"],
        key=lambda r: int(r["id"]),
    )
    # the first run does not delete comments of issues without comments
    assert [(row["id"], row["issue_id"]) for row in rows] == [
        ("10", "1"),
        ("11", "1"),
        *((str(comment_id), "3") for comment_id in range(30, 35)),
    ]
    assert rows[1]["updated"] == datetime(2024, 1, 16, 16, 0, tzinfo=timezone.utc)
    assert rows[1]["body"] == "comment 11 with *wiki markup*"
    assert rows[1]["jsd_public"] is True
    if backend == "python":
        assert rows[1]["author__account_id"] == "a1"
        assert rows[1]["visibility__value"] == "Administrators"
        assert rows[0]["visibility__value"] is None
    else:
        assert columns["visibility"]["data_type"] == "json"
        with pipeline.sql_client() as client:
            visibility = client.execute_sql(
                "SELECT visibility FROM issue_comments WHERE id = '11'"
            )[0][0]
        assert json.loads(visibility) == VISIBILITY
        # and as nested types on destinations that support them
        issues = jira_issues(**CREDENTIALS, backend="pyarrow", nested_types=True)
        _, table = run_to_parquet(
            "jira_comments_nested", issues | issue_comments, "issue_comments"
        )
        assert str(table.schema.field("visibility").type) == (
            "struct<type: string, value: string, identifier: string>"
        )
        rows = {row["id"]: row for row in table.to_pylist()}
        assert rows["11"]["visibility"] == VISIBILITY


def test_issue_comments_captured(http_mock: requests_mock.Mocker) -> None:
    captured = load_captured()
    jira_mock = JiraMock(http_mock)
    jira_mock.add_issues(
        *(make_issue(int(issue_id), UPDATED_1) for issue_id in captured["comments"])
    )
    jira_mock.comments = captured["comments"]
    pipeline = dlt.pipeline(
        "jira_comments_captured", destination="duckdb", dev_mode=True
    )
    issues = jira_issues(**CREDENTIALS)
    assert_load_info(pipeline.run(issues | issue_comments))

    rows = sorted(
        load_tables_to_dicts(pipeline, "issue_comments")["issue_comments"],
        key=lambda r: r["id"],
    )
    assert [(row["id"], row["issue_id"]) for row in rows] == [
        ("10009", "10293"),
        ("10010", "10293"),
    ]
    # wiki markup body of api v2
    assert rows[0]["body"] == (
        "h2. Seeded\n*bold* comment with emoji 🚀, ünïcode and a\nnew line"
    )
    # the edited comment
    assert rows[1]["body"] == "after edit"
    assert rows[1]["updated"] > rows[1]["created"]
    assert rows[1]["created"] == datetime(
        2026, 10, 27, 1, 38, 39, 945000, tzinfo=timezone.utc
    )
    assert rows[0]["author__account_id"] == "account-1"
    assert rows[0]["update_author__display_name"] == "User account-1"


def test_issue_comments_renamed(jira_mock: JiraMock) -> None:
    jira_mock.add_issues(make_issue(1, UPDATED_1))
    jira_mock.comments = {"1": [make_comment(10, UPDATED_1)]}
    issues = jira_issues(**CREDENTIALS)
    comments = issues | issue_comments.with_name("comments")

    rows = to_rows(list(comments))

    # comments are fetched separately when the transformer is renamed
    assert [fetch["fields"] for fetch in jira_mock.bulk_fetches] == [
        DEFAULT_FIELD_IDS,
        ["comment"],
    ]
    assert [row["id"] for row in rows] == ["10"]


@pytest.mark.parametrize("backend", ["python", "pyarrow"])
def test_issue_comments_changed(jira_mock: JiraMock, backend: str) -> None:
    jira_mock.add_issues(*(make_issue(issue_id, UPDATED_1) for issue_id in (1, 2, 3)))
    jira_mock.comments = {
        "1": [make_comment(10, UPDATED_1), make_comment(11, UPDATED_1)],
        "2": [make_comment(20, UPDATED_1)],
    }
    pipeline = dlt.pipeline(
        f"jira_comments_changed_{backend}", destination="duckdb", dev_mode=True
    )

    def load() -> Dict[str, Optional[str]]:
        issues = jira_issues(**CREDENTIALS, backend=backend)
        assert_load_info(pipeline.run(issues | issue_comments))
        rows = load_tables_to_dicts(pipeline, "issue_comments")["issue_comments"]
        return {row["id"]: row["body"] for row in rows}

    assert load() == {
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
    assert load() == {"10": "edited", "30": "comment 30 with *wiki markup*"}
    # issue 2 without comments deletes its comments
    assert pipeline.last_trace.last_normalize_info.row_counts["issue_comments"] == 3


def test_comments_in_pages_of_chunk_size(jira_mock: JiraMock) -> None:
    jira_mock.add_issues(make_issue(1, UPDATED_1), make_issue(2, UPDATED_1))
    jira_mock.comments = {
        "1": [make_comment(comment_id, UPDATED_1) for comment_id in range(10, 13)],
        "2": [make_comment(20, UPDATED_1)],
    }
    issues = jira_issues(**CREDENTIALS, backend="pyarrow", chunk_size=3)
    pages = list(issues | issue_comments)
    # comments of the issues in a page are yielded in pages of `chunk_size`
    assert [page.num_rows for page in pages] == [3, 1]

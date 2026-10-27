from datetime import date, datetime, timezone
from typing import Any, Optional, Type

import dlt
import pytest
import requests_mock
from dlt.extract.exceptions import ResourceExtractionError

from sources.jira import jira_issues

from tests.jira.utils import CREDENTIALS, JiraMock, START


@pytest.mark.parametrize(
    "incremental,time_zone,jql,expected_jql",
    [
        ("auto", "UTC", None, 'created >= "1970-01-01" ORDER BY updated ASC'),
        (None, "UTC", None, 'created >= "1970-01-01"'),
        (
            dlt.sources.incremental("updated", initial_value=START),
            "UTC",
            None,
            'updated >= "2024-01-15 09:30" ORDER BY updated ASC',
        ),
        (
            dlt.sources.incremental("updated", initial_value=START),
            "America/Los_Angeles",
            None,
            'updated >= "2024-01-15 01:30" ORDER BY updated ASC',
        ),
        (
            dlt.sources.incremental("updated", initial_value=START),
            "Asia/Kolkata",
            None,
            'updated >= "2024-01-15 15:00" ORDER BY updated ASC',
        ),
        (
            dlt.sources.incremental(
                "updated",
                initial_value=START,
                end_value=datetime(2024, 1, 16, 12, 0, 30, tzinfo=timezone.utc),
            ),
            "UTC",
            None,
            'updated >= "2024-01-15 09:30" AND updated < "2024-01-16 12:01" ORDER BY updated ASC',
        ),
        (
            dlt.sources.incremental(
                "updated", initial_value=START, last_value_func=min
            ),
            "UTC",
            None,
            'updated < "2024-01-15 09:31" ORDER BY updated DESC',
        ),
        (
            dlt.sources.incremental("updated", initial_value=START, row_order="asc"),
            "UTC",
            None,
            'updated >= "2024-01-15 09:30" ORDER BY updated ASC',
        ),
        (
            dlt.sources.incremental("updated", initial_value=START, row_order="desc"),
            "UTC",
            None,
            'updated >= "2024-01-15 09:30" ORDER BY updated DESC',
        ),
        (
            # row order is relative to the cursor progress, with min "asc" reads the newest first
            dlt.sources.incremental(
                "updated", initial_value=START, last_value_func=min, row_order="asc"
            ),
            "UTC",
            None,
            'updated < "2024-01-15 09:31" ORDER BY updated DESC',
        ),
        (
            dlt.sources.incremental(
                "updated", initial_value=START, on_cursor_value_missing="include"
            ),
            "UTC",
            None,
            '(updated >= "2024-01-15 09:30" OR updated is EMPTY) ORDER BY updated ASC',
        ),
        (
            dlt.sources.incremental(
                "updated", initial_value=START, on_cursor_value_missing="exclude"
            ),
            "UTC",
            None,
            'updated >= "2024-01-15 09:30" AND updated is not EMPTY ORDER BY updated ASC',
        ),
        (
            dlt.sources.incremental("customfield_10033", initial_value=START),
            "UTC",
            None,
            'cf[10033] >= "2024-01-15 09:30" ORDER BY cf[10033] ASC',
        ),
        (
            dlt.sources.incremental(
                "duedate", initial_value=date(2024, 2, 1), end_value=date(2024, 3, 1)
            ),
            "UTC",
            None,
            'duedate >= "2024-02-01" AND duedate <= "2024-03-01" ORDER BY duedate ASC',
        ),
        (
            dlt.sources.incremental(
                "customfield_10032", initial_value=date(2024, 3, 1)
            ),
            "UTC",
            None,
            'cf[10032] >= "2024-03-01" ORDER BY cf[10032] ASC',
        ),
        (
            dlt.sources.incremental(
                "updated", last_value_func=lambda values: values[-1]
            ),
            "UTC",
            None,
            'created >= "1970-01-01"',
        ),
        ("auto", "UTC", "project = KAN", "(project = KAN) ORDER BY updated ASC"),
        (
            dlt.sources.incremental("updated", initial_value=START),
            "UTC",
            "project = KAN OR labels = x",
            '(project = KAN OR labels = x) AND updated >= "2024-01-15 09:30"'
            " ORDER BY updated ASC",
        ),
    ],
    ids=[
        "auto_first_run",
        "no_incremental",
        "start",
        "time_zone_negative",
        "time_zone_half_hour",
        "end",
        "min",
        "row_order_asc_max",
        "row_order_desc_max",
        "row_order_asc_min",
        "include_missing",
        "exclude_missing",
        "custom_field_cursor",
        "date_cursor",
        "custom_date_cursor",
        "custom_last_value_func",
        "user_jql",
        "user_jql_with_start",
    ],
)
def test_issues_jql(
    http_mock: requests_mock.Mocker,
    incremental: Any,
    time_zone: Optional[str],
    jql: Optional[str],
    expected_jql: str,
) -> None:
    jira_mock = JiraMock(http_mock, time_zone=time_zone)
    assert list(jira_issues(**CREDENTIALS, incremental=incremental, jql=jql)) == []
    assert jira_mock.jqls == [expected_jql]
    # search returns values of the cursor field for the incremental
    cursor = (
        "updated"
        if incremental == "auto"
        else getattr(incremental, "cursor_path", "id")
    )
    assert jira_mock.searches[0]["fields"] == cursor
    assert jira_mock.searches[0]["maxResults"] == "5000"
    # time zone is needed to render datetimes only
    assert jira_mock.myself_route.called == (incremental is not None)


@pytest.mark.parametrize(
    "cursor_path,exception",
    [
        ("summary", ValueError),
        ("resolutiondate", KeyError),
        ("fields.updated", ValueError),
    ],
    ids=["not_a_date", "unknown_field", "nested_path"],
)
def test_issues_invalid_cursor(
    jira_mock: JiraMock, cursor_path: str, exception: Type[Exception]
) -> None:
    resource = jira_issues(
        **CREDENTIALS, incremental=dlt.sources.incremental(cursor_path)
    )
    with pytest.raises(ResourceExtractionError) as py_ex:
        list(resource)
    assert isinstance(py_ex.value.__cause__, exception)
    assert not jira_mock.search_route.called


def test_issues_jql_order_by() -> None:
    # issues are ordered by the cursor
    with pytest.raises(ValueError):
        jira_issues(**CREDENTIALS, jql="project = KAN order  by created")

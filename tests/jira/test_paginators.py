from typing import Any, List

import pytest
import requests_mock
from dlt.extract.exceptions import ResourceExtractionError

from sources.jira import worklogs
from sources.jira.helpers.paginators import JiraResponseError

from tests.jira.utils import (
    CREDENTIALS,
    JiraMock,
    T1,
    cursor_pages,
    items,
    make_worklog,
    read_endpoint,
    sent_params,
    serve,
)


def test_cursor_paginator() -> None:
    path = "rest/api/3/search/jql"
    # SearchAndReconcileResults: nextPageToken is omitted on the last page only
    with requests_mock.Mocker() as m:
        serve(
            m,
            path,
            {"issues": [{"id": "1"}], "nextPageToken": "t1"},
            {"issues": [], "nextPageToken": "t2"},
            {"issues": [{"id": "2"}]},
        )
        assert [i["id"] for i in items(cursor_pages(path))] == ["1", "2"]
        assert [p.get("nextPageToken") for p in sent_params(m)] == [None, "t1", "t2"]

    # `isLast` stops even with a token
    with requests_mock.Mocker() as m:
        serve(m, path, {"issues": [{"id": "1"}], "nextPageToken": "t1", "isLast": True})
        assert [i["id"] for i in items(cursor_pages(path))] == ["1"]
        assert m.call_count == 1

    with requests_mock.Mocker() as m:
        serve(m, path, [{"id": "1"}])
        with pytest.raises(JiraResponseError, match=f"{path}.*list"):
            list(cursor_pages(path))


# pages served, endpoint config, ids read and `startAt` of each request
OFFSET_CASES: List[Any] = [
    (
        "one complete page costs one request",
        [{"values": [{"id": "p1"}], "total": 1, "isLast": True}, {"values": []}],
        {"data_path": "values"},
        ["p1"],
        ["0"],
    ),
    (
        "follows isLast false",
        [
            {"values": [{"id": "w1"}, {"id": "w2"}], "total": 3, "isLast": False},
            {"values": [{"id": "w3"}], "total": 3, "isLast": True},
            {"values": []},
        ],
        {"data_path": "values"},
        ["w1", "w2", "w3"],
        ["0", "2"],
    ),
    (
        "stops on total without isLast",
        [
            {"values": [{"id": "w1"}, {"id": "w2"}], "total": 3},
            {"values": [{"id": "w3"}], "total": 3},
            {"values": []},
        ],
        {"data_path": "values"},
        ["w1", "w2", "w3"],
        ["0", "2"],
    ),
    (
        # project search clamps maxResults to 100, stepping by the requested size skips rows
        "advances by rows when Jira clamps the page size",
        [
            {"values": [{"id": "p1"}], "total": 2},
            {"values": [{"id": "p2"}], "total": 2},
        ],
        {"data_path": "values", "page_size": 500},
        ["p1", "p2"],
        ["0", "1"],
    ),
    (
        "stops on an empty page with isLast false",
        [{"values": [], "total": 5, "isLast": False}],
        {"data_path": "values"},
        [],
        ["0"],
    ),
    (
        # users are a bare array without envelope
        "bare array stops on an empty page",
        [[{"id": "a"}, {"id": "b"}], []],
        {},
        ["a", "b"],
        ["0", "2"],
    ),
    (
        # workflow statuses are taken from `statuses`, `startAt` counts `values`
        "advances on the sibling key",
        [
            {
                "values": [{"id": "w1"}, {"id": "w2"}],
                "statuses": [{"id": "s1"}],
                "total": 3,
            },
            {"values": [{"id": "w3"}], "statuses": [{"id": "s2"}], "total": 3},
        ],
        {"data_path": "statuses", "offset_path": "values"},
        ["s1", "s2"],
        ["0", "2"],
    ),
    (
        "keeps paging past a page with no rows taken",
        [
            {"values": [{"id": "w1"}], "statuses": [], "total": 2},
            {"values": [{"id": "w2"}], "statuses": [{"id": "s1"}], "total": 2},
        ],
        {"data_path": "statuses", "offset_path": "values"},
        ["s1"],
        ["0", "1"],
    ),
]


def test_offset_paginator() -> None:
    path = "rest/api/3/workflows/search"
    for case, pages, endpoint, expected_ids, expected_starts in OFFSET_CASES:
        with requests_mock.Mocker() as m:
            serve(m, path, *pages)
            data = items(read_endpoint(path, **endpoint))
            assert [i["id"] for i in data] == expected_ids, case
            assert [p["startAt"] for p in sent_params(m)] == expected_starts, case


def test_worklog_feed_paginator_does_not_advance(jira_mock: JiraMock) -> None:
    jira_mock.worklogs = {
        str(worklog_id): make_worklog(worklog_id, 1, T1) for worklog_id in (1, 2, 3)
    }
    jira_mock.worklog_page_size = 2
    with pytest.raises(ResourceExtractionError) as py_ex:
        list(worklogs(**CREDENTIALS))
    assert isinstance(py_ex.value.__cause__, RuntimeError)

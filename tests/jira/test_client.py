from typing import List

import pytest
import requests_mock
from dlt.extract.exceptions import ResourceExtractionError
from dlt.sources.helpers.requests import HTTPError

from sources.jira import jira_issues
from sources.jira.helpers import client as jira_client
from sources.jira.helpers.paginators import JiraResponseError

from tests.jira.utils import (
    BASE_URL,
    CREDENTIALS,
    JiraMock,
    UPDATED_1,
    items,
    make_issue,
    read_endpoint,
    serve,
    to_rows,
    url_for,
)


def test_errors(http_mock: requests_mock.Mocker) -> None:
    # Jira answers an invalid or unbounded JQL query with 400 and the reason in the body
    JiraMock(http_mock)
    http_mock.get(
        BASE_URL + "rest/api/2/search/jql",
        status_code=400,
        json={"errorMessages": ["Field 'colour' does not exist"], "errors": {}},
    )
    with pytest.raises(ResourceExtractionError) as py_ex:
        list(jira_issues(**CREDENTIALS))
    http_ex = py_ex.value.__cause__
    assert isinstance(http_ex, JiraResponseError)
    # callers handle Jira errors as HTTP errors with the response
    assert isinstance(http_ex, HTTPError)
    assert http_ex.response.status_code == 400
    assert "Field 'colour' does not exist" in str(http_ex)

    # errors without messages are raised with the status
    http_mock.get(url_for("rest/api/3/project/search"), status_code=403, json={})
    with pytest.raises(HTTPError, match="403"):
        list(read_endpoint("rest/api/3/project/search", data_path="values"))


def test_retries_throttling(http_mock: requests_mock.Mocker) -> None:
    jira_mock = JiraMock(http_mock)
    jira_mock.add_issues(make_issue(1, UPDATED_1))
    http_mock.post(
        BASE_URL + "rest/api/2/issue/bulkfetch",
        [
            {"status_code": 429, "headers": {"Retry-After": "0"}},
            {"status_code": 503, "headers": {"Retry-After": "0"}},
            {"json": {"issues": [make_issue(1, UPDATED_1)], "issueErrors": []}},
        ],
    )
    rows = to_rows(list(jira_issues(**CREDENTIALS)))
    assert [row["id"] for row in rows] == ["1"]


def test_paginate_endpoint(monkeypatch: pytest.MonkeyPatch) -> None:
    path = "rest/api/3/workflows/search"
    # a removed endpoint answers with an error envelope or another body
    for payload, match in (
        ({"errorMessages": ["Removed."]}, "'values'.*errorMessages"),
        ([{"id": "w1"}], "'values'.*list"),
    ):
        with requests_mock.Mocker() as m:
            serve(m, path, payload)
            with pytest.raises(JiraResponseError, match=f"{path}.*{match}"):
                list(read_endpoint(path, data_path="values"))

    # rows without the key are reported once
    warnings: List[str] = []
    monkeypatch.setattr(jira_client.logger, "warning", warnings.append)
    with requests_mock.Mocker() as m:
        serve(
            m,
            "rest/api/3/users",
            [{"id": "a"}, {"name": "Former"}],
            [{"name": "Also"}],
            [],
        )
        assert len(items(read_endpoint("rest/api/3/users"))) == 3
    assert len(warnings) == 1
    assert "without 'id'" in warnings[0]

    # workflows that share a status repeat it, statuses without a key are kept
    with requests_mock.Mocker() as m:
        serve(
            m,
            path,
            {
                "values": [{"id": "w1"}],
                "statuses": [{"id": "s1"}, {"id": "s2"}],
                "total": 2,
            },
            {
                "values": [{"id": "w2"}],
                "statuses": [{"id": "s2"}, {"name": "x"}],
                "total": 2,
            },
        )
        statuses = items(
            read_endpoint(
                path, data_path="statuses", offset_path="values", deduplicate=True
            )
        )
    assert [status.get("id") for status in statuses] == ["s1", "s2", None]

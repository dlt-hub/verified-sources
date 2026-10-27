from typing import Any

import pytest
import requests_mock
from pytest_mock import MockerFixture

from sources.jira.helpers.client import JiraClient
from sources.jira.helpers.paginators import JiraResponseError

from tests.jira.utils import (
    CREDENTIALS,
    UPDATED_1,
    items,
    make_issue,
    read_endpoint,
    serve,
    url_for,
)


@pytest.mark.parametrize(
    "payload, match",
    [
        pytest.param(
            {"errorMessages": ["Removed."]},
            "'values'.*errorMessages",
            id="error envelope",
        ),
        pytest.param([{"id": "w1"}], "'values'.*list", id="bare list"),
    ],
)
def test_paginate_endpoint_rejects_body(
    http_mock: requests_mock.Mocker, payload: Any, match: str
) -> None:
    # a removed endpoint answers with an error envelope or another body
    path = "rest/api/3/workflows/search"
    serve(http_mock, path, payload)
    with pytest.raises(JiraResponseError, match=f"{path}.*{match}"):
        list(read_endpoint(path, data_path="values"))


def test_paginate_endpoint_keeps_rows_without_key(
    http_mock: requests_mock.Mocker, mocker: MockerFixture
) -> None:
    warning = mocker.patch("sources.jira.helpers.client.logger.warning")
    serve(
        http_mock,
        "rest/api/3/users",
        [{"id": "a"}, {"name": "Former"}],
        [{"name": "Also"}],
        [],
    )
    assert len(items(read_endpoint("rest/api/3/users"))) == 3
    # rows without the key are reported once
    assert warning.call_count == 1
    assert "without 'id'" in warning.call_args.args[0]


def test_bulk_fetch_short_batch_warns(
    http_mock: requests_mock.Mocker, mocker: MockerFixture
) -> None:
    http_mock.post(
        url_for("rest/api/2/issue/bulkfetch"),
        json={
            "issues": [make_issue(1, UPDATED_1)],
            "issueErrors": [{"id": "2", "errorMessage": "gone"}],
        },
    )
    warning = mocker.patch("sources.jira.helpers.client.logger.warning")
    issues = JiraClient(**CREDENTIALS).bulk_fetch(["1", "2", "3"], ["summary"], 100)
    assert [issue["id"] for issue in issues] == ["1"]
    messages = [call.args[0] for call in warning.call_args_list]
    assert len(messages) == 2
    assert "Skipping Jira issue `2`: gone" in messages[0]
    assert "neither data nor an error for issues ['3']" in messages[1]

from typing import Iterator

import pytest
import requests_mock

from tests.jira.utils import JiraMock, load_captured, make_issue


@pytest.fixture
def http_mock() -> Iterator[requests_mock.Mocker]:
    with requests_mock.Mocker() as m:
        yield m


@pytest.fixture
def jira_mock(http_mock: requests_mock.Mocker) -> JiraMock:
    return JiraMock(http_mock)


@pytest.fixture
def captured_mock(http_mock: requests_mock.Mocker) -> JiraMock:
    """Serves field metadata and issues captured from the test site, kept in `captured`"""
    captured = load_captured()
    jira_mock = JiraMock(http_mock, fields=captured["fields"])
    jira_mock.add_issues(*captured["issues"])
    jira_mock.captured = captured
    return jira_mock


@pytest.fixture
def search_mock(http_mock: requests_mock.Mocker) -> JiraMock:
    """Serves issues found by two overlapping JQL queries"""
    jira_mock = JiraMock(http_mock)
    jira_mock.add_issues(
        *(make_issue(n, "2024-01-15T10:30:00.000+0000") for n in range(1, 5))
    )
    jira_mock.jql_results = {
        "project = X": ["1", "2", "3"],
        "project = Y order by created": ["2", "4"],
    }
    return jira_mock

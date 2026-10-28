"""Mocked tests of the Entra ID auth of the Graph client: token calls go to requests_mock."""

import pytest
from dlt.sources.helpers.requests import HTTPError
from requests_mock import Mocker

from tests.sharepoint.utils import (
    AADSTS_ERROR,
    TOKEN_URL,
    File,
    mock_folder,
    new_fs,
    requests_to,
    token_response,
)


def test_entra_auth_token_refresh(graph: Mocker) -> None:
    graph.post(
        TOKEN_URL, [token_response("first", expires_in=0), token_response("second")]
    )
    mock_folder(graph, "Reports", [[File("A", "a.csv")]])
    fs = new_fs()

    for _ in range(3):
        assert fs.ls("Reports") == ["Reports/a.csv"]

    methods = [r.method for r in graph.request_history]
    assert methods == ["POST", "GET", "POST", "GET", "GET"]
    tokens = [r.headers["Authorization"] for r in requests_to(graph, "/children")]
    assert tokens == ["Bearer first", "Bearer second", "Bearer second"]


def test_entra_auth_token_error(graph: Mocker) -> None:
    rejected = {"error": "invalid_client", "error_description": AADSTS_ERROR}
    graph.post(TOKEN_URL, status_code=401, json=rejected)
    mock_folder(graph, "Reports", [[File("A", "a.csv")]])

    with pytest.raises(HTTPError) as exc_info:
        new_fs().ls("Reports")

    assert TOKEN_URL in str(exc_info.value)
    assert f"invalid_client: {AADSTS_ERROR}" in str(exc_info.value)
    assert [r.method for r in graph.request_history] == ["POST"]

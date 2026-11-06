"""Mocked tests of the Entra ID auth of the Graph client: token calls go to requests_mock."""

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import pytest
from dlt.sources.helpers.requests import HTTPError
from requests_mock import Mocker

from sources.sharepoint import sharepoint_lists
from sources.sharepoint.graph import EXPIRY_MARGIN, USER_AGENT, site_client
from sources.sharepoint.version import __version__
from tests.sharepoint.utils import (
    AADSTS_ERROR,
    CLIENT_ID,
    CLIENT_SECRET,
    NEW,
    SITE_ID,
    TENANT_ID,
    TOKEN_URL,
    File,
    list_item,
    mock_folder,
    mock_list,
    new_fs,
    requests_to,
    token_response,
)


def test_graph_requests_name_the_app(graph: Mocker) -> None:
    """Graph and the download host get the User-Agent SharePoint expects from an ISV app."""
    mock_folder(graph, "Reports", [[File("A", "a.csv", NEW, b"n\n1\n")]])

    with new_fs().open("Reports/a.csv") as f:
        assert f.read() == b"n\n1\n"

    assert USER_AGENT == f"ISV|dltHub|dlt-sharepoint/{__version__}"
    agents = [
        r.headers["User-Agent"] for r in graph.request_history if r.method == "GET"
    ]
    assert len(agents) == 3
    assert set(agents) == {USER_AGENT}


@pytest.mark.parametrize(
    "expires_in",
    [
        pytest.param(0, id="expired"),
        pytest.param(int(EXPIRY_MARGIN.total_seconds()), id="within_margin"),
    ],
)
def test_entra_auth_token_refresh(graph: Mocker, expires_in: int) -> None:
    graph.post(
        TOKEN_URL,
        [token_response("first", expires_in=expires_in), token_response("second")],
    )
    mock_folder(graph, "Reports", [[File("A", "a.csv")]])
    fs = new_fs()

    for _ in range(3):
        assert fs.ls("Reports") == ["Reports/a.csv"]

    methods = [r.method for r in graph.request_history]
    assert methods == ["POST", "GET", "POST", "GET", "GET"]
    tokens = [r.headers["Authorization"] for r in requests_to(graph, "/children")]
    assert tokens == ["Bearer first", "Bearer second", "Bearer second"]


def test_entra_auth_shared_by_resources(graph: Mocker) -> None:
    """All clients of one app and site share one token: three lists need one token request."""
    for title in ("A", "B", "C"):
        mock_list(graph, title, [[list_item("1", "x")]])

    rows = list(sharepoint_lists(["A", "B", "C"]))

    assert len(rows) == 3
    assert [r.method for r in graph.request_history].count("POST") == 1
    auth = site_client(SITE_ID, TENANT_ID, CLIENT_ID, CLIENT_SECRET).auth
    assert auth is new_fs().client.auth
    assert auth is not site_client(SITE_ID, TENANT_ID, CLIENT_ID, "rotated").auth


def test_entra_auth_concurrent_refresh(graph: Mocker) -> None:
    """Threads that find the token expired renew it once, and none sends the old one."""
    graph.post(TOKEN_URL, [token_response("first"), token_response("second")])
    mock_folder(graph, "Reports", [[File("A", "a.csv")]])
    fs = new_fs()
    assert fs.ls("Reports") == ["Reports/a.csv"]
    fs.client.auth.token_expiry = datetime.now(timezone.utc)

    with ThreadPoolExecutor(8) as pool:
        listings = list(pool.map(lambda _: fs.ls("Reports"), range(8)))

    assert listings == [["Reports/a.csv"]] * 8
    assert [r.method for r in graph.request_history].count("POST") == 2
    tokens = [r.headers["Authorization"] for r in requests_to(graph, "/children")]
    assert tokens == ["Bearer first"] + ["Bearer second"] * 8


def test_entra_auth_token_error(graph: Mocker) -> None:
    rejected = {"error": "invalid_client", "error_description": AADSTS_ERROR}
    graph.post(TOKEN_URL, status_code=401, json=rejected)
    mock_folder(graph, "Reports", [[File("A", "a.csv")]])

    with pytest.raises(HTTPError) as exc_info:
        new_fs().ls("Reports")

    assert TOKEN_URL in str(exc_info.value)
    assert f"invalid_client: {AADSTS_ERROR}" in str(exc_info.value)
    assert [r.method for r in graph.request_history] == ["POST"]

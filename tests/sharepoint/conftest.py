"""Fixtures of the mocked SharePoint tests; the live tests read the real credentials instead."""

import pytest
from requests_mock import Mocker

from tests.sharepoint.utils import (
    ACCESS_TOKEN,
    CLIENT_ID,
    CLIENT_SECRET,
    SITE_ID,
    TENANT_ID,
    TOKEN_URL,
    token_response,
)


@pytest.fixture
def credentials_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Sets fake credentials as env vars, which win over a local secrets.toml."""
    monkeypatch.setenv("SOURCES__SHAREPOINT__CREDENTIALS__TENANT_ID", TENANT_ID)
    monkeypatch.setenv("SOURCES__SHAREPOINT__CREDENTIALS__CLIENT_ID", CLIENT_ID)
    monkeypatch.setenv("SOURCES__SHAREPOINT__CREDENTIALS__CLIENT_SECRET", CLIENT_SECRET)
    monkeypatch.setenv("SOURCES__SHAREPOINT__CREDENTIALS__SITE_ID", SITE_ID)


@pytest.fixture
def graph(credentials_env: None, requests_mock: Mocker) -> Mocker:
    """Mocks the token endpoint; each test registers the Graph URLs it calls."""
    requests_mock.post(TOKEN_URL, **token_response(ACCESS_TOKEN))
    return requests_mock

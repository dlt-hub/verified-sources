from typing import Any, Dict, Iterator, List

import pytest

from sources.jira import get_paginated_data
from sources.jira.settings import DEFAULT_ENDPOINTS

AUTH = dict(subdomain="example", email="e@example.com", api_token="token", page_size=2)


class FakeResponse:
    def __init__(self, payload: Any) -> None:
        self._payload = payload

    def raise_for_status(self) -> None:
        pass

    def json(self) -> Any:
        return self._payload


class StubGet:
    """Serves `pages` in order, then `exhausted`, and records the url and params of each request."""

    def __init__(self, pages: List[Any], exhausted: Any = None) -> None:
        self.pages = pages
        self.exhausted = exhausted
        self.sent: List[Dict[str, Any]] = []
        self.urls: List[str] = []

    def __call__(self, url: str, **kwargs: Any) -> FakeResponse:
        self.urls.append(url)
        self.sent.append(dict(kwargs["params"]))
        if len(self.sent) <= len(self.pages):
            return FakeResponse(self.pages[len(self.sent) - 1])
        return FakeResponse(self.exhausted)


def install(monkeypatch: pytest.MonkeyPatch, stub: StubGet) -> StubGet:
    monkeypatch.setattr("sources.jira.requests.get", stub)
    return stub


def items(pages: Iterator[Any]) -> List[Any]:
    return [item for page in pages for item in page]


@pytest.mark.parametrize(
    "payload, match",
    [
        ({"errorMessages": ["Removed."]}, "'values'.*errorMessages"),
        ([{"id": "w1"}], "'values'.*list"),
    ],
    ids=["error_envelope", "bare_array"],
)
def test_missing_data_path_raises(
    payload: Any, match: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    install(monkeypatch, StubGet([payload]))
    with pytest.raises(ValueError, match="rest/api/3/workflow/search.*" + match):
        list(
            get_paginated_data(
                api_path="rest/api/3/workflow/search", data_path="values", **AUTH
            )
        )


def test_workflows_requests_supported_path(monkeypatch: pytest.MonkeyPatch) -> None:
    # rest/api/3/workflow/search is deprecated and scheduled for removal
    api_path = DEFAULT_ENDPOINTS["workflows"]["api_path"]
    assert api_path == "rest/api/3/workflows/search"
    stub = install(monkeypatch, StubGet([], {"values": []}))
    list(get_paginated_data(api_path=api_path, data_path="values", **AUTH))
    assert stub.urls[0] == f"https://{AUTH['subdomain']}.atlassian.net/{api_path}"

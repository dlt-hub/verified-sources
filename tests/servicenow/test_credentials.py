from typing import Any, Dict, Optional

import dlt
import pytest
from dlt.extract.exceptions import ResourceExtractionError

from sources.servicenow import servicenow_table
from sources.servicenow.helpers.credentials import (
    ApiKeyCredentials,
    BasicCredentials,
    OAuthClientCredentials,
    OAuthPasswordCredentials,
    ServiceNowCredentials,
    instance_base_url,
)

from tests.servicenow.utils import (
    INSTANCE,
    FakeInstance,
    add_task_tables,
    item_rows,
    make_records,
    updated_on,
)


@pytest.mark.parametrize(
    "instance_name,url",
    [
        ("dev12345", "https://dev12345.service-now.com"),
        ("acme.service-now.com", "https://acme.service-now.com"),
        ("https://servicenow.acme.com/", "https://servicenow.acme.com"),
        # urls of pages copied from the browser
        (
            "https://dev12345.service-now.com/login.do?a=1#b",
            "https://dev12345.service-now.com",
        ),
        (
            "dev12345.service-now.com/now/nav/ui/home",
            "https://dev12345.service-now.com",
        ),
    ],
)
def test_instance_base_url(instance_name: str, url: str) -> None:
    assert instance_base_url(instance_name) == url


@pytest.mark.parametrize(
    "credentials_class,secrets,grant_type",
    [
        (BasicCredentials, {"username": "dlt", "password": "secret"}, None),
        (ApiKeyCredentials, {"api_key": "key"}, None),
        (
            OAuthPasswordCredentials,
            {
                "client_id": "id",
                "client_secret": "s",
                "username": "dlt",
                "password": "secret",
            },
            "password",
        ),
        (
            OAuthClientCredentials,
            {"client_id": "id", "client_secret": "s"},
            "client_credentials",
        ),
    ],
    ids=["basic", "api-key", "oauth-password", "oauth-client-credentials"],
)
def test_servicenow_table_auth(
    instance: FakeInstance,
    credentials_class: Any,
    secrets: Dict[str, str],
    grant_type: Optional[str],
) -> None:
    add_task_tables(instance, make_records(3, updated_on))
    credentials: ServiceNowCredentials = credentials_class(instance=INSTANCE, **secrets)
    credentials.resolve()
    # tokens expire within the renewal margin and are renewed for every request
    instance.token_expires_in = 60
    rows = item_rows(
        list(
            servicenow_table(
                credentials=credentials, table="incident", write_disposition="replace"
            )
        )
    )
    assert len(rows) == 3
    headers = [headers for _, _, headers in instance.requests]
    if grant_type is None:
        assert instance.token_requests == []
    else:
        assert len(instance.token_requests) == len(instance.requests)
        token = instance.token_requests[0]
        assert token["grant_type"] == [grant_type]
        assert token["client_id"] == ["id"] and token["client_secret"] == ["s"]
        assert token.get("username") == (["dlt"] if grant_type == "password" else None)
        assert [h["Authorization"] for h in headers] == [
            f"Bearer token-{i + 1}" for i in range(len(headers))
        ]
    if credentials_class is ApiKeyCredentials:
        assert all(
            h["x-sn-apikey"] == "key" and "Authorization" not in h for h in headers
        )
    if credentials_class is BasicCredentials:
        assert all(h["Authorization"].startswith("Basic ") for h in headers)


def test_servicenow_table_revoked_token(
    instance: FakeInstance, credentials: BasicCredentials
) -> None:
    add_task_tables(instance, make_records(3, updated_on))
    oauth = OAuthClientCredentials(instance=INSTANCE, client_id="id", client_secret="s")
    oauth.resolve()
    # a token revoked before it expires is requested again once
    instance.failures["incident"] = [(401, "User Not Authenticated", {})]
    resource = servicenow_table(
        credentials=oauth, table="incident", write_disposition="replace"
    )
    assert len(item_rows(list(resource))) == 3
    assert len(instance.token_requests) == 2
    # instances can reject valid basic credentials once
    instance.failures["incident"] = [(401, "User Not Authenticated", {})]
    resource = servicenow_table(
        credentials=credentials, table="incident", write_disposition="replace"
    )
    assert len(item_rows(list(resource))) == 3
    instance.failures["incident"] = [(401, "User Not Authenticated", {})] * 2
    resource = servicenow_table(
        credentials=credentials, table="incident", write_disposition="replace"
    )
    with pytest.raises(
        ResourceExtractionError, match="HTTP 401 .* User Not Authenticated"
    ):
        list(resource)

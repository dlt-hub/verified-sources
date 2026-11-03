"""Microsoft Graph client for one SharePoint site."""

import threading
from datetime import datetime, timedelta, timezone  # noqa: I251
from functools import cache
from typing import Any

from dlt.common.configuration.specs import configspec
from dlt.sources.helpers.requests import HTTPError, Response
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.auth import OAuth2ClientCredentials
from dlt.sources.helpers.rest_client.paginators import JSONLinkPaginator

from .version import __version__

GRAPH_URL = "https://graph.microsoft.com/v1.0"
GRAPH_SCOPE = "https://graph.microsoft.com/.default"
# SharePoint prioritizes traffic that names its vendor and app over undecorated traffic
USER_AGENT = f"ISV|dltHub|dlt-sharepoint/{__version__}"
# renewed this long before `expires_in`, so a request in flight never carries an expired token
EXPIRY_MARGIN = timedelta(seconds=60)


@configspec
class EntraClientCredentials(OAuth2ClientCredentials):
    """Client credentials flow with one token for all threads, renewed under a lock before it expires.

    Token errors carry Entra's error description.
    """

    def __post_init__(self) -> None:
        super().__post_init__()
        self._lock = threading.Lock()

    def __call__(self, request: Any) -> Any:
        with self._lock:
            return super().__call__(request)

    def is_token_expired(self) -> bool:
        return datetime.now(timezone.utc) >= self.token_expiry - EXPIRY_MARGIN

    def obtain_token(self) -> None:
        try:
            super().obtain_token()
        except HTTPError as e:
            if e.response is None:
                raise
            raise HTTPError(
                f"{e}: {entra_error(e.response)}", response=e.response
            ) from e


@cache
def entra_auth(
    tenant_id: str, client_id: str, client_secret: str
) -> EntraClientCredentials:
    """Returns the app-only auth of the app, one per process, so all clients share its token."""
    return EntraClientCredentials(
        access_token_url=f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token",
        client_id=client_id,
        client_secret=client_secret,
        access_token_request_data={"scope": GRAPH_SCOPE},
    )


def site_client(
    site_id: str, tenant_id: str, client_id: str, client_secret: str
) -> RESTClient:
    """Returns a paging Graph client rooted at the site, authenticated with the app's shared token."""
    return RESTClient(
        base_url=f"{GRAPH_URL}/sites/{site_id}",
        headers={"User-Agent": USER_AGENT},
        auth=entra_auth(tenant_id, client_id, client_secret),
        paginator=JSONLinkPaginator(next_url_path="['@odata.nextLink']"),
        data_selector="value",
    )


def entra_error(response: Response) -> str:
    """Returns Entra's error code and description from a failed token response, or its raw text."""
    try:
        body = response.json()
        return f"{body['error']}: {body.get('error_description', '')}".strip(": ")
    except (ValueError, KeyError, TypeError):
        return str(response.text)

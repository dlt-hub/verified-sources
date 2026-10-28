"""Microsoft Graph client for one SharePoint site."""

from typing import Any

from dlt.common.configuration.specs import configspec
from dlt.sources.helpers.requests import HTTPError, Response
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.auth import OAuth2ClientCredentials
from dlt.sources.helpers.rest_client.paginators import JSONLinkPaginator

GRAPH_URL = "https://graph.microsoft.com/v1.0"
GRAPH_SCOPE = "https://graph.microsoft.com/.default"


@configspec
class EntraClientCredentials(OAuth2ClientCredentials):
    """Client credentials flow that keeps Entra's error description in token errors."""

    def obtain_token(self) -> None:
        try:
            super().obtain_token()
        except HTTPError as e:
            if e.response is None:
                raise
            raise HTTPError(
                f"{e}: {entra_error(e.response)}", response=e.response
            ) from e

    def parse_expiration_in_seconds(self, response_json: Any) -> int:
        # a minute early, so a request near the end of the token's life is not rejected
        return max(0, super().parse_expiration_in_seconds(response_json) - 60)


def entra_auth(
    tenant_id: str, client_id: str, client_secret: str
) -> EntraClientCredentials:
    """Builds the app-only auth; the token is requested on the first call."""
    return EntraClientCredentials(
        access_token_url=f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token",
        client_id=client_id,
        client_secret=client_secret,
        access_token_request_data={"scope": GRAPH_SCOPE},
    )


def graph_client(site_id: str, auth: EntraClientCredentials) -> RESTClient:
    """Builds a paging Graph client rooted at the site."""
    return RESTClient(
        base_url=f"{GRAPH_URL}/sites/{site_id}",
        auth=auth,
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

"""Credentials of a ServiceNow instance and the request authentication they create"""

from typing import Any, Dict, Optional, Union
from urllib.parse import urlsplit

from dlt.common.configuration.specs import CredentialsConfiguration, configspec
from dlt.common.typing import TSecretStrValue
from dlt.sources.helpers.requests import Session
from dlt.sources.helpers.rest_client.auth import (
    APIKeyAuth,
    AuthConfigBase,
    HttpBasicAuth,
    OAuth2ClientCredentials,
)

# access tokens are renewed this many seconds before they expire
TOKEN_EXPIRY_MARGIN = 60


def instance_base_url(instance: str) -> str:
    """Returns the url of an instance given by name, for example `dev12345`, by host or by url.
    The path of a url, for example of a page copied from the browser, is dropped.
    """
    instance = instance.strip()
    if "://" not in instance:
        host = instance.split("/", 1)[0]
        if "." not in host:
            host += ".service-now.com"
        instance = "https://" + host
    url = urlsplit(instance)
    return f"{url.scheme}://{url.netloc}"


@configspec
class ServiceNowOAuth2(OAuth2ClientCredentials):
    """OAuth 2.0 client credentials grant, or the password grant when `username` is set"""

    username: Optional[str] = None
    password: Optional[TSecretStrValue] = None

    def build_access_token_request(self) -> Dict[str, Any]:
        request = super().build_access_token_request()
        if self.username:
            request["data"].update(
                grant_type="password", username=self.username, password=self.password
            )
        return request

    def parse_expiration_in_seconds(self, response_json: Any) -> int:
        return max(
            super().parse_expiration_in_seconds(response_json) - TOKEN_EXPIRY_MARGIN, 0
        )


@configspec
class ServiceNowCredentials(CredentialsConfiguration):
    """ServiceNow instance and the authentication of its REST API. `instance` is a name (for
    example `dev12345`), a host or a url.
    """

    instance: str = None

    @property
    def base_url(self) -> str:
        return instance_base_url(self.instance)

    def make_auth(self, session: Session) -> AuthConfigBase:
        """Returns the authentication for requests. OAuth credentials get access tokens with
        `session`.
        """
        raise NotImplementedError()


@configspec
class OAuthPasswordCredentials(ServiceNowCredentials):
    """OAuth 2.0 password grant of an OAuth application registry entry"""

    client_id: str = None
    client_secret: TSecretStrValue = None
    username: str = None
    password: TSecretStrValue = None

    def make_auth(self, session: Session) -> AuthConfigBase:
        return ServiceNowOAuth2(
            access_token_url=f"{self.base_url}/oauth_token.do",
            client_id=self.client_id,
            client_secret=self.client_secret,
            username=self.username,
            password=self.password,
            session=session,
        )


@configspec
class OAuthClientCredentials(ServiceNowCredentials):
    """OAuth 2.0 client credentials grant, available from the Washington DC release when enabled
    with `glide.oauth.inbound.client.credential.grant_type.enabled`
    """

    client_id: str = None
    client_secret: TSecretStrValue = None

    def make_auth(self, session: Session) -> AuthConfigBase:
        return ServiceNowOAuth2(
            access_token_url=f"{self.base_url}/oauth_token.do",
            client_id=self.client_id,
            client_secret=self.client_secret,
            session=session,
        )


@configspec
class ApiKeyCredentials(ServiceNowCredentials):
    """REST API key sent in the `x-sn-apikey` header"""

    api_key: TSecretStrValue = None

    def make_auth(self, session: Session) -> AuthConfigBase:
        return APIKeyAuth(name="x-sn-apikey", api_key=self.api_key, location="header")


@configspec
class BasicCredentials(ServiceNowCredentials):
    """User name and password. When ServiceNow restricts basic authentication, the user needs the
    `snc_basic_auth_api_access` role or the "Web service access only" flag.
    """

    username: str = None
    password: TSecretStrValue = None

    def make_auth(self, session: Session) -> AuthConfigBase:
        return HttpBasicAuth(username=self.username, password=self.password)


# the most specific credentials go first, dlt uses the first one that resolves
ServiceNowAuth = Union[
    OAuthPasswordCredentials,
    OAuthClientCredentials,
    ApiKeyCredentials,
    BasicCredentials,
]

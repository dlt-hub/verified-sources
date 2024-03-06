from base64 import b64encode
import math
from typing import Dict, Final, Literal, Optional
import requests
from requests.auth import AuthBase
from requests import PreparedRequest
import pendulum
import jwt
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

from dlt.common import logger

from dlt.common.configuration.specs.base_configuration import configspec
from dlt.common.configuration.specs import CredentialsConfiguration
from dlt.common.typing import TSecretStrValue


TApiKeyLocation = Literal["header", "cookie", "query", "param"]  # Alias for scheme "in" field

class AuthConfigBase(AuthBase, CredentialsConfiguration):
    """Authenticator base which is both `requests` friendly AuthBase and dlt SPEC
       configurable via env variables or toml files
    """
    pass


@configspec
class BearerTokenAuth(AuthConfigBase):
    type: Final[Literal["http"]] = "http"
    scheme: Literal["bearer"] = "bearer"
    token: TSecretStrValue

    def __init__(self, token: TSecretStrValue) -> None:
        self.token = token

    def __call__(self, request: PreparedRequest) -> PreparedRequest:
        request.headers["Authorization"] = f"Bearer {self.token}"
        return request


@configspec
class APIKeyAuth(AuthConfigBase):
    type: Final[Literal["apiKey"]] = "apiKey"
    location: TApiKeyLocation = "header"
    name: str
    api_key: TSecretStrValue

    def __init__(self, name: str, api_key: TSecretStrValue, location: TApiKeyLocation = "header") -> None:
        self.name = name
        self.api_key = api_key
        self.location = location

    def __call__(self, request: PreparedRequest) -> PreparedRequest:
        if self.location == "header":
            request.headers[self.name] = self.api_key
        elif self.location in ["query", "param"]:
            request.prepare_url(request.url, {self.name: self.api_key})
        elif self.location == "cookie":
            raise NotImplementedError()
        return request


@configspec
class HttpBasicAuth(AuthConfigBase):
    type: Final[Literal["http"]] = "http"
    scheme: Literal["basic"] = "basic"
    username: str
    password: TSecretStrValue

    def __init__(self, username: str, password: TSecretStrValue) -> None:
        self.username = username
        self.password = password

    def __call__(self, request: PreparedRequest) -> PreparedRequest:
        encoded = b64encode(f"{self.username}:{self.password}".encode()).decode()
        request.headers["Authorization"] = f"Basic {encoded}"
        return request


@configspec
class OAuth2AuthBase(AuthConfigBase):
    """Base class for oauth2 authenticators. requires access_token"""
    # TODO: Separate class for flows (implicit, authorization_code, client_credentials, etc)
    type: Final[Literal["oauth2"]] = "oauth2"
    access_token: TSecretStrValue

    def __init__(self, access_token: TSecretStrValue) -> None:
        self.access_token = access_token

    def __call__(self, request: PreparedRequest) -> PreparedRequest:
        request.headers["Authorization"] = f"Bearer {self.access_token}"
        return request


@configspec
class OAuthJWTAuth(BearerTokenAuth):
    """This is a form of Bearer auth, actually there's not standard way to declare it in openAPI"""
    format: Final[Literal["JWT"]] = "JWT"

    client_id: str
    private_key: TSecretStrValue
    auth_endpoint: str
    scopes: Optional[str] = None
    headers: Optional[Dict[str, str]] = None
    private_key_passphrase: Optional[TSecretStrValue] = None

    def __init__(
        self,
        client_id,
        private_key,
        auth_endpoint,
        scopes,
        headers,
        private_key_passphrase=None,
    ):
        self.client_id = client_id
        self.private_key = private_key
        self.private_key_passphrase = private_key_passphrase
        self.auth_endpoint = auth_endpoint
        self.scopes = scopes if isinstance(scopes, str) else " ".join(scopes)
        self.headers = headers
        self.token = None
        self.token_expiry = None

    def __call__(self, r):
        if self.token is None or self.is_token_expired():
            self.obtain_token()
        r.headers["Authorization"] = f"Bearer {self.token}"
        return r

    def is_token_expired(self):
        return not self.token_expiry or pendulum.now() >= self.token_expiry

    def obtain_token(self):
        payload = self.create_jwt_payload()
        data = {
            "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
            "assertion": jwt.encode(
                payload, self.load_private_key(), algorithm="RS256"
            ),
        }

        logger.debug(f"Obtaining token from {self.auth_endpoint}")

        response = requests.post(self.auth_endpoint, headers=self.headers, data=data)
        response.raise_for_status()

        token_response = response.json()
        self.token = token_response["access_token"]
        self.token_expiry = pendulum.now().add(
            seconds=token_response.get("expires_in", 3600)
        )

    def create_jwt_payload(self):
        now = pendulum.now()
        return {
            "iss": self.client_id,
            "sub": self.client_id,
            "aud": self.auth_endpoint,
            "exp": math.floor((now.add(hours=1)).timestamp()),
            "iat": math.floor(now.timestamp()),
            "scope": self.scopes,
        }

    def load_private_key(self):
        private_key_bytes = self.private_key.encode("utf-8")
        return serialization.load_pem_private_key(
            private_key_bytes,
            password=self.private_key_passphrase.encode("utf-8") if self.private_key_passphrase else None,
            backend=default_backend(),
        )

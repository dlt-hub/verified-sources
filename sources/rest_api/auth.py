import math
import requests
from requests.auth import AuthBase
from requests import PreparedRequest
import pendulum
import jwt
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

from dlt.common import logger


class BearerTokenAuth(AuthBase):
    def __init__(self, token: str) -> None:
        self.token = token

    def __call__(self, request: PreparedRequest) -> PreparedRequest:
        request.headers["Authorization"] = f"Bearer {self.token}"
        return request


class APIKeyAuth(AuthBase):
    def __init__(self, key: str, value: str, location: str = "headers") -> None:
        self.key = key
        self.value = value
        self.location = location

    def __call__(self, request: PreparedRequest) -> PreparedRequest:
        if self.location == "headers":
            request.headers[self.key] = self.value
        elif self.location == "params":
            request.prepare_url(request.url, {self.key: self.value})
        return request


class OAuthJWTAuth(AuthBase):
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

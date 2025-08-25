from dlt.sources.helpers import requests
import base64
import json
import random
from intuitlib.enums import Scopes
from typing import Union
from urllib.parse import urlencode
from .settings import (
    discovery_document_url_sandbox,
    discovery_document_url_prod
)


class OAuth2Config:
    def __init__(
        self,
        issuer: str = "",
        auth_endpoint: str = "",
        token_endpoint: str = "",
        userinfo_endpoint: str = "",
        revoke_endpoint: str = "",
        jwks_uri: str = "",
    ):
        self.issuer = issuer
        self.auth_endpoint = auth_endpoint
        self.token_endpoint = token_endpoint
        self.userinfo_endpoint = userinfo_endpoint
        self.revoke_endpoint = revoke_endpoint
        self.jwks_uri = jwks_uri


class Bearer:
    def __init__(
        self,
        refresh_expiry: str,
        access_token: str,
        token_type: str,
        refresh_token: str,
        access_token_expiry: str,
        id_token: Union[str, None] = None,
    ):
        self.refreshExpiry = refresh_expiry
        self.accessToken = access_token
        self.tokenType = token_type
        self.refreshToken = refresh_token
        self.accessTokenExpiry = access_token_expiry
        self.idToken = id_token


class QuickBooksAuth:
    def __init__(
        self,
        client_id: str,
        client_secret: str,
        company_id: str,
        redirect_url: str,
        refresh_token: str = None,
        is_sandbox: Union[bool, None] = True,
    ):
        """
        End user should use this class to generate refresh token once manually and store in secrets.toml
        and continually use it to generate access tokens

        Should the user need to change scopes, then this should be generated again and stored safely

        Source code used is from: https://github.com/IntuitDeveloper/OAuth2PythonSampleApp/blob/master/sampleAppOAuth2/services.py
        """
        self.is_sandbox = is_sandbox or None
        self.client_id = client_id
        self.client_secret = client_secret
        self.company_id = company_id
        self.redirect_url = redirect_url
        self.refresh_token = refresh_token

    @staticmethod
    def string_to_base64(s: str) -> str:
        return base64.b64encode(bytes(s, "utf-8")).decode()

    @staticmethod
    def get_random_string(
        length: int = 64,
        allowed_chars: str = "abcdefghijklmnopqrstuvwxyz"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789",
    ) -> str:
        return "".join(random.choice(allowed_chars) for i in range(length))

    def get_discovery_document(self) -> OAuth2Config:
        if self.is_sandbox:
            discovery_document_url = discovery_document_url_sandbox
        else:
            discovery_document_url = discovery_document_url_prod
        r = requests.get(discovery_document_url)
        if r.status_code >= 400:
            raise ConnectionError(r.json())

        discovery_doc_json = r.json()
        discovery_doc = OAuth2Config(
            issuer=discovery_doc_json["issuer"],
            auth_endpoint=discovery_doc_json["authorization_endpoint"],
            userinfo_endpoint=discovery_doc_json["userinfo_endpoint"],
            revoke_endpoint=discovery_doc_json["revocation_endpoint"],
            token_endpoint=discovery_doc_json["token_endpoint"],
            jwks_uri=discovery_doc_json["jwks_uri"],
        )

        return discovery_doc

    def get_auth_url(self, scope: Union[str, Scopes]) -> str:
        """
        scopes available in settings.py from intuitlib.enums
        """
        auth_endpoint = self.get_discovery_document().auth_endpoint
        auth_url_params = {
            "client_id": self.client_id,
            "redirect_uri": self.redirect_url,
            "response_type": "code",
            "scope": scope,
            "state": self.get_random_string(),
        }
        url = f"{auth_endpoint}?{urlencode(auth_url_params)}"

        return url

    def get_bearer_token(
        self, auth_code: str, client_id: str, client_secret: str, redirect_uri: str
    ) -> Union[str, Bearer]:
        token_endpoint = self.get_discovery_document().token_endpoint
        auth_header = "Basic " + self.string_to_base64(client_id + ":" + client_secret)
        headers = {
            "Accept": "application/json",
            "content-type": "application/x-www-form-urlencoded",
            "Authorization": auth_header,
        }
        payload = {
            "code": auth_code,
            "redirect_uri": redirect_uri,
            "grant_type": "authorization_code",
        }
        r = requests.post(token_endpoint, data=payload, headers=headers)
        if r.status_code != 200:
            return r.text
        bearer_raw = json.loads(r.text)

        if "id_token" in bearer_raw:
            id_token = bearer_raw["id_token"]
        else:
            id_token = None

        return Bearer(
            bearer_raw["x_refresh_token_expires_in"],
            bearer_raw["access_token"],
            bearer_raw["token_type"],
            bearer_raw["refresh_token"],
            bearer_raw["expires_in"],
            id_token=id_token,
        )

    def get_bearer_token_from_refresh_token(self) -> Bearer:
        token_endpoint = self.get_discovery_document().token_endpoint
        auth_header = "Basic " + self.string_to_base64(
            self.client_id + ":" + self.client_secret
        )
        headers = {
            "Accept": "application/json",
            "content-type": "application/x-www-form-urlencoded",
            "Authorization": auth_header,
        }

        payload = {"refresh_token": self.refresh_token, "grant_type": "refresh_token"}
        r = requests.post(token_endpoint, data=payload, headers=headers)
        bearer_raw = json.loads(r.text)

        if "id_token" in bearer_raw:
            id_token = bearer_raw["id_token"]
        else:
            id_token = None

        return Bearer(
            bearer_raw["x_refresh_token_expires_in"],
            bearer_raw["access_token"],
            bearer_raw["token_type"],
            bearer_raw["refresh_token"],
            bearer_raw["expires_in"],
            id_token=id_token,
        )

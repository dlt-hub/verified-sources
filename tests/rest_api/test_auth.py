import pytest
from typing import cast
from sources import rest_api
from sources.rest_api.typing import ApiKeyAuthConfig, AuthConfig
from dlt.sources.helpers.rest_client.auth import APIKeyAuth


class TestCustomAuth:
    @pytest.fixture
    def custom_auth_config(self) -> AuthConfig:
        config: AuthConfig = {
            "type": "custom_oauth2",  # type: ignore
            "access_token_url": "https://example.com/oauth/token",
            "client_id": "test_client_id",
            "client_secret": "test_client_secret",
            "access_token_request_data": {
                "grant_type": "account_credentials",
                "account_id": "test_account_id",
            },
        }
        return config

    def test_creates_builtin_auth_without_registering(self) -> None:
        config: ApiKeyAuthConfig = {
            "type": "api_key",
            "api_key": "test-secret",
            "location": "header",
        }
        auth = cast(APIKeyAuth, rest_api.config_setup.create_auth(config))
        assert auth.api_key == "test-secret"

    def test_not_registering_throws_error(self, custom_auth_config: AuthConfig) -> None:
        with pytest.raises(ValueError) as e:
            rest_api.config_setup.create_auth(custom_auth_config)

        assert e.match("Invalid authentication: custom_oauth2.")

from typing import (
    Final,
    Literal,
)

from dlt.common.configuration.specs.base_configuration import configspec

from dlt.sources.helpers.rest_client import auth


@configspec
class BearerTokenAuth(auth.BearerTokenAuth):
    type: Final[Literal["http"]] = "http"  # noqa: A003
    scheme: Literal["bearer"] = "bearer"


@configspec
class APIKeyAuth(auth.APIKeyAuth):
    type: Final[Literal["apiKey"]] = "apiKey"  # noqa: A003


@configspec
class HttpBasicAuth(auth.HttpBasicAuth):
    type: Final[Literal["http"]] = "http"  # noqa: A003
    scheme: Literal["basic"] = "basic"


@configspec
class OAuth2AuthBase(auth.OAuth2AuthBase):
    """Base class for oauth2 authenticators. requires access_token"""

    type: Final[Literal["oauth2"]] = "oauth2"  # noqa: A003


@configspec
class OAuthJWTAuth(BearerTokenAuth, auth.OAuthJWTAuth):
    pass

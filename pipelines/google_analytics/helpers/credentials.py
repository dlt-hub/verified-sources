"""
This module handles how credentials are read in dlt sources
"""
from typing import ClassVar, List, Optional
from dlt.common import logger
from dlt.common.configuration import configspec
from dlt.common.configuration.specs import CredentialsConfiguration
from dlt.common.exceptions import MissingDependencyException
from dlt.common.typing import TSecretValue
try:
    from google.oauth2.credentials import Credentials
except ImportError:
    raise MissingDependencyException("Google OAuth Library", ["google-auth-oauthlib"])
try:
    from requests_oauthlib import OAuth2Session
except ImportError:
    raise MissingDependencyException("Requests-OAuthlib", ["requests_oauthlib"])


class GoogleAnalyticsCredentialsBase(CredentialsConfiguration):
    """
    The Base version of all the GoogleAnalyticsCredentials classes.
    """
    __config_gen_annotations__: ClassVar[List[str]] = []


@configspec
class GoogleAnalyticsCredentialsOAuth(GoogleAnalyticsCredentialsBase):
    """
    This class is used to store credentials Google Analytics
    """
    client_id: str
    client_secret: TSecretValue
    project_id: TSecretValue
    refresh_token: TSecretValue
    access_token: Optional[TSecretValue] = None

    def auth(self) -> None:
        """
        Will produce an access token from the given credentials.
        """
        try:
            google = OAuth2Session(client_id=self.client_id, scope=["https://www.googleapis.com/auth/analytics.readonly"], redirect_uri="https://localhost")
            extra = {
                "client_id": self.client_id,
                "client_secret": self.client_secret
            }
            self.access_token = google.refresh_token(token_url="https://oauth2.googleapis.com/token", refresh_token=self.refresh_token, **extra)["access_token"]
        except Exception as e:
            logger.warning("Couldn't create access token from credentials. Refresh token may have expired!")
            logger.warning(str(e))
            raise ValueError("Invalid credentials for creating an OAuth token!")

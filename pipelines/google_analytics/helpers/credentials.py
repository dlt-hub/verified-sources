"""
This module handles how credentials are read in dlt sources
"""
from typing import ClassVar, List, Optional
from dlt.common import logger
from dlt.common.exceptions import MissingDependencyException
from dlt.common.configuration import configspec
from dlt.common.configuration.specs import CredentialsConfiguration
from dlt.common.typing import TSecretValue
try:
    from google.oauth2.credentials import Credentials
except ImportError:
    raise MissingDependencyException("Google OAuth2 library", ["google-auth-oauthlib"])


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
    refresh_token: TSecretValue
    access_token: Optional[TSecretValue] = None

    def auth(self):
        """
        Will produce an access token from the given credentials.
        :returns: An access token to GA4
        """
        try:
            creds = Credentials.from_authorized_user_info(
                info={
                    "scopes": "https://www.googleapis.com%2Fauth%2Fanalytics.readonly"
                }
            )
            self.access_token = creds.token
        except Exception as e:
            logger.warning("Couldn't create access token from credentials. Refresh token may have expired!")
            logger.warning(e)
            raise ValueError("Invalid credentials for creating an OAuth token!")

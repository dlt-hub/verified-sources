"""
This module handles how credentials are read in dlt sources
"""
from typing import ClassVar, List, Optional
from dlt.common import logger
from dlt.common.exceptions import MissingDependencyException
from dlt.common.configuration import configspec
from dlt.common.configuration.specs import CredentialsConfiguration
from dlt.common.typing import TSecretValue

# try:
#     from google.oauth2.credentials import Credentials
# except ImportError:
#     raise MissingDependencyException("Google OAuth2 library", ["google-auth-oauthlib"])
try:
    from google_auth_oauthlib.flow import InstalledAppFlow
except ImportError:
    raise MissingDependencyException("Google Auth library", ["google-auth-oauthlib"])


class GoogleAnalyticsCredentialsBase(CredentialsConfiguration):
    """
    The Base version of all the GoogleAnalyticsCredentials classes.
    """
    __config_gen_annotations__: ClassVar[List[str]] = []


def local_server_flow(client_config, scopes, port=0):
    """Run an OAuth flow using a local server strategy.
    Creates an OAuth flow and runs `google_auth_oauthlib.flow.InstalledAppFlow.run_local_server <https://google-auth-oauthlib.readthedocs.io/en/latest/reference/google_auth_oauthlib.flow.html#google_auth_oauthlib.flow.InstalledAppFlow.run_local_server>`_.
    This will start a local web server and open the authorization URL in
    the user's browser.
    Pass this function to ``flow`` parameter of :meth:`~gspread.oauth` to run
    a local server flow.
    """
    flow = InstalledAppFlow.from_client_config(client_config, scopes)
    return flow.run_local_server(port=port)


@configspec
class GoogleAnalyticsCredentialsOAuth(GoogleAnalyticsCredentialsBase):
    """
    This class is used to store credentials Google Analytics
    """
    client_id: str
    client_secret: TSecretValue
    project_id: TSecretValue
    refresh_token: TSecretValue  # are we using this anywhere?
    access_token: Optional[TSecretValue] = None

    def auth(self):
        """
        Will produce an access token from the given credentials.
        :returns: An access token to GA4
        """
        try:
            scopes = ["https://www.googleapis.com/auth/analytics.readonly"]
            credentials = {
                "installed": {
                    "client_id": self.client_id,
                    "project_id": self.project_id,
                    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                    "token_uri": "https://oauth2.googleapis.com/token",
                    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
                    "client_secret": self.client_secret,
                    "redirect_uris": [
                        "http://localhost"
                    ]
                }
            }
            creds = local_server_flow(client_config=credentials, scopes=scopes)
            token = self.access_token = creds.token
            return token
        except Exception as e:
            logger.warning("Couldn't create access token from credentials. Refresh token may have expired!")
            logger.warning(e)
            raise ValueError("Invalid credentials for creating an OAuth token!")

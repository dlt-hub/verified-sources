"""
This script will help you obtain an OAuth token from your GCP account with access to GA4. Alternatively service account credentials can be used (see docs)
This script will receive client_id and client_secret to produce an OAuth refresh_token which is then saved in secrets.toml along with client credentials.
Before running this script you must:
1. Ensure your email used for the GCP account has access to the GA4 property.
2. Open a project in your GCP account.
3. Enable the Analytics API in the project
4. Search credentials in the search bar and go to Credentials
5. Create credentials -> OAuth client ID -> Select Desktop App from Application type and give a name to the client.
6. Download the credentials and fill client_id, client_secret and project_id in secrets.toml
7. Go back to credentials and select OAuth consent screen in the left
8. Fill in App name, user support email(your email), authorized domain (localhost.com), dev contact info (your email again)
9. Add the following scope: “https://www.googleapis.com/auth/analytics.readonly”
10. Add your own email as a test user."""
from typing import Optional

import dlt
from dlt.common.configuration.inject import with_config
from dlt.common.exceptions import MissingDependencyException

from helpers.credentials import GoogleAnalyticsCredentialsOAuth
try:
    from google_auth_oauthlib.flow import InstalledAppFlow
except ImportError:
    raise MissingDependencyException("Google Auth library", ["google-auth-oauthlib"])


@with_config(sections=("sources", "google_analytics"))
def print_refresh_token(credentials: Optional[GoogleAnalyticsCredentialsOAuth] = None) -> None:
    """
    Will get client_id, client_secret and project_id from secrets.toml and then will print the refresh token.
    """
    scopes = ["https://www.googleapis.com/auth/analytics.readonly"]
    credentials_json = {
        "installed": {
            "client_id": "",
            "project_id": "",
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "client_secret": "",
            "redirect_uris": [
                "http://localhost"
            ]
        }
    }
    credentials = credentials or GoogleAnalyticsCredentialsOAuth()
    print(credentials)
    default_client_id = credentials.client_id or ""
    credentials_json["installed"]["client_id"] = input(f"Enter client_id ({default_client_id}): ") or default_client_id
    if not default_client_id:
        raise ValueError(default_client_id)
    default_project_id = credentials.project_id or ""
    credentials_json["installed"]["project_id"] = input(f"Enter project_id: ({default_project_id})") or default_project_id
    if not default_project_id:
        raise ValueError(default_project_id)
    default_client_secret = credentials.client_secret or ""
    credentials_json["installed"]["client_secret"] = input(f"Enter client_secret: ({default_client_secret})") or default_client_secret
    if not default_client_secret:
        raise ValueError(default_client_secret)

    # run local server to get the refresh token for this client
    flow = InstalledAppFlow.from_client_config(credentials_json, scopes)
    creds = flow.run_local_server(port=0)
    print(f"Refresh token: {creds.refresh_token}")


if __name__ == "__main__":
    print(
        "Before running this script you must:\n"
        "1. Ensure your email used for the GCP account has access to the GA4 property.\n"
        "2. Open a project in your GCP account.\n"
        "3. Enable the Analytics API in the project\n"
        "4. Search credentials in the search bar and go to Credentials\n"
        "5. Create credentials -> OAuth client ID -> Select Desktop App from Application type and give a name to the client.\n"
        "6. Download the credentials. \n"
        "7. Go back to credentials and select OAuth consent screen in the left\n"
        "8. Fill in App name, user support email(your email), authorized domain (localhost.com), dev contact info (your email again)\n"
        "9. Add the following scope: “https://www.googleapis.com/auth/analytics.readonly”\n"
        "10. Add your own email as a test user."
        "11. Run this script and fill the values from the downloaded credentials.\n"
    )
    print_refresh_token()

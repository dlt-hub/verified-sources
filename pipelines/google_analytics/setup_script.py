"""This script will receive client_id and client_secret to produce an oauth refresh_token which is then saved in secrets.toml along with client credentials."""
import dlt
from dlt.common.exceptions import MissingDependencyException
try:
    from google_auth_oauthlib.flow import InstalledAppFlow
except ImportError:
    raise MissingDependencyException("Google Auth library", ["google-auth-oauthlib"])


def print_refresh_token() -> None:
    """
    Will get client_id, client_secret and project_id from secrets.toml and then will print the refresh token.
    """
    scopes = ["https://www.googleapis.com/auth/analytics.readonly"]
    credentials = {
        "installed": {
            "client_id": input("Enter client_id: "),
            "project_id": input("Enter project_id: "),
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "client_secret": input("Enter client_secret: "),
            "redirect_uris": [
                "http://localhost"
            ]
        }
    }
    # run local server to get the refresh token for this client
    flow = InstalledAppFlow.from_client_config(credentials, scopes)
    creds = flow.run_local_server(port=0)
    print(f"Refresh token: {creds.refresh_token}")


if __name__ == "__main__":
    print_refresh_token()

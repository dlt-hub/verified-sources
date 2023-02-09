from zenpy import Zenpy
from typing import Any


def auth_zendesk(credentials: dict[str]) -> Zenpy:
    """
    Helper, gets a dict of credentials and authenticates to the Zendesk API
    @:param: credentials - dict of credentials, 3 main ways of authenticating to zendesk api, all require some differing credentials
    @:returns: API client to make requests to Zendesk API
    """

    # TODO: Maybe add this to dlt.common?
    email_pass_auth = "email" in credentials and "password" in credentials and credentials["email"] != "" and credentials["password"] != ""
    api_token_auth = "email" in credentials and "token" in credentials and "email" in credentials
    oauth_auth = "oauth_token" in credentials
    if email_pass_auth:
        zen_client = Zenpy(
            password=credentials["password"],
            email=credentials["email"]
        )
    elif api_token_auth:
        zen_client = Zenpy(
            email=credentials["email"],
            token=credentials["token"],
            subdomain=credentials["subdomain"]
        )
    elif oauth_auth:
        zen_client = Zenpy(oauth_token=credentials["oauth_token"])
    else:
        raise ValueError("Couldn't find valid credentials")
    return zen_client



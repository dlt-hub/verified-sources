from asana import Client as AsanaClient


def get_client(
    access_token: str,
) -> AsanaClient:
    """Returns an Asana client with a valid access token"""
    asana = AsanaClient.access_token(access_token)
    return asana

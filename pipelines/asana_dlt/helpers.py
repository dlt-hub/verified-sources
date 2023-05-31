from asana import Client as AsanaClient


def get_client(
    access_token: str,
) -> AsanaClient:
    """
    Returns an Asana API client.
    Returns:
        AsanaClient: The Asana API client.
    """
    asana = AsanaClient.access_token(access_token)
    return asana

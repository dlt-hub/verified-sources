import base64
from dlt.sources.helpers import requests
from dlt.common.typing import TDataItem
from typing import Iterable


def create_auth_headers(api_secret_key):
    """
    Constructs Basic Authorization header required by Freshdesk.

    Args:
        api_secret_key (str): Freshdesk API secret key.

    Returns:
        dict: Headers with Basic Authorization.
    """
    encoded_key = base64.b64encode(f"{api_secret_key}:X".encode()).decode()
    headers = {"Authorization": f"Basic {encoded_key}"}
    return headers


def get_endpoint(
    api_secret_key: str, domain: str, endpoint: str
) -> Iterable[TDataItem]:
    """
    Fetches data from a Freshdesk API endpoint.

    Args:
        api_secret_key (str): Freshdesk API secret key.
        endpoint (str): Freshdesk API endpoint.

    Yields:
        Iterable[TDataItem]: Data fetched from the endpoint.
    """
    headers = create_auth_headers(api_secret_key)
    url = f"https://{domain}.freshdesk.com/api/v2/{endpoint}"
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    data = response.json()
    yield data


def paginated_response(url, page, per_page, updated_since, headers):
    """
    Retrieves data from a paginated Freshdesk API endpoint.

    Args:
        url (str): The base URL of the paginated endpoint.
        page (int): Page number to start from.
        per_page (int): Number of items per page.
        updated_since (str): Timestamp for filtering.
        headers (dict): Headers for the API request.

    Yields:
        dict: Data fetched from the paginated endpoint.
    """
    while True:
        paginated_url = (
            f"{url}?per_page={per_page}&page={page}&updated_since={updated_since}"
        )
        response = requests.get(paginated_url, headers=headers)
        response.raise_for_status()

        tickets = response.json()
        if tickets is None:
            print("Error: No data returned from API")
            break

        if tickets:
            yield tickets
            page += 1
        else:
            break

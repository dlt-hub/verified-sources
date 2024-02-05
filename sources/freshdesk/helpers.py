import base64
from dlt.sources.helpers import requests  # Assuming this should be the requests library directly
from typing import Iterable, Dict


def create_auth_headers(api_secret_key: str) -> Dict[str, str]:
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

def get_endpoint(api_secret_key: str, domain: str, endpoint: str) -> Iterable[Dict]:
    """
    Fetches data from a Freshdesk API endpoint.

    Args:
        api_secret_key (str): Freshdesk API secret key.
        domain (str): The Freshdesk domain.
        endpoint (str): Freshdesk API endpoint.

    Yields:
        Iterable[Dict]: Data fetched from the endpoint.
    """
    headers = create_auth_headers(api_secret_key)
    url = f"https://{domain}.freshdesk.com/api/v2/{endpoint}"
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    data = response.json()
    yield data

def paginated_response(url: str, page: int, per_page: int, updated_since: str, headers: Dict[str, str]) -> Iterable[Dict]:
    """
    Retrieves data from a paginated Freshdesk API endpoint.

    Args:
        url (str): The base URL of the paginated endpoint.
        page (int): Page number to start from.
        per_page (int): Number of items per page.
        updated_since (str): Timestamp for filtering.
        headers (dict): Headers for the API request.

    Yields:
        Iterable[Dict]: Data fetched from the paginated endpoint.
    """
    while True:
        paginated_url = f"{url}?per_page={per_page}&page={page}&updated_since={updated_since}"
        response = requests.get(paginated_url, headers=headers)
        response.raise_for_status()

        data = response.json()
        if not data:
            break

        yield data
        page += 1

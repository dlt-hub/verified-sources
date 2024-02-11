from typing import Any, Dict, Iterable, Optional

from dlt.sources.helpers import requests


def get_endpoint(
    api_secret_key: str,
    page: int,
    per_page: int,
    domain: str,
    endpoint: str,
    updated_at: Optional[str] = None,
) -> Iterable[Dict[str, Any]]:
    """
    Fetches data from a Freshdesk API endpoint.
    """
    url = f"https://{domain}.freshdesk.com/api/v2/{endpoint}"
    yield from paginated_response(
        url, page, per_page, updated_at, api_secret_key, endpoint
    )


def paginated_response(
    url: str,
    page: int,
    per_page: int,
    updated_at: Optional[str],
    api_secret_key: str,
    endpoint: str,
) -> Iterable[Dict[str, Any]]:
    """
    Retrieves data from a paginated Freshdesk API endpoint.
    """
    while True:
        paginated_url = f"{url}?per_page={per_page}&page={page}"
        if endpoint == "tickets":
            paginated_url += f"&updated_since={updated_at}"

        response = requests.get(paginated_url, auth=(api_secret_key, "X"))
        response.raise_for_status()

        data = response.json()
        if not data:
            break

        yield data
        page += 1

"""Freshdesk source helpers"""

from typing import Any, Iterable, Optional

from dlt.common.typing import TDataItem
from dlt.sources.helpers import requests


def paginated_response(
    endpoint: str,
    page: int,
    per_page: int,
    updated_at: Optional[Any],
    domain: str,
    api_secret_key: str,
) -> Iterable[TDataItem]:
    """
    Retrieves data from an endpoint with pagination.

    Args:
        page (int): The starting page number for the API request.
        per_page (int): The number of items requested per page.
        updated_at (Optional[Any]): An optional 'updated_at' to limit the data retrieved.
                                    Defaults to None.
        domain (str): The Freshdesk domain to construct the request URL.
        api_secret_key (str): The API key used for authentication.
        endpoint (str): The endpoint to retrieve data from (e.g., 'tickets', 'contacts').

    Yields:
        Iterable[TDataItem]: Data items retrieved from the endpoint.

    """
    base_url = f"https://{domain}.freshdesk.com/api/v2/{endpoint}"

    while True:
        params = {"per_page": per_page, "page": page}
        if endpoint in ["tickets", "contacts"]:
            params["updated_since" if endpoint == "tickets" else "_updated_since"] = (
                updated_at
            )

        response = requests.get(base_url, params=params, auth=(api_secret_key, "X"))
        response.raise_for_status()

        data = response.json()
        if not data:
            break

        yield data
        page += 1

import dlt
import pytest
from dlt.common import pendulum

from dlt.sources.helpers import requests
from sources.personio.helpers import PersonioAPI


FIRST_DAY_OF_MILLENNIUM = pendulum.datetime(2000, 1, 1).to_date_string()
DATE_NOW = pendulum.now().to_date_string()


@pytest.fixture
def client():
    return PersonioAPI(
        client_id=dlt.secrets["sources.personio.client_id"],
        client_secret=dlt.secrets["sources.personio.client_secret"],
    )


def get_metadata(endpoint, headers, params):
    response = requests.get(
        f"https://api.personio.de/v1/{endpoint}", headers=headers, params=params
    )
    json_response = response.json()
    metadata = json_response.get("metadata", {})
    return metadata


endpoints_data = [
    # (endpoint, params, offset_by_page)
    ("company/time-offs", {"limit": 200, "offset": 0}, True),
    ("company/employees", {"limit": 10, "offset": 0}, False),
    (
        "company/attendances",
        {"limit": 1, "start_date": FIRST_DAY_OF_MILLENNIUM, "end_date": DATE_NOW},
        False,
    ),
]


@pytest.mark.parametrize("endpoint, params, offset_by_page", endpoints_data)
def test_client(endpoint, params, offset_by_page, client):
    headers = {"Authorization": f"Bearer {client.access_token}"}

    total_pages = get_metadata(endpoint, headers, params).get("total_pages")
    pages = list(
        client.get_pages(
            endpoint,
            params=params,
            offset_by_page=offset_by_page,
        )
    )
    assert len(pages) == total_pages

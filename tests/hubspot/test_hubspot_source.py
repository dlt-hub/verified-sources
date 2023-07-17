from unittest.mock import patch, ANY, call

import dlt
import pytest
from itertools import chain
from typing import Any
from urllib.parse import urljoin

from dlt.sources.helpers import requests
from sources.hubspot import hubspot, hubspot_events_for_objects, contacts
from sources.hubspot.helpers import fetch_data, BASE_URL
from sources.hubspot.settings import (
    CRM_CONTACTS_ENDPOINT,
    CRM_COMPANIES_ENDPOINT,
    CRM_DEALS_ENDPOINT,
    CRM_PRODUCTS_ENDPOINT,
    CRM_TICKETS_ENDPOINT,
    CRM_QUOTES_ENDPOINT,
)
from tests.hubspot.mock_data import (
    mock_contacts_data,
    mock_companies_data,
    mock_deals_data,
    mock_products_data,
    mock_tickets_data,
    mock_quotes_data,
    mock_contacts_with_history,
    mock_contacts_properties,
)
from tests.utils import (
    ALL_DESTINATIONS,
    assert_load_info,
    load_table_counts,
    load_table_distinct_counts,
)


@pytest.fixture()
def mock_response():
    def _mock_response(status=200, json_data=None, headers=None):
        mock_resp = requests.Response()
        mock_resp.status_code = status
        mock_resp.headers.update(headers or {})
        if json_data:
            mock_resp.json = lambda: json_data
        return mock_resp

    return _mock_response


def test_fetch_data_companies(mock_response):
    mock_data = mock_companies_data
    expected_data = [_p["properties"] for _p in mock_data["results"]]
    mock_resp = mock_response(json_data=mock_data)

    with patch("dlt.sources.helpers.requests.get", return_value=mock_resp):
        # get first page
        data = list(fetch_data(CRM_COMPANIES_ENDPOINT, "12345"))[0]
        assert len(data) == len(expected_data)
        assert data == expected_data


def test_fetch_data_contacts(mock_response):
    mock_data = mock_contacts_data
    expected_data = [_p["properties"] for _p in mock_data["results"]]
    mock_resp = mock_response(json_data=mock_data)

    with patch("dlt.sources.helpers.requests.get", return_value=mock_resp):
        data = list(fetch_data(CRM_CONTACTS_ENDPOINT, "12345"))[0]
        assert len(data) == len(expected_data)
        assert data == expected_data


def test_fetch_data_deals(mock_response):
    mock_data = mock_deals_data
    expected_data = [_p["properties"] for _p in mock_data["results"]]
    mock_resp = mock_response(json_data=mock_data)

    with patch("dlt.sources.helpers.requests.get", return_value=mock_resp):
        data = list(fetch_data(CRM_DEALS_ENDPOINT, "12345"))[0]
        assert len(data) == len(expected_data)
        assert data == expected_data


def test_fetch_data_products(mock_response):
    mock_data = mock_products_data
    expected_data = [_p["properties"] for _p in mock_data["results"]]
    mock_resp = mock_response(json_data=mock_data)

    with patch("dlt.sources.helpers.requests.get", return_value=mock_resp):
        data = list(fetch_data(CRM_PRODUCTS_ENDPOINT, "12345"))[0]
        assert len(data) == len(expected_data)
        assert data == expected_data


def test_fetch_data_tickets(mock_response):
    mock_data = mock_tickets_data
    expected_data = [_p["properties"] for _p in mock_data["results"]]
    mock_resp = mock_response(json_data=mock_data)

    with patch("dlt.sources.helpers.requests.get", return_value=mock_resp):
        data = list(fetch_data(CRM_TICKETS_ENDPOINT, "12345"))[0]
        assert len(data) == len(expected_data)
        assert data == expected_data


def test_fetch_data_quotes(mock_response):
    mock_data = mock_quotes_data
    expected_data = [_p["properties"] for _p in mock_data["results"]]
    mock_resp = mock_response(json_data=mock_data)

    with patch("dlt.sources.helpers.requests.get", return_value=mock_resp):
        data = list(fetch_data(CRM_QUOTES_ENDPOINT, "12345"))[0]
        assert len(data) == len(expected_data)
        assert data == expected_data


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_resource_contacts_with_history(destination_name: str, mock_response) -> None:
    prop_names = [p["name"] for p in mock_contacts_properties["results"]]
    prop_string = ",".join(prop_names)

    def fake_get(url: str, *args, **kwargs) -> Any:  # type: ignore[no-untyped-def]
        if "/properties" in url:
            return mock_response(json_data=mock_contacts_properties)
        return mock_response(json_data=mock_contacts_with_history)

    expected_rows = []
    for contact in mock_contacts_with_history["results"]:
        for items in contact["propertiesWithHistory"].values():  # type: ignore[attr-defined]
            expected_rows.extend(items)

    with patch("dlt.sources.helpers.requests.get", side_effect=fake_get) as m:
        pipeline = dlt.pipeline(
            pipeline_name="hubspot",
            destination=destination_name,
            dataset_name="hubspot_data",
            full_refresh=True,
        )
        load_info = pipeline.run(contacts(api_key="fake_key", include_history=True))
    assert_load_info(load_info)

    assert m.call_count == 3

    # Check that API is called with all properties listed
    m.assert_has_calls(
        [
            call(
                urljoin(BASE_URL, "/crm/v3/properties/contacts"),
                headers=ANY,
                params=None,
            ),
            call(
                urljoin(BASE_URL, CRM_CONTACTS_ENDPOINT),
                headers=ANY,
                params={"properties": prop_string, "limit": 100},
            ),
            call(
                urljoin(BASE_URL, CRM_CONTACTS_ENDPOINT),
                headers=ANY,
                params={"propertiesWithHistory": prop_string, "limit": 50},
            ),
        ]
    )

    assert load_table_counts(pipeline, "contacts_property_history") == {
        "contacts_property_history": len(expected_rows)
    }


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_all_resources(destination_name: str) -> None:
    pipeline = dlt.pipeline(
        pipeline_name="hubspot",
        destination=destination_name,
        dataset_name="hubspot_data",
        full_refresh=True,
    )
    load_info = pipeline.run(hubspot(include_history=True))
    print(load_info)
    assert_load_info(load_info)
    table_names = [
        t["name"]
        for t in pipeline.default_schema.data_tables()
        if not t["name"].endswith("_property_history") and not t.get("parent")
    ]

    # make sure no duplicates (ie. pages wrongly overlap)
    assert (
        load_table_counts(pipeline, *table_names)
        == load_table_distinct_counts(pipeline, "hs_object_id", *table_names)
        == {"companies": 200, "deals": 500, "contacts": 402}
    )

    history_table_names = [
        t["name"]
        for t in pipeline.default_schema.data_tables()
        if t["name"].endswith("_property_history")
    ]
    # Check history tables
    assert load_table_counts(pipeline, *history_table_names) == {
        "contacts_property_history": 16826,
        "deals_property_history": 13849,
    }

    # Check property from couple of contacts against known data
    with pipeline.sql_client() as client:
        rows = set(
            tuple(row)
            for row in client.execute_sql(
                """
            SELECT ch.property_name, ch.value, ch.source_type, ch.source_type, CAST(ch.timestamp AS VARCHAR)
                FROM contacts
            JOIN
                contacts_property_history AS ch ON contacts.id = ch.object_id
            WHERE contacts.id IN ('51', '1') AND property_name = 'email';
            """
            )
        )
    assert set(rows) == set(
        [
            (
                "email",
                "emailmaria@hubspot.com",
                "API",
                "API",
                "2022-06-15 08:51:51.399+00",
            ),
            ("email", "bh@hubspot.com", "API", "API", "2022-06-15 08:51:51.399+00"),
        ]
    )


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_event_resources(destination_name: str) -> None:
    pipeline = dlt.pipeline(
        pipeline_name="hubspot",
        destination=destination_name,
        dataset_name="hubspot_data",
        full_refresh=True,
    )
    load_info = pipeline.run(
        hubspot_events_for_objects("company", ["7086461639", "7086464459"])
    )
    print(load_info)
    assert_load_info(load_info)

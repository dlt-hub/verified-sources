from unittest.mock import patch, ANY, call

import dlt
import pytest
from typing import Any
from urllib.parse import urljoin

from dlt.common import pendulum
from dlt.extract.exceptions import ResourceExtractionError
from dlt.sources.helpers import requests
from sources.hubspot import hubspot, hubspot_events_for_objects
from sources.hubspot.helpers import fetch_data, BASE_URL
from sources.hubspot.settings import (
    DEFAULT_CONTACT_PROPS,
    CRM_CONTACTS_ENDPOINT,
    CRM_COMPANIES_ENDPOINT,
    CRM_DEALS_ENDPOINT,
    CRM_PRODUCTS_ENDPOINT,
    CRM_TICKETS_ENDPOINT,
    CRM_QUOTES_ENDPOINT,
)
from sources.hubspot.utils import chunk_properties
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
    expected_rows = []
    expected_props = "address,annualrevenue,associatedcompanyid,associatedcompanylastupdated,city,closedate,company,company_size,country,createdate,currentlyinworkflow,date_of_birth,days_to_close,degree,email,engagements_last_meeting_booked,engagements_last_meeting_booked_campaign,engagements_last_meeting_booked_medium,engagements_last_meeting_booked_source,fax,field_of_study,first_conversion_date,first_conversion_event_name,first_deal_created_date,firstname,followercount,gender,graduation_date,hs_object_id,hubspot_owner_assigneddate,hubspot_owner_id,hubspot_team_id,hubspotscore,industry,ip_city,ip_country,ip_country_code,ip_latlon,ip_state,ip_state_code,ip_zipcode,job_function,jobtitle,kloutscoregeneral,lastmodifieddate,lastname,lifecyclestage,linkedinbio,linkedinconnections,marital_status,message,military_status,mobilephone,notes_last_contacted,notes_last_updated,notes_next_activity_date,num_associated_deals,num_contacted_notes,num_conversion_events,num_notes,num_unique_conversion_events,numemployees,owneremail,ownername,phone,photo,recent_conversion_date,recent_conversion_event_name,recent_deal_amount,recent_deal_close_date,relationship_status,salutation,school,seniority,start_date,state,surveymonkeyeventlastupdated,total_revenue,twitterbio,twitterhandle,twitterprofilephoto,webinareventlastupdated,website,work_email,zip"

    def fake_get(url: str, *args, **kwargs) -> Any:  # type: ignore[no-untyped-def]
        if "/properties" in url:
            return mock_response(json_data=mock_contacts_properties)
        return mock_response(json_data=mock_contacts_with_history)

    for contact in mock_contacts_with_history["results"]:
        for items in contact["propertiesWithHistory"].values():  # type: ignore[attr-defined]
            expected_rows.extend(items)

    with patch("dlt.sources.helpers.requests.get", side_effect=fake_get) as m:
        pipeline = dlt.pipeline(
            pipeline_name="hubspot",
            destination=destination_name,
            dataset_name="hubspot_data",
            dev_mode=True,
        )
        source = hubspot(
            api_key="fake_key",
            include_history=True,
        )
        load_info = pipeline.run(
            source.with_resources("contacts", "contacts_property_history")
        )

    assert_load_info(load_info)

    assert m.call_count == 4

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
                params={
                    "properties": expected_props,
                    "limit": 100,
                },
            ),
            call(
                urljoin(BASE_URL, "/crm/v3/properties/contacts"),
                headers=ANY,
                params=None,
            ),
            call(
                urljoin(BASE_URL, CRM_CONTACTS_ENDPOINT),
                headers=ANY,
                params={
                    "propertiesWithHistory": expected_props,
                    "limit": 50,
                },
            ),
        ]
    )

    assert load_table_counts(pipeline, "contacts_property_history") == {
        "contacts_property_history": len(expected_rows)
    }


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_too_many_properties(destination_name: str) -> None:
    with pytest.raises(ResourceExtractionError):
        with patch(
            "sources.hubspot.ENTITY_PROPERTIES", {"contact": ["property"] * 500}
        ):
            source = hubspot(api_key="fake_key", include_history=True)
            list(source.with_resources("contacts"))


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_only_users_properties(destination_name: str, mock_response) -> None:
    def fake_get(url: str, *args, **kwargs) -> Any:  # type: ignore[no-untyped-def]
        if "/properties" in url:
            return mock_response(json_data=mock_contacts_properties)
        return mock_response(json_data=mock_contacts_with_history)

    expected_props = "prop1,prop2,prop3"
    props = ["prop1", "prop2", "prop3"]

    pipeline = dlt.pipeline(
        pipeline_name="hubspot",
        destination=destination_name,
        dataset_name="hubspot_data",
        dev_mode=True,
    )
    source = hubspot(api_key="fake_key", include_custom_props=False)
    with patch("sources.hubspot.ENTITY_PROPERTIES", {"contact": props}):
        with patch("dlt.sources.helpers.requests.get", side_effect=fake_get) as m:
            load_info = pipeline.run(source.with_resources("contacts"))

        assert_load_info(load_info)

        m.assert_has_calls(
            [
                call(
                    urljoin(BASE_URL, CRM_CONTACTS_ENDPOINT),
                    headers=ANY,
                    params={
                        "properties": expected_props,
                        "limit": 100,
                    },
                ),
            ]
        )


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_only_default_props(destination_name: str, mock_response) -> None:
    def fake_get(url: str, *args, **kwargs) -> Any:  # type: ignore[no-untyped-def]
        if "/properties" in url:
            return mock_response(json_data=mock_contacts_properties)
        return mock_response(json_data=mock_contacts_with_history)

    expected_props = ",".join(DEFAULT_CONTACT_PROPS)

    pipeline = dlt.pipeline(
        pipeline_name="hubspot",
        destination=destination_name,
        dataset_name="hubspot_data",
        dev_mode=True,
    )
    source = hubspot(api_key="fake_key", include_custom_props=False)

    with patch("dlt.sources.helpers.requests.get", side_effect=fake_get) as m:
        load_info = pipeline.run(source.with_resources("contacts"))

    assert_load_info(load_info)

    m.assert_has_calls(
        [
            call(
                urljoin(BASE_URL, CRM_CONTACTS_ENDPOINT),
                headers=ANY,
                params={
                    "properties": expected_props,
                    "limit": 100,
                },
            ),
        ]
    )


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_users_and_custom_properties(destination_name: str, mock_response) -> None:
    def fake_get(url: str, *args, **kwargs) -> Any:  # type: ignore[no-untyped-def]
        if "/properties" in url:
            return mock_response(json_data=mock_contacts_properties)
        return mock_response(json_data=mock_contacts_with_history)

    expected_props = "address,annualrevenue,associatedcompanyid,associatedcompanylastupdated,city,closedate,company,company_size,country,createdate,currentlyinworkflow,date_of_birth,days_to_close,degree,email,engagements_last_meeting_booked,engagements_last_meeting_booked_campaign,engagements_last_meeting_booked_medium,engagements_last_meeting_booked_source,fax,field_of_study,first_conversion_date,first_conversion_event_name,first_deal_created_date,firstname,followercount,gender,graduation_date,hubspot_owner_assigneddate,hubspot_owner_id,hubspot_team_id,hubspotscore,industry,ip_city,ip_country,ip_country_code,ip_latlon,ip_state,ip_state_code,ip_zipcode,job_function,jobtitle,kloutscoregeneral,lastmodifieddate,lastname,lifecyclestage,linkedinbio,linkedinconnections,marital_status,message,military_status,mobilephone,notes_last_contacted,notes_last_updated,notes_next_activity_date,num_associated_deals,num_contacted_notes,num_conversion_events,num_notes,num_unique_conversion_events,numemployees,owneremail,ownername,phone,photo,prop1,prop2,prop3,recent_conversion_date,recent_conversion_event_name,recent_deal_amount,recent_deal_close_date,relationship_status,salutation,school,seniority,start_date,state,surveymonkeyeventlastupdated,total_revenue,twitterbio,twitterhandle,twitterprofilephoto,webinareventlastupdated,website,work_email,zip"
    props = ["prop1", "prop2", "prop3"]

    pipeline = dlt.pipeline(
        pipeline_name="hubspot",
        destination=destination_name,
        dataset_name="hubspot_data",
        dev_mode=True,
    )
    source = hubspot(api_key="fake_key")
    with patch("sources.hubspot.ENTITY_PROPERTIES", {"contact": props}):
        with patch("dlt.sources.helpers.requests.get", side_effect=fake_get) as m:
            load_info = pipeline.run(source.with_resources("contacts"))

        assert_load_info(load_info)

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
                    params={
                        "properties": expected_props,
                        "limit": 100,
                    },
                ),
            ]
        )


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_custom_only_properties(destination_name: str, mock_response) -> None:
    def fake_get(url: str, *args, **kwargs) -> Any:  # type: ignore[no-untyped-def]
        if "/properties" in url:
            return mock_response(json_data=mock_contacts_properties)
        return mock_response(json_data=mock_contacts_with_history)

    pipeline = dlt.pipeline(
        pipeline_name="hubspot",
        destination=destination_name,
        dataset_name="hubspot_data",
        dev_mode=True,
    )
    source = hubspot(api_key="fake_key")

    with patch("dlt.sources.helpers.requests.get", side_effect=fake_get) as m:
        load_info = pipeline.run(source.with_resources("contacts"))

    assert_load_info(load_info)

    for prop in m.mock_calls[1][2]["params"]["properties"].split(","):
        assert not prop.startswith("hs_") or prop == "hs_object_id"


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_all_resources(destination_name: str) -> None:
    pipeline = dlt.pipeline(
        pipeline_name="hubspot",
        destination=destination_name,
        dataset_name="hubspot_data",
        dev_mode=True,
    )
    load_info = pipeline.run(
        hubspot(include_history=True).with_resources(
            "contacts", "deals", "companies", "contacts_property_history"
        )
    )

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
        == {"companies": 4, "contacts": 3, "deals": 2}
    )

    history_table_names = [
        t["name"]
        for t in pipeline.default_schema.data_tables()
        if t["name"].endswith("_property_history")
    ]
    table_counts = load_table_counts(pipeline, *history_table_names)
    # Check history tables
    # NOTE: this value is increasing... maybe we should start testing ranges
    assert table_counts["contacts_property_history"] >= 76

    # Check property from a couple of contacts against known data
    with pipeline.sql_client() as client:
        rows = [
            list(row)
            for row in client.execute_sql(
                """
                SELECT ch.property_name, ch.value, ch.source_type, ch.timestamp
                FROM contacts
                JOIN
                contacts_property_history AS ch ON contacts.id = ch.object_id
                WHERE contacts.id IN ('51', '1') AND property_name = 'email';
                """
            )
        ]

    for row in rows:
        row[-1] = pendulum.instance(row[-1])

    assert set((tuple(row) for row in rows)) == set(
        [
            (
                "email",
                "emailmaria@hubspot.com",
                "API",
                pendulum.parse("2023-06-28 13:55:47.558"),
            ),
            (
                "email",
                "thisisnewemail@hubspot.com",
                "CRM_UI",
                pendulum.parse("2023-07-01 23:34:57.837"),
            ),
            (
                "email",
                "bh@hubspot.com",
                "API",
                pendulum.parse("2023-06-28 13:55:47.558"),
            ),
        ]
    )


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_event_resources(destination_name: str) -> None:
    pipeline = dlt.pipeline(
        pipeline_name="hubspot",
        destination=destination_name,
        dataset_name="hubspot_data",
        dev_mode=True,
    )
    load_info = pipeline.run(
        hubspot_events_for_objects("company", ["7086461639", "7086464459"])
    )
    print(load_info)
    assert_load_info(load_info)


def test_chunk_properties():
    properties = ["prop1", "prop2", "prop3", "prop4"]
    max_length = 12
    expected = [["prop1", "prop2"], ["prop3", "prop4"]]
    result = list(chunk_properties(properties, max_length))
    assert result == expected

    properties = ["prop1", "prop2"]
    max_length = len("prop1,prop2")
    expected = [["prop1", "prop2"]]
    result = list(chunk_properties(properties, max_length))
    assert result == expected

    properties = ["p1", "p2", "p3", "p4", "p5"]
    max_length = 5  # Should accommodate 'p1,p2'
    expected = [["p1", "p2"], ["p3", "p4"], ["p5"]]
    result = list(chunk_properties(properties, max_length))
    assert result == expected

import pytest
from unittest import mock
from typing import Dict, Any
from pathlib import Path
import json

import requests_mock
import dlt
from dlt.common import pendulum
from dlt.common.pipeline import TSourceState
from dlt.common.schema import Schema
from dlt.sources.helpers import requests

from sources.pipedrive import pipedrive_source, leads
from tests.utils import (
    ALL_DESTINATIONS,
    assert_load_info,
    assert_query_data,
    load_table_counts,
)
from sources.pipedrive.helpers.custom_fields_munger import (
    update_fields_mapping,
    rename_fields,
)


ALL_RESOURCES = {
    "custom_fields_mapping",
    "activities",
    "activity_types",
    "deals",
    "deals_flow",
    "deals_participants",
    "files",
    "filters",
    "notes",
    "persons",
    "organizations",
    "pipelines",
    "products",
    "stages",
    "users",
    "leads",
}

# we have no data in our test account (only leads)
# TESTED_RESOURCES = (
#     ALL_RESOURCES
#     - {  # Currently there is no test data for these resources
#         "pipelines",
#         "stages",
#         "filters",
#         "files",
#         "activity_types",
#         "notes",
#     }
# )

TESTED_RESOURCES = {"custom_fields_mapping", "leads", }


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_all_resources(destination_name: str) -> None:
    # mind the full_refresh flag - it makes sure that data is loaded to unique dataset. this allows you to run the tests on the same database in parallel
    # configure the pipeline with your destination details
    pipeline = dlt.pipeline(
        pipeline_name="pipedrive",
        destination=destination_name,
        dataset_name="pipedrive_data",
        full_refresh=True,
    )
    load_info = pipeline.run(pipedrive_source())
    print(load_info)
    assert_load_info(load_info)

    # ALl root tables exist in schema
    schema_tables = set(pipeline.default_schema.tables)
    assert schema_tables > TESTED_RESOURCES - {"deals_flow"}
    # assert "deals_flow_activity" in schema_tables
    # assert "deals_flow_deal_change" in schema_tables


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_leads_resource_incremental(destination_name: str) -> None:
    pipeline = dlt.pipeline(
        pipeline_name="pipedrive",
        destination=destination_name,
        dataset_name="pipedrive_data",
        full_refresh=True,
    )
    # Load all leads from beginning
    data = leads()
    info = pipeline.run(data, write_disposition="append")
    assert_load_info(info)

    counts = load_table_counts(pipeline, "leads")
    assert counts["leads"] > 1

    # Run again and assert no new data is added
    data = leads()
    info = pipeline.run(data, write_disposition="append")
    assert_load_info(info, expected_load_packages=0)

    assert load_table_counts(pipeline, "leads") == counts


@pytest.mark.skip("We have no data in our test account.")
@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_custom_fields_munger(destination_name: str) -> None:
    pipeline = dlt.pipeline(
        pipeline_name="pipedrive",
        destination=destination_name,
        dataset_name="pipedrive_data",
        full_refresh=True,
    )

    load_info = pipeline.run(
        pipedrive_source().with_resources(
            "persons", "products", "deals", "custom_fields_mapping"
        )
    )

    print(load_info)
    assert_load_info(load_info)

    schema = pipeline.default_schema

    raw_query_string = (
        "SELECT {fields} FROM {table} WHERE {condition} ORDER BY {fields}"
    )

    # test person custom fields data munging
    condition = "name IN ('TEST FIELD 1', 'TEST FIELD 2 ') AND endpoint = 'person'"
    query_string = raw_query_string.format(
        fields="normalized_name", table="custom_fields_mapping", condition=condition
    )
    table_data = ["test_field_1", "test_field_2"]
    assert_query_data(pipeline, query_string, table_data)

    # test persons' custom fields data munging

    persons_table = schema.get_table("persons")
    assert "test_field_1" in persons_table["columns"]
    assert "test_field_2" in persons_table["columns"]
    assert "single_option" in persons_table["columns"]

    condition = "test_field_1 = 'Test Value 1'"
    query_string = raw_query_string.format(
        fields="test_field_1", table="persons", condition=condition
    )
    table_data = ["Test Value 1"]
    assert_query_data(pipeline, query_string, table_data)

    condition = "test_field_2 = 'Test Value 2'"
    query_string = raw_query_string.format(
        fields="test_field_2", table="persons", condition=condition
    )
    table_data = ["Test Value 2"]
    assert_query_data(pipeline, query_string, table_data)

    # Test enum field is mapped to its string value
    condition = "single_option = 'aaa'"
    query_string = (
        raw_query_string.format(
            fields="single_option", table="persons", condition=condition
        )
        + " LIMIT 1"
    )
    assert_query_data(pipeline, query_string, ["aaa"])

    # Test standard person enum field have values mapped
    condition = "label = 'Hot lead'"
    query_string = (
        raw_query_string.format(fields="label", table="persons", condition=condition)
        + " LIMIT 1"
    )
    assert_query_data(pipeline, query_string, ["Hot lead"])

    # Test set field is mapped in value table
    person_multiple_options_table = schema.get_table("persons__multiple_options")
    assert "value" in person_multiple_options_table["columns"]
    query_string = (
        raw_query_string.format(
            fields="value", table="persons__multiple_options", condition="value = 'abc'"
        )
        + " LIMIT 1"
    )
    assert_query_data(pipeline, query_string, ["abc"])

    # Test deal field label
    condition = "value = 'label with, comma'"
    query_string = (
        raw_query_string.format(
            fields="value", table="deals__label", condition=condition
        )
        + " LIMIT 1"
    )
    assert_query_data(pipeline, query_string, ["label with, comma"])

    # test product custom fields data munging

    condition = "name = 'TEST FIELD 1' AND endpoint='product'"
    query_string = raw_query_string.format(
        fields="normalized_name", table="custom_fields_mapping", condition=condition
    )
    table_data = ["test_field_1"]
    assert_query_data(pipeline, query_string, table_data)

    # test products' custom fields data munging

    products_table = schema.get_table("products")
    assert "test_field_1" in products_table["columns"]

    condition = "test_field_1 = 'Test Value 1'"
    query_string = raw_query_string.format(
        fields="test_field_1", table="products", condition=condition
    )
    table_data = ["Test Value 1"]
    assert_query_data(pipeline, query_string, table_data)

    # test custom fields mapping

    custom_fields_mapping = schema.get_table("custom_fields_mapping")
    assert "endpoint" in custom_fields_mapping["columns"]
    assert "hash_string" in custom_fields_mapping["columns"]
    assert "name" in custom_fields_mapping["columns"]
    assert "normalized_name" in custom_fields_mapping["columns"]

    condition = (
        "endpoint = 'person' AND normalized_name IN ('test_field_1', 'test_field_2')"
    )
    query_string = raw_query_string.format(
        fields="name", table="custom_fields_mapping", condition=condition
    )
    table_data = ["TEST FIELD 1", "TEST FIELD 2 "]
    assert_query_data(pipeline, query_string, table_data)

    condition = "endpoint = 'product' AND normalized_name = 'test_field_1'"
    query_string = raw_query_string.format(
        fields="name", table="custom_fields_mapping", condition=condition
    )
    table_data = ["TEST FIELD 1"]
    assert_query_data(pipeline, query_string, table_data)

    print(pipeline.state)


def test_since_timestamp() -> None:
    """since_timestamp is coerced correctly to UTC implicit ISO timestamp and passed to endpoint function"""
    with mock.patch(
        "sources.pipedrive.helpers.pages.get_pages",
        autospec=True,
        return_value=iter([]),
    ) as m:
        pipeline = dlt.pipeline(pipeline_name="pipedrive", full_refresh=True)
        incremental_source = pipedrive_source(
            since_timestamp="1986-03-03T04:00:00+04:00"
        ).with_resources("persons")
        pipeline.extract(incremental_source)

    assert (
        m.call_args.kwargs["extra_params"]["since_timestamp"] == "1986-03-03 00:00:00"
    )

    with mock.patch(
        "sources.pipedrive.helpers.pages.get_pages",
        autospec=True,
        return_value=iter([]),
    ) as m:
        pipeline = dlt.pipeline(pipeline_name="pipedrive", full_refresh=True)
        pipeline.extract(pipedrive_source(since_timestamp=pendulum.parse("1986-03-03T04:00:00+04:00")).with_resources("persons"))  # type: ignore[arg-type]

    assert (
        m.call_args.kwargs["extra_params"]["since_timestamp"] == "1986-03-03 00:00:00"
    )

    with mock.patch(
        "sources.pipedrive.helpers.pages.get_pages",
        autospec=True,
        return_value=iter([]),
    ) as m:
        pipeline = dlt.pipeline(pipeline_name="pipedrive", full_refresh=True)
        pipeline.extract(pipedrive_source().with_resources("persons"))

    assert (
        m.call_args.kwargs["extra_params"]["since_timestamp"] == "1970-01-01 00:00:00"
    )


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_incremental(destination_name: str) -> None:
    pipeline = dlt.pipeline(
        pipeline_name="pipedrive",
        destination=destination_name,
        dataset_name="pipedrive",
        full_refresh=True,
    )

    # No items older than initial value are loaded
    ts = pendulum.parse("2023-03-15T10:17:44Z")
    source = pipedrive_source(since_timestamp=ts).with_resources("leads", "custom_fields_mapping")  # type: ignore[arg-type]

    pipeline.run(source)

    with pipeline.sql_client() as c:
        with c.execute_query("SELECT min(update_time) FROM leads") as cur:
            row = cur.fetchone()

    assert pendulum.instance(row[0]) >= ts  # type: ignore

    # Just check that incremental state is created
    state: TSourceState = pipeline.state  # type: ignore[assignment]
    assert isinstance(
        state["sources"]["pipedrive"]["resources"]["leads"]["incremental"][
            "update_time"
        ],
        dict,
    )


def test_resource_settings() -> None:
    source = pipedrive_source()

    resource_names = set(source.resources)

    assert resource_names == ALL_RESOURCES

    assert source.resources["custom_fields_mapping"].write_disposition == "replace"

    s = source.discover_schema()

    for rs_name in resource_names - {"custom_fields_mapping"}:
        rs = source.resources[rs_name]
        assert rs.write_disposition == "merge"
        assert s.tables[rs_name]["columns"]["id"]["primary_key"] is True


def test_update_fields_new_enum_field() -> None:
    mapping: Dict[str, Any] = {}
    items = [
        {
            "edit_flag": True,
            "name": "custom_field_1",
            "key": "random_hash_1",
            "field_type": "enum",
            "options": [
                {"id": 3, "label": "a"},
                {"id": 4, "label": "b"},
                {"id": 5, "label": "c"},
            ],
        }
    ]

    with mock.patch.object(dlt.current, "source_schema", return_value=Schema("test")):
        result = update_fields_mapping(items, mapping)

    assert result == {
        "random_hash_1": {
            "name": "custom_field_1",
            "normalized_name": "custom_field_1",
            "field_type": "enum",
            "options": {"3": "a", "4": "b", "5": "c"},
        }
    }


def test_update_fields_add_enum_field_options() -> None:
    mapping: Dict[str, Any] = {
        "random_hash_1": {
            "name": "custom_field_1",
            "normalized_name": "custom_field_1",
            "options": {"3": "a", "4": "b", "5": "c"},
        }
    }
    items = [
        {
            "edit_flag": True,
            "name": "custom_field_1",
            "key": "random_hash_1",
            "field_type": "enum",
            "options": [
                {"id": 3, "label": "a"},
                {"id": 4, "label": "previously_b"},
                {"id": 5, "label": "c"},
                {"id": 7, "label": "d"},
            ],
        }
    ]

    with mock.patch.object(dlt.current, "source_schema", return_value=Schema("test")):
        result = update_fields_mapping(items, mapping)

    assert result["random_hash_1"]["options"] == {
        "3": "a",
        "4": "b",
        "5": "c",
        "7": "d",
    }


def test_rename_fields_with_enum() -> None:
    data_item = {"random_hash_1": "42", "id": 44, "name": "asdf"}
    mapping = {
        "random_hash_1": {
            "name": "custom_field_1",
            "normalized_name": "custom_field_1",
            "field_type": "enum",
            "options": {"3": "a", "42": "b", "5": "c"},
        }
    }

    result = rename_fields([data_item], mapping)

    assert result == [{"custom_field_1": "b", "id": 44, "name": "asdf"}]


def test_rename_fields_with_set() -> None:
    data_item = {"random_hash_1": "42,44,23", "id": 44, "name": "asdf"}
    mapping = {
        "random_hash_1": {
            "name": "custom_field_1",
            "normalized_name": "custom_field_1",
            "field_type": "set",
            "options": {"44": "a", "42": "b", "522": "c", "23": "c"},
        }
    }

    result = rename_fields([data_item], mapping)

    assert result == [{"custom_field_1": ["b", "a", "c"], "id": 44, "name": "asdf"}]


def test_recents_none_data_items_from_recents() -> None:
    """Pages from /recents sometimes contain `None` data items which cause errors.
    Reproduces this with a mocked response. Simply verify that extract runs without exceptions, meaning nones are filtered out.
    """
    mock_data = json.loads(
        Path("./tests/pipedrive/recents_response_with_null.json").read_text(
            encoding="utf8"
        )
    )
    with requests_mock.Mocker(session=requests.client.session, real_http=True) as m:
        m.register_uri("GET", "/v1/recents", json=mock_data)
        pipeline = dlt.pipeline(
            pipeline_name="pipedrive", dataset_name="pipedrive_data", full_refresh=True
        )
        pipeline.extract(
            pipedrive_source().with_resources("persons", "custom_fields_mapping")
        )

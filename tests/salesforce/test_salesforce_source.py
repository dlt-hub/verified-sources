from typing import OrderedDict
import inspect
from tests.utils import ALL_DESTINATIONS, assert_load_info, load_table_counts
import pytest
from dlt.common import pendulum
import dlt
from sources.salesforce import salesforce_source
from sources.salesforce.helpers.records import _process_record
from simple_salesforce import Salesforce
from dlt.sources.helpers.requests import Session


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_all_resources(destination_name: str) -> None:
    expected_tables = [
        "account",
        "campaign",
        "contact",
        "lead",
        "opportunity",
        "pricebook_2",
        "pricebook_entry",
        "product_2",
        "sf_user",
        "user_role",
    ]

    pipeline = dlt.pipeline(
        pipeline_name="salesforce",
        destination=destination_name,
        dataset_name="salesforce_data",
        dev_mode=True,
    )

    source = salesforce_source()

    # Execute the pipeline
    load_info = pipeline.run(source)
    assert_load_info(load_info)

    table_names = [t["name"] for t in pipeline.default_schema.data_tables()]
    table_counts = load_table_counts(pipeline, *table_names)

    assert set(table_counts.keys()) >= set(expected_tables)
    assert table_counts["sf_user"] == 8
    assert table_counts["account"] == 19
    assert table_counts["campaign"] == 4
    assert table_counts["contact"] == 26
    assert table_counts["lead"] == 22
    assert table_counts["opportunity"] == 31
    assert table_counts["pricebook_2"] == 2
    assert table_counts["pricebook_entry"] == 34
    assert table_counts["product_2"] == 17
    assert table_counts["user_role"] == 18

    # Execute the pipeline
    source = salesforce_source()

    # Execute the pipeline
    load_info = pipeline.run(source)

    table_names = [t["name"] for t in pipeline.default_schema.data_tables()]
    new_table_counts = load_table_counts(pipeline, *table_names)
    assert new_table_counts == table_counts


@pytest.mark.parametrize(
    "api_type",
    ["bulk", "standard"],
)
def test_process_record(api_type: str) -> None:
    date_fields = {"LastModifiedDate", "SystemModstamp", "CreatedDate"}
    sample_data = OrderedDict(
        [
            (
                "attributes",
                OrderedDict(
                    [
                        ("type", "Contact"),
                        (
                            "url",
                            "/services/data/v59.0/sobjects/Contact/003Wz00000E9PTuIAN",
                        ),
                    ]
                ),
            ),
            ("Id", "003Wz00000E9PTuIAN"),
            (
                "CreatedDate",
                "2025-08-06T14:16:08.000+0000"
                if api_type == "standard"
                else 1754489768000,
            ),
            (
                "LastModifiedDate",
                "2025-08-06T14:16:08.000+0000"
                if api_type == "standard"
                else 1754489768000,
            ),
            (
                "SystemModstamp",
                "2025-08-06T14:16:08.000+0000"
                if api_type == "standard"
                else 1754489768000,
            ),
        ]
    )

    processed_record = _process_record(sample_data, date_fields, api_type=api_type)
    assert "attributes" not in processed_record
    for date_field in date_fields:
        try:
            pendulum.DateTime.strptime(
                processed_record[date_field], "%Y-%m-%dT%H:%M:%S.%fZ"
            )
        except ValueError:
            raise AssertionError("Value does not match format %Y-%m-%dT%H:%M:%S.%fZ")

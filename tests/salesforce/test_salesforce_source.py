import os
from typing import Any, Dict, List

import dlt
import pytest

from sources.salesforce import SOURCE_SOBJECTS, salesforce_source, salesforce_table
from sources.salesforce.helpers.query import TQueryApi
from sources.salesforce.helpers.table import TTableBackend
from tests.salesforce.seed import data
from tests.salesforce.utils import assert_dlt_types_loaded, count_rows
from tests.utils import ALL_DESTINATIONS, assert_load_info, load_table_counts

# tests below read the test org seeded with tests/salesforce/seed/seed.py, see README.md


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_all_resources(destination_name: str) -> None:
    # run all query jobs at once
    os.environ["EXTRACT__WORKERS"] = str(len(SOURCE_SOBJECTS))
    pipeline = dlt.pipeline(
        pipeline_name="salesforce",
        destination=destination_name,
        dataset_name="salesforce_data",
        dev_mode=True,
    )
    load_info = pipeline.run(salesforce_source())
    assert_load_info(load_info)

    table_names = [t["name"] for t in pipeline.default_schema.data_tables()]
    table_counts = load_table_counts(pipeline, *table_names)
    # the org has its own users, roles and sample products, only seeded records are counted
    assert table_counts["sf_user"] > 0
    assert table_counts["user_role"] >= len(data.CREATED["UserRole"]["records"])
    seeded: Dict[str, List[Dict[str, Any]]] = {
        "account": data.ACCOUNTS,
        "campaign": data.CAMPAIGNS,
        "contact": data.CONTACTS,
        "lead": data.LEADS,
        "opportunity": data.OPPORTUNITIES,
        "pricebook_2": data.PRICEBOOKS,
        "pricebook_entry": data.PRICEBOOK_ENTRIES,
        "product_2": data.PRODUCTS,
    }
    for table_name, records in seeded.items():
        assert count_rows(pipeline, table_name, "dlt_seed_id__c IS NOT NULL") == len(
            records
        )
    for table_name, sobject in (("task", "Task"), ("event", "Event")):
        assert count_rows(pipeline, table_name, "subject LIKE 'dlt %'") == len(
            data.CREATED[sobject]["records"]
        )

    columns = pipeline.default_schema.get_table_columns("account")
    assert columns["id"]["primary_key"] is True
    assert columns["system_modstamp"]["data_type"] == "timestamp"
    assert columns["annual_revenue"]["data_type"] == "decimal"
    assert columns["number_of_employees"]["data_type"] == "bigint"
    assert columns["is_deleted"]["hard_delete"] is True

    # Salesforce accepts the datetime literals generated from the last cursor values
    assert_load_info(pipeline.run(salesforce_source()))
    assert load_table_counts(pipeline, *table_names) == table_counts


@pytest.mark.parametrize(
    "backend,defer_table_reflect",
    [("python", False), ("pyarrow", False), ("python", True)],
    ids=["python", "pyarrow", "python-deferred"],
)
def test_salesforce_table_dlt_types(
    backend: TTableBackend, defer_table_reflect: bool
) -> None:
    # compares values of all Salesforce field types, decimals are tested with captured data
    pipeline = dlt.pipeline(
        pipeline_name="salesforce_dlt_types",
        destination="duckdb",
        dataset_name="salesforce_data",
        dev_mode=True,
    )
    assert_load_info(
        pipeline.run(
            salesforce_table(
                sobject=data.DLT_TYPES,
                backend=backend,
                defer_table_reflect=defer_table_reflect,
            )
        )
    )
    # also checks that deleted records are removed by merge with the hard delete hint
    assert_dlt_types_loaded(pipeline, double_as_decimal=False)


@pytest.mark.parametrize("include_deleted", [False, True], ids=["query", "query_all"])
def test_salesforce_table_filter_deleted_records(include_deleted: bool) -> None:
    resource = salesforce_table(
        sobject=data.DLT_TYPES,
        incremental=None,
        include_deleted=include_deleted,
        soql_filter="Category__c != 'plain'",
        included_columns=["Id", "Seed_Id__c", "IsDeleted"],
        backend="python",
    )
    rows = list(resource)
    active = sorted(row["Seed_Id__c"] for row in rows if not row["IsDeleted"])
    assert active == sorted(
        record["Seed_Id__c"]
        for record in data.DLT_TYPES_RECORDS
        if record["Category__c"] != "plain"
    )
    # deleted records are purged from the recycle bin after 15 days, so their count is not fixed
    deleted = [row for row in rows if row["IsDeleted"]]
    if not include_deleted:
        assert deleted == []


@pytest.mark.parametrize("api", ["bulk", "rest"])
def test_salesforce_table_pages(api: TQueryApi) -> None:
    # every result page is a separate Arrow table, REST API pages have at least 200 records
    tables = list(
        salesforce_table(sobject=data.PAGES, chunk_size=200, api=api, backend="pyarrow")
    )
    assert len(tables) >= len(data.PAGES_RECORDS) // 200
    values = sorted(
        value for table in tables for value in table.column("Value__c").to_pylist()
    )
    assert values == [float(record["Value__c"]) for record in data.PAGES_RECORDS]


def test_salesforce_table_bulk_api_not_supported() -> None:
    # the Bulk API does not support TaskStatus, records are read with the REST API
    statuses = list(salesforce_table(sobject="TaskStatus", backend="python"))
    assert statuses and all(isinstance(row["MasterLabel"], str) for row in statuses)


def test_salesforce_table_chunked_loading() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="salesforce_chunked",
        destination="duckdb",
        dataset_name="salesforce_data",
        dev_mode=True,
    )
    pages = salesforce_table(
        sobject=data.PAGES,
        chunk_size=250,
        api="rest",
        soql_filter="Value__c <= 600",
        incremental=dlt.sources.incremental(
            "Value__c",
            row_order="asc",  # load in order so the next run continues where the last stopped
            range_start="open",  # values are unique so no records are loaded twice
        ),
    )
    # every run loads a single page of max 250 records until there are no more records
    runs = 0
    while not pipeline.run(pages.add_limit(1)).is_empty:
        runs += 1
        assert runs < 10
    assert runs == 3
    table_name = pipeline.default_schema.naming.normalize_path(data.PAGES)
    assert count_rows(pipeline, table_name, "1 = 1") == 600

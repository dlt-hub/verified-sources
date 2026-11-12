import os
import uuid
from datetime import datetime, timedelta, timezone  # noqa: I251
from time import sleep
from typing import Any, Dict, List, Optional, Set

import dlt
import pytest
from dlt.sources import DltResource

from sources.salesforce import SOURCE_SOBJECTS, salesforce_source, salesforce_table
from sources.salesforce.helpers.query import TQueryApi
from sources.salesforce.helpers.table import TTableBackend
from tests.salesforce.seed import data
from tests.salesforce.utils import (
    assert_dlt_types_loaded,
    count_rows,
    delete_records,
    last_row_count,
    load_rows,
    salesforce_client,
    wait_for_org_clock,
    write_records,
)
from tests.utils import (
    ALL_DESTINATIONS,
    assert_load_info,
    load_table_counts,
    skip_if_not_active,
)

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
        "opportunity_line_item": data.OPPORTUNITY_LINE_ITEMS,
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


@skip_if_not_active("duckdb")
@pytest.mark.parametrize("backend", ["python", "pyarrow"])
@pytest.mark.parametrize("api", ["bulk", "rest"])
def test_seeded_full_and_incremental(backend: TTableBackend, api: TQueryApi) -> None:
    """Loads `DltPages__c` in full, again, and in windows of `Changed_At__c`. Expected counts
    come from the seed data"""
    changed_at = sorted(record["Changed_At__c"] for record in data.PAGES_RECORDS)
    seed_ids = sorted((record["Seed_Id__c"],) for record in data.PAGES_RECORDS)

    def pages(**incremental_args: Any) -> DltResource:
        return salesforce_table(
            sobject=data.PAGES,
            backend=backend,
            api=api,
            incremental=dlt.sources.incremental("Changed_At__c", **incremental_args),
        )

    def make_pipeline(name: str) -> dlt.Pipeline:
        return dlt.pipeline(
            pipeline_name=name, destination="duckdb", dataset_name=name, dev_mode=True
        )

    pipeline = make_pipeline("salesforce_seeded")
    assert_load_info(pipeline.run(pages()))
    assert last_row_count(pipeline, data.PAGES) == len(changed_at)
    assert load_rows(pipeline, data.PAGES, "Seed_Id__c") == seed_ids

    # merge on a timestamp turns off deduplication, the closed range reads the last value again
    assert_load_info(pipeline.run(pages()))
    assert last_row_count(pipeline, data.PAGES) == changed_at.count(changed_at[-1])
    assert load_rows(pipeline, data.PAGES, "Seed_Id__c") == seed_ids

    # bounds are seeded values, each loads in one window. with `min` the windows go back in time
    quarters = changed_at[250::250]
    for last_value_func, bounds in (
        (max, [changed_at[0], *quarters, changed_at[-1] + timedelta(seconds=1)]),
        (min, [changed_at[-1], *quarters[::-1], changed_at[0] - timedelta(seconds=1)]),
    ):
        backfill = make_pipeline(f"salesforce_seeded_{last_value_func.__name__}")
        for start, end in zip(bounds, bounds[1:]):
            expected = sum(
                start <= value < end if last_value_func is max else end < value <= start
                for value in changed_at
            )
            assert expected > 0
            assert_load_info(
                backfill.run(
                    pages(
                        initial_value=start,
                        end_value=end,
                        last_value_func=last_value_func,
                        range_start="closed",
                        range_end="open",
                    )
                )
            )
            assert last_row_count(backfill, data.PAGES) == expected, (start, end)
        # windows with an end value do not store incremental state
        assert "sources" not in backfill.state
        assert load_rows(backfill, data.PAGES, "Seed_Id__c") == seed_ids


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
@pytest.mark.parametrize("backend", ["python", "pyarrow"])
def test_incremental_changes(destination_name: str, backend: TTableBackend) -> None:
    """Adds, updates and deletes `DltChanges__c` records between runs of the default incremental.
    Expected values come from the records the test writes"""
    client = salesforce_client()
    run_id = uuid.uuid4().hex
    run_filter = f"Run_Id__c = '{run_id}'"
    pipeline = dlt.pipeline(
        pipeline_name="salesforce_changes",
        destination=destination_name,
        dataset_name="salesforce_changes",
        dev_mode=True,
    )
    ids: Dict[str, str] = {}

    def create(*names: str) -> None:
        records = [{"Name": name, "Run_Id__c": run_id, "Value__c": 1} for name in names]
        ids.update(zip(names, write_records(client, "POST", data.CHANGES, records)))

    def run() -> int:
        """Loads the records of this test, returns rows read"""
        wait_for_org_clock(client, data.CHANGES, run_filter)
        changes = salesforce_table(
            sobject=data.CHANGES, backend=backend, lag=0, soql_filter=run_filter
        )
        assert_load_info(pipeline.run(changes))
        return last_row_count(pipeline, data.CHANGES)

    try:
        # the last write before a run is alone in its second. the closed range of the next run
        # reads it again
        create("a1", "a2", "a3", "a4")
        sleep(1.5)
        create("a5")
        assert run() == 5
        assert load_rows(pipeline, data.CHANGES, "Name", "Value__c") == [
            ("a1", 1.0),
            ("a2", 1.0),
            ("a3", 1.0),
            ("a4", 1.0),
            ("a5", 1.0),
        ]

        write_records(client, "PATCH", data.CHANGES, [{"Id": ids["a1"], "Value__c": 2}])
        delete_records(client, [ids.pop("a2")])
        create("b1")
        sleep(1.5)
        create("b2")
        # a5 again, updated a1, deleted a2, b1 and b2
        assert run() == 5
        changed = [
            ("a1", 2.0),
            ("a3", 1.0),
            ("a4", 1.0),
            ("a5", 1.0),
            ("b1", 1.0),
            ("b2", 1.0),
        ]
        assert load_rows(pipeline, data.CHANGES, "Name", "Value__c") == changed

        # b2 again
        assert run() == 1
        assert load_rows(pipeline, data.CHANGES, "Name", "Value__c") == changed
    finally:
        if ids:
            delete_records(client, list(ids.values()))


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
        write_disposition="replace",
        include_deleted=include_deleted,
        soql_filter="Category__c != 'plain'",
        included_columns=["Id", "Seed_Id__c", "IsDeleted"],
        backend="python",
    )
    rows = list(resource)
    # python rows hold the CSV strings, the normalizer coerces them
    active = sorted(row["Seed_Id__c"] for row in rows if row["IsDeleted"] == "false")
    assert active == sorted(
        record["Seed_Id__c"]
        for record in data.DLT_TYPES_RECORDS
        if record["Category__c"] != "plain"
    )
    # deleted records are purged from the recycle bin after 15 days, so their count is not fixed
    deleted = [row for row in rows if row["IsDeleted"] == "true"]
    if not include_deleted:
        assert deleted == []


def test_salesforce_table_deleted_line_items() -> None:
    # line items deleted with their opportunity stay in the recycle bin for 15 days, so queryAll
    # returns them. line items deleted on their own skip the recycle bin and are purged within days
    rows = list(
        salesforce_table(
            sobject="OpportunityLineItem",
            write_disposition="replace",
            soql_filter=f"{data.SEED_ID_FIELD} LIKE '{data.DELETED_LINE_ITEM_PREFIX}%'",
            included_columns=["Id", "IsDeleted", data.SEED_ID_FIELD],
            backend="python",
        )
    )
    assert all(row["IsDeleted"] == "true" for row in rows)
    kinds_by_run: Dict[str, Set[str]] = {}
    for row in rows:
        kind, run_id = row[data.SEED_ID_FIELD][
            len(data.DELETED_LINE_ITEM_PREFIX) :
        ].split("-")
        kinds_by_run.setdefault(run_id, set()).add(kind)
    assert all("cascade" in kinds for kinds in kinds_by_run.values())


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


@skip_if_not_active("duckdb")
@pytest.mark.parametrize("backend", ["python", "pyarrow"])
@pytest.mark.parametrize("api", ["bulk", "rest"])
@pytest.mark.parametrize(
    "last_value_func,initial_value",
    [
        (max, None),
        (min, None),
        (max, datetime(2023, 1, 1, tzinfo=timezone.utc)),
        (min, datetime(2025, 1, 1, tzinfo=timezone.utc)),
    ],
    ids=["max", "min", "max_initial_value", "min_initial_value"],
)
def test_seeded_chunks(
    backend: TTableBackend,
    api: TQueryApi,
    last_value_func: Any,
    initial_value: Optional[datetime],
) -> None:
    # 300 leaves a partial last chunk of 100
    chunk_size = 300
    expected = sorted(
        (record["Changed_At__c"] for record in data.PAGES_RECORDS),
        reverse=last_value_func is min,
    )
    pages = salesforce_table(
        sobject=data.PAGES,
        chunk_size=chunk_size,
        api=api,
        backend=backend,
        incremental=dlt.sources.incremental(
            "Changed_At__c",
            initial_value=initial_value,
            # in the order of `last_value_func`, newest first with `min`
            row_order="asc",
            last_value_func=last_value_func,
            range_start="open",  # seeded values are unique, the last one is not read again
        ),
    )
    pipeline = dlt.pipeline(
        pipeline_name="salesforce_seeded_chunks", destination="duckdb", dev_mode=True
    )

    for start in range(0, len(expected), chunk_size):
        chunk = expected[start : start + chunk_size]
        assert_load_info(pipeline.run(pages.add_limit(1)))
        assert last_row_count(pipeline, data.PAGES) == len(chunk)
        assert pages.state["incremental"]["Changed_At__c"]["last_value"] == chunk[-1]
    pipeline.run(pages)
    assert last_row_count(pipeline, data.PAGES) == 0

    assert load_rows(pipeline, data.PAGES, "Seed_Id__c", "Changed_At__c") == sorted(
        (record["Seed_Id__c"], record["Changed_At__c"]) for record in data.PAGES_RECORDS
    )
    # checked last so that records skipped or read twice surface first
    assert pages.state["incremental"]["Changed_At__c"]["last_value"].tzinfo is not None

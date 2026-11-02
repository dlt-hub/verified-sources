import os
import sys
from datetime import date, datetime, time, timezone  # noqa: I251
from typing import Any, Dict, List, Optional

import dlt
import pytest
from dlt.common.typing import TAnyDateTime
from dlt.extract.exceptions import ResourceExtractionError
from dlt.pipeline.exceptions import PipelineStepFailed
from pytest_mock import MockerFixture

from sources.servicenow import servicenow_source, servicenow_table
from sources.servicenow.helpers.credentials import BasicCredentials
from sources.servicenow.helpers.table import TReadApi, hidden_fields

from tests.servicenow.utils import (
    INCIDENT,
    INCIDENT_ROW,
    FakeInstance,
    add_task_tables,
    item_rows,
    make_records,
    random_id_records,
    updated_on,
)
from tests.utils import assert_load_info, load_table_counts, load_tables_to_dicts


def test_servicenow_table_ignored_cursor_range(
    instance: FakeInstance, credentials: BasicCredentials
) -> None:
    add_task_tables(instance, make_records(30, updated_on))
    pipeline = dlt.pipeline(
        pipeline_name="servicenow_cursor_range", destination="duckdb", dev_mode=True
    )
    assert_load_info(
        pipeline.run(servicenow_table(credentials=credentials, table="incident"))
    )
    # the instance ignores conditions on a field without `query_range` access
    instance.ignored_fields = {"sys_updated_on"}
    with pytest.raises(PipelineStepFailed, match="outside of the requested range"):
        pipeline.run(servicenow_table(credentials=credentials, table="incident"))


def test_servicenow_table_incremental_runs(
    instance: FakeInstance, credentials: BasicCredentials
) -> None:
    add_task_tables(instance, make_records(30, updated_on))
    instance.now = "2024-02-02 03:00:00"
    pipeline = dlt.pipeline(
        pipeline_name="servicenow_incremental", destination="duckdb", dev_mode=True
    )

    def incident() -> Any:
        return servicenow_table(credentials=credentials, table="incident", page_size=7)

    # records updated at or after the instance time are read on the next run
    assert_load_info(pipeline.run(incident()))
    assert load_table_counts(pipeline, "incident") == {"incident": 27}
    query = instance.queries("incident")[1]
    # the first run also reads records without a cursor value
    assert query.startswith(
        "sys_updated_on<2024-02-02 03:00:00^ORsys_updated_onISEMPTY^ORDERBYsys_id"
    )
    # the first run reads no deleted records
    assert instance.queries("sys_audit_delete") == []

    instance.now = "2024-02-03 00:00:00"
    instance.tables["incident"][0]["caller_id"] = "updated"
    instance.tables["incident"][0]["sys_updated_on"] = "2024-02-02 22:00:00"
    instance.tables["sys_audit_delete"] = []
    assert_load_info(pipeline.run(incident()))
    assert load_table_counts(pipeline, "incident") == {"incident": 30}
    # the next run starts 600 seconds before the max cursor
    assert instance.queries("incident")[-1].startswith(
        "sys_updated_on>=2024-02-02 01:50:00^sys_updated_on<2024-02-03 00:00:00^"
    )
    rows = load_tables_to_dicts(pipeline, "incident")["incident"]
    assert [row["caller_id"] for row in rows if row["sys_id"] == f"{30:032x}"] == [
        "updated"
    ]
    (source_state,) = pipeline.state["sources"].values()
    state = source_state["resources"]["incident"]["incremental"]["sys_updated_on"]
    assert state["last_value"] == datetime(2024, 2, 2, 22, tzinfo=timezone.utc)


def test_servicenow_table_deletes(
    instance: FakeInstance, credentials: BasicCredentials
) -> None:
    add_task_tables(instance, make_records(4, updated_on))
    instance.add_table("u_incident", {"u_extra": "string"}, parent="incident")
    instance.now = "2024-03-01 00:00:00"
    pipeline = dlt.pipeline(
        pipeline_name="servicenow_deletes", destination="duckdb", dev_mode=True
    )

    def task() -> Any:
        # deletes of records of tables that extend `task` are read too
        return servicenow_table(credentials=credentials, table="task")

    instance.tables["task"] = instance.tables["incident"]
    assert_load_info(pipeline.run(task()))
    assert load_table_counts(pipeline, "task") == {"task": 4}

    deleted = instance.tables["task"].pop(0)
    instance.tables["sys_audit_delete"] = [
        {
            "sys_id": "a1",
            "documentkey": deleted["sys_id"],
            "tablename": "u_incident",
            "sys_created_on": "2024-03-01 10:00:00",
        },
        # a record of another table
        {
            "sys_id": "a2",
            "documentkey": instance.tables["task"][0]["sys_id"],
            "tablename": "sys_user",
            "sys_created_on": "2024-03-01 10:00:00",
        },
    ]
    instance.now = "2024-03-02 00:00:00"
    assert_load_info(pipeline.run(task()))
    assert load_table_counts(pipeline, "task") == {"task": 3}
    window = (
        "sys_created_on>=2024-02-01 02:50:00^sys_created_on<2024-03-02 00:00:00"
        "^tablenameINtask,incident,u_incident"
    )
    # the read ends with an empty page
    assert instance.queries("sys_audit_delete") == [
        f"{window}^ORDERBYsys_id",
        f"{window}^sys_id>a1^ORDERBYsys_id",
    ]
    columns = pipeline.default_schema.get_table("task")["columns"]
    assert columns["is_deleted"]["hard_delete"] is True
    assert columns["sys_updated_on"]["dedup_sort"] == "desc"

    # a user without access to `sys_audit_delete`
    del instance.tables["sys_audit_delete"]
    with pytest.raises(Exception, match="include_deleted=False"):
        pipeline.run(task())
    instance.requests.clear()
    pipeline.run(
        servicenow_table(credentials=credentials, table="task", include_deleted=False)
    )
    # the deleted record moved the cursor to the time it was deleted
    assert instance.queries("task")[-1].startswith(
        "sys_updated_on>=2024-03-01 09:50:00^"
    )
    assert instance.queries("sys_audit_delete") == []


def test_servicenow_table_hidden_fields(
    instance: FakeInstance, credentials: BasicCredentials, mocker: MockerFixture
) -> None:
    add_task_tables(instance, make_records(3, updated_on, number="1", priority="2"))
    instance.hidden_fields = {"priority"}
    warning = mocker.patch("sources.servicenow.helpers.table.logger.warning")
    for backend, api in (("python", "table"), ("pyarrow", "table"), ("pyarrow", "csv")):
        warning.reset_mock()
        resource = servicenow_table(
            credentials=credentials,
            table="incident",
            write_disposition="replace",
            included_columns=["number", "priority"],
            backend=backend,
            api=api,
        )
        list(resource)
        messages = [
            call.args[0]
            for call in warning.call_args_list
            if "not returned" in call.args[0]
        ]
        assert len(messages) == 1
        assert "'priority'" in messages[0] and "'number'" not in messages[0]


def test_servicenow_table_hidden_metadata(
    instance: FakeInstance, credentials: BasicCredentials
) -> None:
    add_task_tables(instance, make_records(3, updated_on))
    instance.hidden_fields = {"super_class.name"}
    with pytest.raises(ValueError, match="sys_db_object"):
        servicenow_table(credentials=credentials, table="incident")

    instance.hidden_fields = {"documentkey"}
    instance.tables["sys_audit_delete"] = [
        {
            "sys_id": "a1",
            "documentkey": "1",
            "tablename": "incident",
            "sys_created_on": "2024-02-15 00:00:00",
        }
    ]
    pipeline = dlt.pipeline(
        pipeline_name="servicenow_hidden_metadata", destination="duckdb", dev_mode=True
    )
    # the first run reads no deleted records
    assert_load_info(
        pipeline.run(servicenow_table(credentials=credentials, table="incident"))
    )
    with pytest.raises(
        PipelineStepFailed, match="sys_audit_delete` are returned without"
    ):
        pipeline.run(servicenow_table(credentials=credentials, table="incident"))


def test_servicenow_table_cursor_order(
    instance: FakeInstance, credentials: BasicCredentials
) -> None:
    add_task_tables(instance, make_records(5, updated_on))
    pipeline = dlt.pipeline(
        pipeline_name="servicenow_order", destination="duckdb", dev_mode=True
    )
    # records ordered by the cursor are loaded in chunks
    resource = servicenow_table(
        credentials=credentials,
        table="incident",
        incremental=dlt.sources.incremental("sys_updated_on", row_order="asc"),
        page_size=2,
    ).add_limit(1)
    # every run reads the record with the last cursor value again
    for _ in range(4):
        pipeline.run(resource)
    assert load_table_counts(pipeline, "incident") == {"incident": 5}
    queries = instance.queries("incident")
    assert queries[1].endswith("^ORDERBYsys_updated_on^ORDERBYsys_id")


@pytest.mark.parametrize("backend", ["python", "pyarrow"])
def test_servicenow_table_empty_cursor(
    instance: FakeInstance, credentials: BasicCredentials, backend: str
) -> None:
    # records imported without timestamps, more than a page of them
    records = make_records(9, updated_on)
    for record in records[:5]:
        record["sys_updated_on"] = ""
    add_task_tables(instance, records)
    instance.tables["sys_audit_delete"] = []
    # pages ordered by the cursor continue after the empty values
    resource = servicenow_table(
        credentials=credentials,
        table="incident",
        page_size=2,
        backend=backend,
        incremental=dlt.sources.incremental(
            "sys_updated_on", row_order="asc", on_cursor_value_missing="include"
        ),
    )
    rows = item_rows(list(resource))
    assert sorted(row["sys_id"] for row in rows) == sorted(r["sys_id"] for r in records)
    assert sum(row["sys_updated_on"] is None for row in rows) == 5

    pipeline = dlt.pipeline(
        pipeline_name="servicenow_empty_cursor", destination="duckdb", dev_mode=True
    )
    # the first run reads all 9 records, the next run reads again the 1 record within the lag
    for expected in (9, 1):
        resource = servicenow_table(
            credentials=credentials, table="incident", page_size=2, backend=backend
        )
        assert_load_info(pipeline.run(resource))
        assert (
            pipeline.last_trace.last_normalize_info.row_counts["incident"] == expected
        )
    assert load_table_counts(pipeline, "incident") == {"incident": 9}


@pytest.mark.parametrize("api", ["table", "csv"])
@pytest.mark.parametrize("row_order", [None, "asc"], ids=["default", "row-order"])
def test_servicenow_table_hidden_cursor(
    instance: FakeInstance,
    credentials: BasicCredentials,
    row_order: Optional[str],
    api: TReadApi,
) -> None:
    add_task_tables(instance, make_records(3, updated_on))
    instance.tables["sys_audit_delete"] = []
    # a field ACL hides the cursor. Without an error, the incremental reads all records on
    # every run
    instance.hidden_fields = {"sys_updated_on"}
    resource = servicenow_table(
        credentials=credentials,
        table="incident",
        incremental=(
            dlt.sources.incremental("sys_updated_on", row_order=row_order)
            if row_order
            else None
        ),
        api=api,
    )
    with pytest.raises(ResourceExtractionError, match="field ACL"):
        list(resource)


@pytest.mark.parametrize("from_source", [False, True], ids=["table", "source"])
@pytest.mark.parametrize(
    "backend,api",
    [("python", "table"), ("pyarrow", "table"), ("pyarrow", "csv")],
    ids=["python", "pyarrow", "csv"],
)
def test_servicenow_table_partitioned_load(
    instance: FakeInstance,
    credentials: BasicCredentials,
    backend: str,
    api: TReadApi,
    from_source: bool,
) -> None:
    records = random_id_records(60)
    for i, record in enumerate(records):
        record.update(
            number=str(1000 + i),
            priority=str(i % 5),
            active="true" if i % 2 else "false",
            business_duration=f"1970-01-0{1 + i % 3} 0{i % 9}:00:00",
        )
    add_task_tables(instance, records)
    instance.tables["sys_audit_delete"] = []
    # an ACL hides a record of the first page, which comes back shorter than the page size
    instance.hidden = {min(record["sys_id"] for record in records)}
    options: Dict[str, Any] = dict(
        credentials=credentials, page_size=5, backend=backend, api=api, max_partitions=4
    )
    if from_source:
        # resources of the source are parallelized, ranges are read in parallel too
        items = list(servicenow_source(tables=["incident"], **options))
    else:
        items = list(servicenow_table(table="incident", **options))
    rows = item_rows(items)
    assert sorted(row["sys_id"] for row in rows) == sorted(
        record["sys_id"]
        for record in records
        if record["sys_id"] not in instance.hidden
    )
    # the first page and one count of the records after it plan 4 ranges. Like a read of the
    # whole table, each range ends with an empty page and a count
    counts = instance.count_queries("incident")
    assert len(counts) == 5
    assert (
        len(
            [
                query
                for query in counts
                if "sys_id>=" not in query and "sys_id<" not in query
            ]
        )
        == 1
    )
    queries = [query for query in instance.queries("incident") if "ORDERBY" in query]
    assert (
        len({query.split("^ORDERBY")[0] for query in queries if "sys_id<" in query})
        >= 3
    )
    assert any("sys_id>=" in query and "sys_id<" not in query for query in queries)
    # the default incremental limits every range to the window of the run
    assert all("sys_updated_on<" in query for query in queries)
    # ranges are read by the extract workers, not one after another in the main thread
    assert len(instance.threads - {"MainThread"}) > 1
    # a sequential read returns the same rows
    options["allow_partitioned_load"] = False
    sequential = item_rows(list(servicenow_table(table="incident", **options)))
    assert sorted(rows, key=lambda row: row["sys_id"]) == sorted(
        sequential, key=lambda row: row["sys_id"]
    )


def test_servicenow_table_partition_error(
    instance: FakeInstance, credentials: BasicCredentials, mocker: MockerFixture
) -> None:
    add_task_tables(instance, random_id_records(60))
    # requests of ranges after the first one fail
    mocker.patch.object(
        instance,
        "_denied",
        side_effect=lambda params: "sys_id>=" in params.get("sysparm_query", ""),
    )
    resource = servicenow_table(
        credentials=credentials,
        table="incident",
        write_disposition="replace",
        page_size=5,
    )
    with pytest.raises(ResourceExtractionError, match="Insufficient rights"):
        list(resource)


@pytest.mark.parametrize(
    "case",
    ["disabled", "row_order", "add_limit", "one_page", "uncounted", "not_hex"],
)
def test_servicenow_table_sequential_load(
    instance: FakeInstance, credentials: BasicCredentials, case: str
) -> None:
    records = random_id_records(5 if case == "one_page" else 30)
    if case == "not_hex":
        for i, record in enumerate(records):
            record["sys_id"] = f"imported-{i:04d}"
    add_task_tables(instance, records)
    instance.tables["sys_audit_delete"] = []
    if case == "uncounted":
        instance.uncounted = {"incident"}
    resource = servicenow_table(
        credentials=credentials,
        table="incident",
        page_size=5,
        allow_partitioned_load=case != "disabled",
        incremental=(
            dlt.sources.incremental("sys_updated_on", row_order="asc")
            if case == "row_order"
            else None
        ),
    )
    if case == "add_limit":
        resource.add_limit(3)
    rows = item_rows(list(resource))
    assert len(rows) == (15 if case == "add_limit" else len(records))
    assert not any(
        "sys_id<" in query or "sys_id>=" in query
        for query in instance.queries("incident")
    )


def test_servicenow_table_partitioned_records(
    instance: FakeInstance, credentials: BasicCredentials, mocker: MockerFixture
) -> None:
    # sys_ids that are not random fall into the first range and are read there
    records = make_records(30, updated_on, priority="2")
    add_task_tables(instance, records)
    ordered = sorted(record["sys_id"] for record in records)
    # ACLs hide a page of records and a field
    instance.hidden = set(ordered[10:15])
    instance.hidden_fields = {"priority"}
    warning = mocker.patch("sources.servicenow.helpers.table.logger.warning")
    resource = servicenow_table(
        credentials=credentials,
        table="incident",
        write_disposition="replace",
        page_size=5,
        included_columns=["number", "priority"],
    )
    rows = item_rows(list(resource))
    assert sorted(row["sys_id"] for row in rows) == [
        sys_id for sys_id in ordered if sys_id not in instance.hidden
    ]
    assert any("sys_id<" in query for query in instance.queries("incident"))
    hidden = [
        call.args[0]
        for call in warning.call_args_list
        if "not returned" in call.args[0]
    ]
    assert len(hidden) == 1


@pytest.mark.parametrize(
    "start_date",
    ["2024-02-02", date(2024, 2, 2), datetime(2024, 2, 2, tzinfo=timezone.utc), None],
    ids=["str", "date", "datetime", "config"],
)
@pytest.mark.parametrize(
    "write_disposition,expected_queries,expected_rows",
    [
        (
            None,
            [
                "sys_updated_on>=2024-02-02 00:00:00^sys_updated_on<2024-03-01 00:00:00",
                "sys_updated_on>=2024-02-02 04:50:00^sys_updated_on<2024-03-01 00:00:00",
            ],
            [6, 1],
        ),
        (
            "replace",
            ["sys_updated_on>=2024-02-02 00:00:00^sys_updated_on<2024-03-01 00:00:00"]
            * 2,
            [6, 6],
        ),
    ],
    ids=["merge", "replace"],
)
def test_servicenow_table_start_date(
    instance: FakeInstance,
    credentials: BasicCredentials,
    start_date: Optional[TAnyDateTime],
    write_disposition: Optional[str],
    expected_queries: List[str],
    expected_rows: List[int],
) -> None:
    # records updated every hour from 2024-02-01. replace loads the records since the start date
    # on every run, merge continues from the last run
    add_task_tables(instance, make_records(30, updated_on))
    instance.tables["sys_audit_delete"] = []
    kwargs: Dict[str, Any] = {"start_date": start_date}
    if start_date is None:
        # an explicit `None` overrides the config value
        kwargs = {}
        os.environ["SOURCES__SERVICENOW__INCIDENT__START_DATE"] = "2024-02-02"
    pipeline = dlt.pipeline(
        pipeline_name="servicenow_start_date", destination="duckdb", dev_mode=True
    )
    for expected in expected_rows:
        resource = servicenow_table(
            credentials=credentials,
            table="incident",
            write_disposition=write_disposition,
            **kwargs,
        )
        assert resource.section == "servicenow"
        assert_load_info(pipeline.run(resource))
        assert (
            pipeline.last_trace.last_normalize_info.row_counts["incident"] == expected
        )
    first_pages = [
        query.removesuffix("^ORDERBYsys_id")
        for query in instance.queries("incident")
        if query and "sys_id>" not in query
    ]
    assert first_pages == expected_queries
    assert load_table_counts(pipeline, "incident") == {"incident": 6}


def test_servicenow_table_start_date_errors(
    instance: FakeInstance, credentials: BasicCredentials
) -> None:
    add_task_tables(instance)
    instance.add_table("cmn_schedule", {"sys_id": "GUID", "name": "string"})
    with pytest.raises(ValueError, match="not both"):
        servicenow_table(
            credentials=credentials,
            table="incident",
            start_date="2024-02-02",
            incremental=dlt.sources.incremental("sys_created_on"),
        )
    # the table has no field that the default incremental can start from
    with pytest.raises(ValueError, match="start_date"):
        servicenow_table(
            credentials=credentials, table="cmn_schedule", start_date="2024-02-02"
        )


def test_servicenow_table_without_pyarrow(
    instance: FakeInstance, credentials: BasicCredentials, mocker: MockerFixture
) -> None:
    # "auto" picks the python backend, which must not import pyarrow
    mocker.patch.dict(sys.modules, {"pyarrow": None, "dlt.common.libs.pyarrow": None})
    add_task_tables(instance, [INCIDENT])
    items = list(servicenow_table(credentials=credentials, table="incident"))
    assert items == [{**INCIDENT_ROW, "is_deleted": None}]


@pytest.mark.parametrize(
    "last_value_func,initial_value",
    [
        (max, None),
        (min, None),
        (max, datetime(1999, 1, 1, tzinfo=timezone.utc)),
        (min, datetime(2100, 1, 1, tzinfo=timezone.utc)),
    ],
    ids=["max", "min", "max_initial_value", "min_initial_value"],
)
def test_servicenow_table_chunks(
    instance: FakeInstance,
    credentials: BasicCredentials,
    last_value_func: Any,
    initial_value: Optional[datetime],
) -> None:
    # cursor values in an order unrelated to `sys_id`
    records = random_id_records(23)
    add_task_tables(instance, records)
    instance.tables["sys_audit_delete"] = []
    table = servicenow_table(
        credentials=credentials,
        table="incident",
        page_size=5,
        incremental=dlt.sources.incremental(
            "sys_updated_on",
            initial_value=initial_value,
            last_value_func=last_value_func,
            # in the order of `last_value_func`, newest first with `min`
            row_order="asc",
            range_start="open",
        ),
    )
    pipeline = dlt.pipeline(
        pipeline_name="servicenow_chunks", destination="duckdb", dev_mode=True
    )
    expected = sorted(
        (record["sys_updated_on"] for record in records),
        reverse=last_value_func is min,
    )
    for start in range(0, len(expected), 5):
        chunk = expected[start : start + 5]
        assert_load_info(pipeline.run(table.add_limit(1)))
        assert pipeline.last_trace.last_normalize_info.row_counts["incident"] == len(
            chunk
        )
        last_value = table.state["incremental"]["sys_updated_on"]["last_value"]
        assert last_value == datetime.fromisoformat(chunk[-1]).replace(
            tzinfo=timezone.utc
        )
    pipeline.run(table)
    assert "incident" not in pipeline.last_trace.last_normalize_info.row_counts
    assert load_table_counts(pipeline, "incident") == {"incident": len(records)}
    # with `min` the incremental goes back in time and does not read deleted records
    assert bool(instance.queries("sys_audit_delete")) == (last_value_func is max)


def test_servicenow_table_next_run(
    instance: FakeInstance, credentials: BasicCredentials
) -> None:
    # your own incremental keeps dlt's deduplication of the records at the last value
    add_task_tables(instance, random_id_records(23))
    instance.tables["sys_audit_delete"] = []
    table = servicenow_table(
        credentials=credentials,
        table="incident",
        page_size=5,
        incremental=dlt.sources.incremental("sys_updated_on"),
    )
    pipeline = dlt.pipeline(
        pipeline_name="servicenow_next_run", destination="duckdb", dev_mode=True
    )
    assert_load_info(pipeline.run(table))
    assert pipeline.last_trace.last_normalize_info.row_counts["incident"] == 23
    pipeline.run(table)
    assert "incident" not in pipeline.last_trace.last_normalize_info.row_counts


def test_servicenow_table_applied_incremental(
    instance: FakeInstance, credentials: BasicCredentials
) -> None:
    # an incremental applied with `apply_hints` replaces the default one with its lag,
    # deduplication and deletes
    add_task_tables(instance, make_records(9, updated_on))
    instance.tables["sys_audit_delete"] = []
    pipeline = dlt.pipeline(
        pipeline_name="servicenow_applied_incremental",
        destination="duckdb",
        dev_mode=True,
    )
    resource = servicenow_table(credentials=credentials, table="incident")
    resource.apply_hints(incremental=dlt.sources.incremental("sys_updated_on"))
    assert_load_info(pipeline.run(resource))
    instance.requests.clear()
    pipeline.run(resource)
    # without lag the next run starts at the last value and loads no record again
    last = max(updated_on(i) for i in range(9))
    assert any(
        query.startswith(f"sys_updated_on>={last}")
        for query in instance.queries("incident")
    )
    assert "incident" not in pipeline.last_trace.last_normalize_info.row_counts
    assert instance.queries("sys_audit_delete")

    # a new pipeline starts without the state of the `max` incremental
    dlt.pipeline(pipeline_name="servicenow_applied_min", dev_mode=True)
    instance.requests.clear()
    start = datetime(2024, 2, 1, 5, tzinfo=timezone.utc)
    resource.apply_hints(
        incremental=dlt.sources.incremental(
            "sys_updated_on", initial_value=start, last_value_func=min
        )
    )
    assert len(item_rows(list(resource))) == 6
    assert any(
        "sys_updated_on<=2024-02-01 05:00:00" in query
        for query in instance.queries("incident")
    )
    # a range going back has no deletes
    assert instance.queries("sys_audit_delete") == []


def test_servicenow_table_included_columns(
    instance: FakeInstance, credentials: BasicCredentials
) -> None:
    add_task_tables(instance, make_records(3, updated_on, number="1", priority="2"))
    instance.tables["sys_audit_delete"] = []
    # the cursor and the keys are loaded with the included columns
    resource = servicenow_table(
        credentials=credentials,
        table="incident",
        included_columns=["priority"],
        merge_key="number",
    )
    columns = resource.compute_table_schema()["columns"]
    assert set(columns) == {
        "priority",
        "number",
        "sys_id",
        "sys_updated_on",
        "is_deleted",
    }
    assert resource.write_disposition == "merge"
    assert len(item_rows(list(resource))) == 3
    # excluded columns remove them
    resource = servicenow_table(
        credentials=credentials,
        table="incident",
        included_columns=["priority"],
        excluded_columns=["sys_updated_on"],
    )
    with pytest.raises(ResourceExtractionError, match="not a field"):
        list(resource)


def test_servicenow_table_csv_page_size(
    instance: FakeInstance, credentials: BasicCredentials, mocker: MockerFixture
) -> None:
    # an export returns at most 10000 records, so a larger page is never full
    add_task_tables(instance, make_records(3, updated_on))
    warning = mocker.patch("sources.servicenow.helpers.table.logger.warning")
    resource = servicenow_table(
        credentials=credentials,
        table="incident",
        write_disposition="replace",
        api="csv",
        page_size=20000,
    )
    assert len(item_rows(list(resource))) == 3
    assert any("at most 10000" in call.args[0] for call in warning.call_args_list)
    record_counts = {
        params["sysparm_record_count"]
        for params, _ in instance.page_requests("incident")
        if "sysparm_record_count" in params
    }
    assert record_counts == {"10000"}

import os
import sys
from datetime import date, datetime, time, timezone  # noqa: I251
from typing import Any, Dict, List, Optional

import dlt
import pytest
from dlt.common import Decimal
from dlt.common.typing import TAnyDateTime
from dlt.extract.exceptions import ResourceExtractionError
from dlt.pipeline.exceptions import PipelineStepFailed
from pytest_mock import MockerFixture

from sources.servicenow import servicenow_source, servicenow_table
from sources.servicenow.helpers.credentials import BasicCredentials
from sources.servicenow.helpers.table import TReadApi

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

READERS = pytest.mark.parametrize(
    "backend,api",
    [("python", "table"), ("pyarrow", "table"), ("pyarrow", "csv")],
    ids=["python", "pyarrow", "csv"],
)


def typed_value(cursor: str, i: int) -> str:
    """Raw value of the `i`-th record in an order unrelated to `sys_id`. As text, "10" sorts
    before "9"
    """
    n = (i * 7) % 23 + 1
    letter = chr(ord("a") + n - 1)
    return {
        "sys_updated_on": f"2024-02-{n:02d} 10:00:00",
        "number": str(n),
        "u_due": f"2024-02-{n:02d}",
        "u_cost": f"{n}.5",
        # text in mixed case
        "caller_id": letter.upper() if i % 2 else letter,
    }[cursor]


def loaded_value(cursor: str, raw: str) -> Any:
    if cursor == "sys_updated_on":
        return datetime.fromisoformat(raw).replace(tzinfo=timezone.utc)
    if cursor == "caller_id":
        return raw
    return {"number": int, "u_due": date.fromisoformat, "u_cost": Decimal}[cursor](raw)


def typed_records(cursor: str) -> List[Dict[str, Any]]:
    records = random_id_records(23)
    for i, record in enumerate(records):
        record[cursor] = typed_value(cursor, i)
    return records


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

    # a field ACL hides the record keys of deleted records
    instance.hidden_fields = {"documentkey"}
    with pytest.raises(
        PipelineStepFailed, match="sys_audit_delete` are returned without"
    ):
        pipeline.run(task())
    instance.hidden_fields = set()

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


@READERS
def test_servicenow_table_hidden_fields(
    instance: FakeInstance,
    credentials: BasicCredentials,
    mocker: MockerFixture,
    backend: str,
    api: TReadApi,
) -> None:
    records = make_records(30, updated_on, number="1", priority="2")
    add_task_tables(instance, records)
    instance.hidden_fields = {"super_class.name"}
    with pytest.raises(ValueError, match="sys_db_object"):
        servicenow_table(credentials=credentials, table="incident")

    # ACLs hide a page of records and a field. Sequential `sys_id`s fall into the first range
    ordered = sorted(record["sys_id"] for record in records)
    instance.hidden = set(ordered[10:15])
    instance.hidden_fields = {"priority"}
    warning = mocker.patch("sources.servicenow.helpers.table.logger.warning")
    resource = servicenow_table(
        credentials=credentials,
        table="incident",
        write_disposition="replace",
        page_size=5,
        included_columns=["number", "priority"],
        backend=backend,
        api=api,
    )
    rows = item_rows(list(resource))
    assert sorted(row["sys_id"] for row in rows) == [
        sys_id for sys_id in ordered if sys_id not in instance.hidden
    ]
    assert any("sys_id<" in query for query in instance.queries("incident"))
    messages = [
        call.args[0]
        for call in warning.call_args_list
        if "not returned" in call.args[0]
    ]
    assert len(messages) == 1
    assert "'priority'" in messages[0] and "'number'" not in messages[0]


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
    incremental: Dict[str, Any] = (
        {"incremental": dlt.sources.incremental("sys_updated_on", row_order=row_order)}
        if row_order
        else {}
    )
    resource = servicenow_table(
        credentials=credentials, table="incident", api=api, **incremental
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


def test_servicenow_table_incremental_argument(
    instance: FakeInstance, credentials: BasicCredentials
) -> None:
    add_task_tables(instance, make_records(30, updated_on))
    instance.tables["sys_audit_delete"] = []
    # without incremental each run reads all records and replaces the table
    pipeline = dlt.pipeline(
        pipeline_name="servicenow_no_incremental", destination="duckdb", dev_mode=True
    )
    for _ in range(2):
        resource = servicenow_table(
            credentials=credentials, table="incident", incremental=None
        )
        assert_load_info(pipeline.run(resource))
    assert resource.write_disposition == "replace"
    assert resource.incremental.incremental is None
    assert load_table_counts(pipeline, "incident") == {
        "incident": pipeline.last_trace.last_normalize_info.row_counts["incident"]
    }

    # an incremental without cursor sets values of the default one, which keeps its settings
    resource = servicenow_table(
        credentials=credentials,
        table="incident",
        incremental=dlt.sources.incremental(lag=60),
    )
    pipeline.extract(resource)
    incremental = resource.incremental.incremental
    assert (incremental.cursor_path, incremental.lag) == ("sys_updated_on", 60)
    assert incremental.on_cursor_value_missing == "include"
    assert incremental.primary_key == ()


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


def test_servicenow_table_without_pyarrow(
    instance: FakeInstance, credentials: BasicCredentials, mocker: MockerFixture
) -> None:
    # "auto" picks the python backend, which must not import pyarrow
    mocker.patch.dict(sys.modules, {"pyarrow": None, "dlt.common.libs.pyarrow": None})
    add_task_tables(instance, [INCIDENT])
    items = list(servicenow_table(credentials=credentials, table="incident"))
    assert items == [{**INCIDENT_ROW, "is_deleted": None}]


@pytest.mark.parametrize("last_value_func", [max, min])
@pytest.mark.parametrize("cursor", ["sys_updated_on", "number", "u_due", "u_cost"])
@pytest.mark.parametrize("backend", ["python", "pyarrow"])
def test_servicenow_table_chunks(
    instance: FakeInstance,
    credentials: BasicCredentials,
    cursor: str,
    last_value_func: Any,
    backend: str,
) -> None:
    # keyset pages ordered by timestamps, numbers and dates, cursor values in an order unrelated
    # to `sys_id`
    records = typed_records(cursor)
    add_task_tables(instance, records)
    instance.tables["sys_audit_delete"] = []
    table = servicenow_table(
        credentials=credentials,
        table="incident",
        page_size=5,
        backend=backend,
        incremental=dlt.sources.incremental(
            cursor,
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
        (loaded_value(cursor, record[cursor]) for record in records),
        reverse=last_value_func is min,
    )
    for start in range(0, len(expected), 5):
        chunk = expected[start : start + 5]
        assert_load_info(pipeline.run(table.add_limit(1)))
        assert pipeline.last_trace.last_normalize_info.row_counts["incident"] == len(
            chunk
        )
        assert table.state["incremental"][cursor]["last_value"] == chunk[-1]
    pipeline.run(table)
    assert "incident" not in pipeline.last_trace.last_normalize_info.row_counts
    assert load_table_counts(pipeline, "incident") == {"incident": len(records)}
    # the request of the instance time has no query
    direction = "DESC" if last_value_func is min else ""
    assert all(
        query.endswith(f"ORDERBY{direction}{cursor}^ORDERBY{direction}sys_id")
        for query in instance.queries("incident")
        if query
    )
    # only a `max` incremental on `sys_updated_on` reads deleted records
    assert bool(instance.queries("sys_audit_delete")) == (
        cursor == "sys_updated_on" and last_value_func is max
    )


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


@READERS
@pytest.mark.parametrize("cursor", ["number", "u_due", "u_cost", "caller_id"])
def test_servicenow_table_typed_cursor(
    instance: FakeInstance,
    credentials: BasicCredentials,
    cursor: str,
    backend: str,
    api: TReadApi,
) -> None:
    # the instance compares text case-insensitively, python case-sensitively
    instance.collation = str.lower
    records = typed_records(cursor)
    add_task_tables(instance, records)
    pipeline = dlt.pipeline(
        pipeline_name="servicenow_typed_cursor", destination="duckdb", dev_mode=True
    )
    table = servicenow_table(
        credentials=credentials,
        table="incident",
        backend=backend,
        api=api,
        incremental=dlt.sources.incremental(cursor),
    )
    assert_load_info(pipeline.run(table))
    assert pipeline.last_trace.last_normalize_info.row_counts["incident"] == 23
    # the next run starts at the last value with a literal of the type of the field, only
    # timestamps have an upper bound. Text in other case comes back without a false range error
    # and is filtered
    instance.requests.clear()
    pipeline.run(table)
    assert "incident" not in pipeline.last_trace.last_normalize_info.row_counts
    last = str(max(loaded_value(cursor, record[cursor]) for record in records))
    assert instance.queries("incident")[0] == f"{cursor}>={last}^ORDERBYsys_id"


def test_servicenow_table_cursor_bounds(
    instance: FakeInstance, credentials: BasicCredentials
) -> None:
    add_task_tables(instance, make_records(9, updated_on))
    instance.tables["sys_audit_delete"] = []
    # the instance stores whole seconds, the start is rounded up
    table = servicenow_table(
        credentials=credentials,
        table="incident",
        start_date=datetime(2024, 2, 1, 5, 0, 0, 500000, tzinfo=timezone.utc),
    )
    assert len(item_rows(list(table))) == 3
    assert any(
        query.startswith("sys_updated_on>=2024-02-01 05:00:01^sys_updated_on<")
        for query in instance.queries("incident")
    )
    # a naive value is in the context timezone, UTC by default
    table = servicenow_table(
        credentials=credentials,
        table="incident",
        incremental=dlt.sources.incremental(
            "sys_updated_on", initial_value=datetime(2024, 2, 1, 5)
        ),
    )
    assert len(item_rows(list(table))) == 4
    # a value that cannot be compared with the loaded values
    table = servicenow_table(
        credentials=credentials,
        table="incident",
        incremental=dlt.sources.incremental(
            "sys_updated_on", initial_value="2024-02-01 05:00:00"
        ),
    )
    with pytest.raises(ResourceExtractionError, match="holds timestamp values"):
        list(table)


def test_servicenow_table_unsupported_cursors(
    instance: FakeInstance, credentials: BasicCredentials, mocker: MockerFixture
) -> None:
    records = make_records(9, updated_on)
    for i, record in enumerate(records):
        record["u_start"] = f"1970-01-01 {i:02d}:00:00"
    add_task_tables(instance, records)
    warning = mocker.patch("sources.servicenow.helpers.table.logger.warning")
    table = servicenow_table(
        credentials=credentials,
        table="incident",
        incremental=dlt.sources.incremental(
            "u_start", initial_value=time(5), range_start="open"
        ),
    )
    # all records are read, the incremental filters them
    assert len(item_rows(list(table))) == 3
    assert instance.queries("incident")[0] == "ORDERBYsys_id"
    assert any("cannot compare" in call.args[0] for call in warning.call_args_list)
    table = servicenow_table(
        credentials=credentials,
        table="incident",
        incremental=dlt.sources.incremental("u_start", row_order="asc"),
    )
    with pytest.raises(ResourceExtractionError, match="cannot compare"):
        list(table)
    # the instance orders text case-insensitively, python case-sensitively
    table = servicenow_table(
        credentials=credentials,
        table="incident",
        incremental=dlt.sources.incremental("caller_id", row_order="asc"),
    )
    with pytest.raises(ResourceExtractionError, match="case-insensitively"):
        list(table)


def test_servicenow_table_deferred(
    instance: FakeInstance, credentials: BasicCredentials
) -> None:
    add_task_tables(instance, typed_records("number"))
    instance.tables["sys_audit_delete"] = []
    table = servicenow_table(
        credentials=credentials, table="incident", defer_table_reflect=True
    )
    # creating the resource makes no requests
    assert instance.requests == []
    assert table.write_disposition == "merge"
    table.apply_hints(columns={"u_score": {"data_type": "text"}})
    pipeline = dlt.pipeline(
        pipeline_name="servicenow_deferred", destination="duckdb", dev_mode=True
    )
    assert_load_info(pipeline.run(table))
    assert pipeline.last_trace.last_normalize_info.row_counts["incident"] == 23
    columns = pipeline.default_schema.get_table_columns("incident")
    assert columns["number"]["data_type"] == "bigint"
    assert columns["u_due"]["data_type"] == "date"
    assert columns["u_score"]["data_type"] == "text"
    assert columns["is_deleted"]["hard_delete"] is True
    assert columns["sys_id"]["primary_key"] is True

    # `add_limit` does not count the item that carries the hints, your own incremental keeps
    # the deduplication
    table = servicenow_table(
        credentials=credentials,
        table="incident",
        page_size=5,
        defer_table_reflect=True,
        incremental=dlt.sources.incremental("number", row_order="asc"),
    )
    assert_load_info(pipeline.run(table.add_limit(1)))
    assert pipeline.last_trace.last_normalize_info.row_counts["incident"] == 5

    # the default incremental expects `sys_updated_on`
    instance.add_table(
        "u_log",
        {"sys_id": "GUID", "sys_created_on": "glide_date_time"},
        records=[{"sys_id": "a" * 32, "sys_created_on": "2024-01-01 00:00:00"}],
    )
    table = servicenow_table(
        credentials=credentials, table="u_log", defer_table_reflect=True
    )
    with pytest.raises(ResourceExtractionError, match="defer_table_reflect"):
        list(table)


def test_servicenow_table_cursor_not_moved(
    instance: FakeInstance, credentials: BasicCredentials, mocker: MockerFixture
) -> None:
    # more records share a timestamp than a chunk holds
    add_task_tables(instance, make_records(12, lambda i: "2024-02-01 10:00:00"))
    instance.tables["sys_audit_delete"] = []
    warning = mocker.patch("sources.servicenow.helpers.table.logger.warning")
    table = servicenow_table(
        credentials=credentials,
        table="incident",
        page_size=5,
        incremental=dlt.sources.incremental("sys_updated_on", row_order="asc"),
    )
    pipeline = dlt.pipeline(
        pipeline_name="servicenow_not_moved", destination="duckdb", dev_mode=True
    )
    pipeline.run(table.add_limit(1))
    assert not any("did not move" in call.args[0] for call in warning.call_args_list)
    pipeline.run(table.add_limit(1))
    messages = [call.args[0] for call in warning.call_args_list]
    assert any("did not move past" in message for message in messages)
    assert any("reads no deleted records" in message for message in messages)

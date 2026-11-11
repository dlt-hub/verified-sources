import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone  # noqa: I251
from functools import partial
from time import sleep
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import parse_qs, urlparse

import dlt
import pytest
from dlt.extract.resource import DltResource

from sources.servicenow import DEFAULT_TABLES, servicenow_source, servicenow_table
from sources.servicenow.helpers.client import (
    TABLE_API_PARAMS,
    ServiceNowClient,
    make_session,
)

from tests.servicenow.seed import data
from tests.servicenow.utils import (
    delete_record,
    get_credentials,
    wait_for_instance,
    write_record,
)
from tests.utils import (
    ALL_DESTINATIONS,
    assert_load_info,
    load_table_counts,
    load_tables_to_dicts,
    skip_if_not_active,
)

# tests below read the instance seeded with tests/servicenow/seed/seed.py, see README.md


@pytest.fixture(scope="module", autouse=True)
def awake_instance() -> None:
    # developer instances hibernate when idle and answer with an HTML page while they wake up
    wait_for_instance(ServiceNowClient(get_credentials()))


def readable_sys_ids(table: str) -> Set[str]:
    """Returns the `sys_id` of the records of `table` the user can read. The count of the instance
    includes records hidden by ACLs, pages read by offset leave them out.
    """
    client = ServiceNowClient(get_credentials())
    count = client.count(table, "")
    assert count is not None
    page_size = 10000

    def read_page(offset: int) -> List[str]:
        response = client.get(
            f"v2/table/{table}",
            {
                **TABLE_API_PARAMS,
                "sysparm_query": "ORDERBYsys_id",
                "sysparm_fields": "sys_id",
                "sysparm_limit": page_size,
                "sysparm_offset": offset,
            },
        )
        return [record["sys_id"] for record in response.json()["result"]]

    with ThreadPoolExecutor(8) as pool:
        pages = list(pool.map(read_page, range(0, count, page_size)))
    return {sys_id for page in pages for sys_id in page}


def loaded_sys_ids(pipeline: dlt.Pipeline, table: str) -> List[str]:
    with pipeline.sql_client() as client:
        return [row[0] for row in client.execute_sql(f"SELECT sys_id FROM {table}")]


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_all_resources(destination_name: str) -> None:
    pipeline = dlt.pipeline(
        pipeline_name="servicenow",
        destination=destination_name,
        dataset_name="servicenow_data",
        dev_mode=True,
    )
    assert_load_info(pipeline.run(servicenow_source()))
    counts = load_table_counts(pipeline, *DEFAULT_TABLES)
    # demo data of a developer instance
    assert (
        counts["incident"] > 0 and counts["sys_user"] > 0 and counts["sys_choice"] > 0
    )
    columns = pipeline.default_schema.get_table_columns("incident")
    assert columns["sys_id"]["primary_key"] is True
    assert columns["sys_updated_on"]["data_type"] == "timestamp"
    assert columns["opened_at"]["data_type"] == "timestamp"
    assert columns["priority"]["data_type"] == "bigint"
    assert columns["active"]["data_type"] == "bool"
    assert columns["business_duration"]["data_type"] == "bigint"
    assert columns["is_deleted"]["hard_delete"] is True
    assert "work_notes" not in columns

    # the next run reads the records changed since 10 minutes before the last cursor value,
    # and the deleted records
    assert_load_info(
        pipeline.run(servicenow_source().with_resources("incident", "sys_user"))
    )
    assert load_table_counts(pipeline, "incident")["incident"] == counts["incident"]


@skip_if_not_active("duckdb")
@pytest.mark.parametrize(
    "backend,api", [("python", "table"), ("pyarrow", "table"), ("pyarrow", "csv")]
)
def test_dlt_types(backend: str, api: str) -> None:
    pipeline = dlt.pipeline(
        pipeline_name=f"servicenow_types_{backend}_{api}",
        destination="duckdb",
        dev_mode=True,
    )
    table = servicenow_table(
        table=data.DLT_TYPES,
        encoded_query=f"{data.SEED_ID_FIELD}STARTSWITHtypes-",
        backend=backend,
        api=api,
    )
    assert_load_info(pipeline.run(table))
    rows = {
        row[data.SEED_ID_FIELD]: row
        for row in load_tables_to_dicts(pipeline, data.DLT_TYPES)[data.DLT_TYPES]
    }
    assert set(rows) == {record[data.SEED_ID_FIELD] for record in data.RECORDS}
    for record in data.RECORDS:
        loaded: Dict[str, Any] = rows[record[data.SEED_ID_FIELD]]
        for field in data.FIELDS:
            if field == "u_journal":
                assert field not in loaded
            elif field == "u_reference":
                assert loaded.get(field) is None
            else:
                expected = data.expected_value(record, field)
                if api == "csv" and isinstance(expected, str):
                    # the instance exports CSV in Windows-1252, its default charset
                    expected = expected.encode("cp1252", "replace").decode("cp1252")
                assert loaded.get(field) == expected, (
                    record[data.SEED_ID_FIELD],
                    field,
                )


@skip_if_not_active("duckdb")
@pytest.mark.parametrize("api", ["table", "csv"])
def test_pages(api: str) -> None:
    session = make_session()
    page_requests: List[Dict[str, List[str]]] = []

    def record_page_request(response: Any, *args: Any, **kwargs: Any) -> None:
        url = urlparse(response.url)
        params = parse_qs(url.query)
        if data.DLT_PAGES in url.path and "sysparm_limit=1&" not in url.query + "&":
            page_requests.append(params)

    session.hooks["response"].append(record_page_request)
    pipeline = dlt.pipeline(
        pipeline_name=f"servicenow_pages_{api}",
        destination="duckdb",
        dev_mode=True,
    )
    source = servicenow_source(
        tables=[data.DLT_PAGES],
        session=session,
        page_size=data.PAGE_SIZE,
        api=api,
        allow_partitioned_load=False,
    )
    assert_load_info(pipeline.run(source))
    rows = load_tables_to_dicts(pipeline, data.DLT_PAGES)[data.DLT_PAGES]
    assert sorted((row[data.SEED_ID_FIELD], row["u_number"]) for row in rows) == [
        (record[data.SEED_ID_FIELD], record["u_number"])
        for record in data.page_records()
    ]
    # two full pages, a short one and the empty page that ends the read
    assert len(page_requests) == data.PAGE_RECORDS // data.PAGE_SIZE + 2
    assert all(
        params.get("sysparm_limit", params.get("sysparm_record_count"))
        == [str(data.PAGE_SIZE)]
        for params in page_requests
    )

    # the next run merges the records changed since 10 minutes before the last cursor value
    page_requests.clear()
    assert_load_info(pipeline.run(source))
    assert (
        load_table_counts(pipeline, data.DLT_PAGES)[data.DLT_PAGES] == data.PAGE_RECORDS
    )


def record_range_query(
    queries: List[str], response: Any, *args: Any, **kwargs: Any
) -> None:
    query = parse_qs(urlparse(response.url).query).get("sysparm_query", [""])[0]
    if "sys_id<" in query or "sys_id>=" in query:
        queries.append(query)


@skip_if_not_active("duckdb")
@pytest.mark.parametrize("api", ["table", "csv"])
def test_partitioned_load_identical(api: str) -> None:
    # `sys_id` ranges read in parallel load the same dataset as pages read one after another
    datasets: Dict[bool, List[Dict[str, Any]]] = {}
    for partitioned in (False, True):
        session = make_session()
        range_queries: List[str] = []
        session.hooks["response"].append(partial(record_range_query, range_queries))
        pipeline = dlt.pipeline(
            pipeline_name=f"servicenow_identical_{api}_{partitioned}",
            destination="duckdb",
            dev_mode=True,
        )
        source = servicenow_source(
            tables=["sys_choice"],
            session=session,
            page_size=2000,
            api=api,
            allow_partitioned_load=partitioned,
        )
        assert_load_info(pipeline.run(source))
        assert bool(range_queries) == partitioned
        rows = load_tables_to_dicts(pipeline, "sys_choice")["sys_choice"]
        datasets[partitioned] = sorted(
            (
                {
                    name: value
                    for name, value in row.items()
                    if not name.startswith("_dlt")
                }
                for row in rows
            ),
            key=lambda row: row["sys_id"],
        )
    assert len(datasets[True]) > 10000
    assert len({row["sys_id"] for row in datasets[True]}) == len(datasets[True])
    assert datasets[True] == datasets[False]


@skip_if_not_active("duckdb")
@pytest.mark.parametrize("backend", ["python", "pyarrow"])
def test_pages_empty_cursor(backend: str) -> None:
    # empty values come first in the order of the cursor, more than a page of them
    pipeline = dlt.pipeline(
        pipeline_name=f"servicenow_pages_empty_{backend}",
        destination="duckdb",
        dev_mode=True,
    )
    table = servicenow_table(
        table=data.DLT_PAGES,
        page_size=data.PAGE_SIZE,
        backend=backend,
        incremental=dlt.sources.incremental(
            "u_when", row_order="asc", on_cursor_value_missing="include"
        ),
    )
    assert_load_info(pipeline.run(table))
    rows = load_tables_to_dicts(pipeline, data.DLT_PAGES)[data.DLT_PAGES]
    assert sorted(row["u_number"] for row in rows) == list(range(data.PAGE_RECORDS))
    assert sum(row["u_when"] is None for row in rows) == data.EMPTY_WHEN_RECORDS


@skip_if_not_active("duckdb")
def test_large_table() -> None:
    # about 160000 records of a developer instance in pages of 10000, the instance takes
    # minutes to send all fields of them
    pipeline = dlt.pipeline(
        pipeline_name="servicenow_large", destination="duckdb", dev_mode=True
    )
    table = servicenow_table(
        table="sys_dictionary", included_columns=["name", "element", "internal_type"]
    )
    assert_load_info(pipeline.run(table))
    loaded = loaded_sys_ids(pipeline, "sys_dictionary")
    readable = readable_sys_ids("sys_dictionary")
    assert len(readable) > 100000
    assert len(loaded) == len(set(loaded))
    assert set(loaded) == readable


READERS = pytest.mark.parametrize(
    "backend,api",
    [("python", "table"), ("pyarrow", "table"), ("pyarrow", "csv")],
    ids=["python", "pyarrow", "csv"],
)


def seeded(
    backend: str,
    api: str,
    partitioned: bool = False,
    cursor: str = data.INCREMENTAL_CURSOR,
    **incremental_args: Any,
) -> DltResource:
    """The seeded incremental table with an incremental on `cursor`"""
    return servicenow_table(
        table=data.DLT_INCREMENTAL,
        page_size=data.CHUNK_SIZE,
        backend=backend,
        api=api,
        allow_partitioned_load=partitioned,
        incremental=dlt.sources.incremental(cursor, **incremental_args),
    )


def last_row_count(pipeline: dlt.Pipeline) -> int:
    row_counts = pipeline.last_trace.last_normalize_info.row_counts
    return row_counts.get(data.DLT_INCREMENTAL, 0)


def loaded_seed_ids(pipeline: dlt.Pipeline) -> List[str]:
    with pipeline.sql_client() as client:
        rows = client.execute_sql(
            f"SELECT {data.SEED_ID_FIELD} FROM {data.DLT_INCREMENTAL}"
        )
    return sorted(row[0] for row in rows)


@skip_if_not_active("duckdb")
@pytest.mark.parametrize("partitioned", [False, True], ids=["pages", "partitions"])
@READERS
def test_seeded_full_and_incremental(backend: str, api: str, partitioned: bool) -> None:
    records = data.incremental_records()
    values = [record[data.INCREMENTAL_CURSOR] for record in records]
    seed_ids = sorted(record[data.SEED_ID_FIELD] for record in records)
    pipeline = dlt.pipeline(
        pipeline_name=f"servicenow_seeded_{backend}_{api}_{partitioned}",
        destination="duckdb",
        dev_mode=True,
    )
    assert_load_info(pipeline.run(seeded(backend, api, partitioned)))
    # the run stops at the time it starts, so the record from 2032 is not read
    assert last_row_count(pipeline) == len(records)
    assert loaded_seed_ids(pipeline) == seed_ids

    # the record with the last value is read again and deduplicated
    pipeline.run(seeded(backend, api, partitioned))
    assert last_row_count(pipeline) == 0
    assert load_table_counts(pipeline, data.DLT_INCREMENTAL) == {
        data.DLT_INCREMENTAL: len(records)
    }

    # bounds are seeded values, so a value on a bound loads in exactly one window
    values = sorted(values)
    quarters = [values[len(values) * k // 4] for k in (1, 2, 3)]
    # with `min` the windows go back in time, the last one ends before the first seeded value
    for last_value_func, bounds in (
        (max, [values[0], *quarters, values[-1] + timedelta(seconds=1)]),
        (min, [values[-1], *quarters[::-1], values[0] - timedelta(seconds=1)]),
    ):
        backfill = dlt.pipeline(
            pipeline_name=f"servicenow_seeded_backfill_{last_value_func.__name__}",
            destination="duckdb",
            dev_mode=True,
        )
        for start, end in zip(bounds, bounds[1:]):
            expected = sum(
                start <= value < end if last_value_func is max else end < value <= start
                for value in values
            )
            assert expected > 0
            assert_load_info(
                backfill.run(
                    seeded(
                        backend,
                        api,
                        partitioned,
                        initial_value=start,
                        end_value=end,
                        last_value_func=last_value_func,
                        range_start="closed",
                        range_end="open",
                    )
                )
            )
            assert last_row_count(backfill) == expected
        # windows with an end value do not store incremental state
        assert "sources" not in backfill.state
        assert loaded_seed_ids(backfill) == seed_ids

    # a window that ends after the future cursor value reads that record
    future = dlt.pipeline(
        pipeline_name=f"servicenow_seeded_future_{backend}_{api}_{partitioned}",
        destination="duckdb",
        dev_mode=True,
    )
    window = seeded(
        backend,
        api,
        partitioned,
        initial_value=values[-1] + timedelta(seconds=1),
        end_value=datetime(2033, 1, 1, tzinfo=timezone.utc),
    )
    assert_load_info(future.run(window))
    assert loaded_seed_ids(future) == [data.FUTURE_RECORD[data.SEED_ID_FIELD]]


@skip_if_not_active("duckdb")
@READERS
@pytest.mark.parametrize(
    "cursor,last_value_func,initial_value,loads_future",
    [
        # timestamp ranges without an upper end stop at the time the run starts
        (data.INCREMENTAL_CURSOR, max, None, False),
        (data.INCREMENTAL_CURSOR, min, None, False),
        (
            data.INCREMENTAL_CURSOR,
            max,
            datetime(1999, 1, 1, tzinfo=timezone.utc),
            False,
        ),
        (data.INCREMENTAL_CURSOR, min, datetime(2100, 1, 1, tzinfo=timezone.utc), True),
        # the instance compares and orders integers as numbers, 9 before 10
        ("u_number", max, None, True),
        ("u_number", min, None, True),
    ],
    ids=[
        "max",
        "min",
        "max_initial_value",
        "min_initial_value",
        "integer_max",
        "integer_min",
    ],
)
def test_seeded_chunks(
    backend: str,
    api: str,
    cursor: str,
    last_value_func: Any,
    initial_value: Optional[datetime],
    loads_future: bool,
) -> None:
    records = data.incremental_records() + (
        [data.FUTURE_RECORD] if loads_future else []
    )
    expected = sorted(
        (record[cursor] for record in records), reverse=last_value_func is min
    )
    # `sys_id` ranges are not read with `add_limit`, pages come in the order of the cursor
    table = seeded(
        backend,
        api,
        cursor=cursor,
        initial_value=initial_value,
        last_value_func=last_value_func,
        # in the order of `last_value_func`, newest first with `min`
        row_order="asc",
        range_start="open",  # seeded values are unique, the last one is not read again
    )
    pipeline = dlt.pipeline(
        pipeline_name=f"servicenow_seeded_chunks_{backend}_{api}",
        destination="duckdb",
        dev_mode=True,
    )
    for start in range(0, len(expected), data.CHUNK_SIZE):
        chunk = expected[start : start + data.CHUNK_SIZE]
        assert_load_info(pipeline.run(table.add_limit(1)))
        assert last_row_count(pipeline) == len(chunk)
        assert table.state["incremental"][cursor]["last_value"] == chunk[-1]
    pipeline.run(table)
    assert last_row_count(pipeline) == 0

    with pipeline.sql_client() as client:
        rows = client.execute_sql(
            f"SELECT {data.SEED_ID_FIELD}, {cursor} FROM {data.DLT_INCREMENTAL}"
        )
    assert sorted(rows) == sorted(
        (record[data.SEED_ID_FIELD], record[cursor]) for record in records
    )
    if cursor == data.INCREMENTAL_CURSOR:
        assert table.state["incremental"][cursor]["last_value"].tzinfo is not None


@skip_if_not_active("duckdb")
def test_seeded_deferred() -> None:
    # the fields are read when extracting, the default incremental merges on `sys_updated_on`
    table = servicenow_table(table=data.DLT_INCREMENTAL, defer_table_reflect=True)
    pipeline = dlt.pipeline(
        pipeline_name="servicenow_seeded_deferred", destination="duckdb", dev_mode=True
    )
    assert_load_info(pipeline.run(table))
    # the future `u_updated_at` does not matter to the default cursor
    assert last_row_count(pipeline) == data.INCREMENTAL_RECORDS + 1
    columns = pipeline.default_schema.get_table_columns(data.DLT_INCREMENTAL)
    assert columns["u_number"]["data_type"] == "bigint"
    assert columns[data.INCREMENTAL_CURSOR]["data_type"] == "timestamp"
    assert columns["is_deleted"]["hard_delete"] is True


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
@READERS
def test_incremental_changes(destination_name: str, backend: str, api: str) -> None:
    # adds, updates and deletes records of the run between runs of the default incremental
    run_id = uuid.uuid4().hex
    client = ServiceNowClient(get_credentials())
    sys_ids: Dict[str, str] = {}

    def create(name: str) -> None:
        values = {data.SEED_ID_FIELD: name, "u_run_id": run_id, "u_value": 1}
        sys_ids[name] = write_record(client, data.DLT_CHANGES, values)

    def run() -> int:
        table = servicenow_table(
            table=data.DLT_CHANGES,
            encoded_query=f"u_run_id={run_id}",
            lag=0,
            backend=backend,
            api=api,
        )
        # runs read until the time of the instance when they start, which is excluded
        sleep(1.5)
        assert_load_info(pipeline.run(table))
        row_counts = pipeline.last_trace.last_normalize_info.row_counts
        return row_counts.get(data.DLT_CHANGES, 0)

    def loaded() -> List[Tuple[str, int]]:
        rows = load_tables_to_dicts(pipeline, data.DLT_CHANGES)[data.DLT_CHANGES]
        return sorted((row[data.SEED_ID_FIELD], int(row["u_value"])) for row in rows)

    pipeline = dlt.pipeline(
        pipeline_name=f"servicenow_changes_{backend}_{api}",
        destination=destination_name,
        dev_mode=True,
    )
    try:
        for name in ("a1", "a2", "a3", "a4"):
            create(name)
        # the last change of a run is alone in its second, the next run reads only it again
        sleep(1.5)
        create("a5")
        assert run() == 5

        write_record(client, data.DLT_CHANGES, {"u_value": 2}, sys_ids["a1"])
        delete_record(client, data.DLT_CHANGES, sys_ids.pop("a2"))
        create("b1")
        sleep(1.5)
        create("b2")
        # a5 again, a1, the delete of a2, b1 and b2
        assert run() == 5
        expected = [("a1", 2), ("a3", 1), ("a4", 1), ("a5", 1), ("b1", 1), ("b2", 1)]
        assert loaded() == expected

        # b2 again
        assert run() == 1
        assert loaded() == expected
    finally:
        for sys_id in sys_ids.values():
            delete_record(client, data.DLT_CHANGES, sys_id)

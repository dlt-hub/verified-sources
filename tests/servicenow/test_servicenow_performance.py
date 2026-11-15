import os
from datetime import timedelta  # noqa: I251
from functools import partial
from typing import Any, Iterator

import dlt
import pytest
from dlt.extract.resource import DltResource

from sources.servicenow import servicenow_source, servicenow_table
from sources.servicenow.helpers.client import ServiceNowClient
from sources.servicenow.helpers.query import TableQuery
from sources.servicenow.helpers.table import TReadApi, TTableBackend

from tests.perf_utils import assert_loaded_rows, in_window, seeded_windows
from tests.seed_utils import HttpLog, account_lock, print_extract_speed
from tests.servicenow.seed import data
from tests.servicenow.utils import get_credentials, wait_for_instance
from tests.utils import assert_load_info, last_row_count, skip_if_not_active

pytestmark = [pytest.mark.performance, skip_if_not_active("duckdb")]

TABLE = data.PERF_TABLES[0]


@pytest.fixture(scope="module")
def perf_account() -> Iterator[None]:
    """Holds the instance for the performance tests, skips when it is not seeded"""
    with account_lock("servicenow"):
        client = ServiceNowClient(get_credentials())
        wait_for_instance(client)
        for table in data.PERF_TABLES:
            query = TableQuery([f"name={table}"])
            exists = next(client.records("sys_db_object", query, ["sys_id"]), None)
            count = client.count(table, "") if exists else None
            if count != data.PERF_RECORDS:
                pytest.skip(
                    f"{table} has {count} records, run the seed with --performance"
                )
        yield


def assert_perf_rows(pipeline: dlt.Pipeline, table: str) -> None:
    assert_loaded_rows(
        pipeline,
        table,
        "u_number",
        partial(data.perf_record, table),
        range(data.PERF_RECORDS),
    )


def perf_table(
    backend: TTableBackend,
    api: TReadApi,
    partitioned: bool,
    page_size: int = 10000,
    **incremental_args: Any,
) -> DltResource:
    """The first performance table with an incremental on its seeded cursor"""
    return servicenow_table(
        table=TABLE,
        backend=backend,
        api=api,
        page_size=page_size,
        allow_partitioned_load=partitioned,
        incremental=dlt.sources.incremental(data.PERF_CURSOR, **incremental_args),
    )


def test_performance_baseline(perf_http: HttpLog) -> None:
    pipeline = dlt.pipeline(
        pipeline_name="servicenow_perf_baseline", destination="duckdb", dev_mode=True
    )
    assert_load_info(pipeline.run(servicenow_table(table=TABLE)))
    print_extract_speed(pipeline, f"servicenow {TABLE}, defaults", perf_http)
    assert_perf_rows(pipeline, TABLE)


@pytest.mark.parametrize(
    "backend,api",
    [("python", "table"), ("pyarrow", "table"), ("pyarrow", "csv")],
    ids=["python", "pyarrow", "csv"],
)
def test_performance_parallel_tables(
    perf_http: HttpLog, backend: TTableBackend, api: TReadApi
) -> None:
    pipeline = dlt.pipeline(
        pipeline_name=f"servicenow_perf_tables_{backend}_{api}",
        destination="duckdb",
        dev_mode=True,
    )
    source = servicenow_source(tables=data.PERF_TABLES, backend=backend, api=api)
    assert_load_info(pipeline.run(source))
    print_extract_speed(
        pipeline,
        f"servicenow {backend} {api}, {len(data.PERF_TABLES)} tables",
        perf_http,
    )
    for table in data.PERF_TABLES:
        assert_perf_rows(pipeline, table)


@pytest.mark.parametrize(
    "partitioned,workers,api",
    [(False, 5, "table"), (True, 10, "table"), (True, 5, "csv")],
    ids=["pages", "partitions_10_workers", "csv_partitions"],
)
def test_performance_split_table(
    perf_http: HttpLog, partitioned: bool, workers: int, api: TReadApi
) -> None:
    os.environ["EXTRACT__WORKERS"] = str(workers)
    pipeline = dlt.pipeline(
        pipeline_name=f"servicenow_perf_split_{partitioned}_{workers}_{api}",
        destination="duckdb",
        dev_mode=True,
    )
    resource = servicenow_table(
        table=TABLE, api=api, allow_partitioned_load=partitioned
    )
    assert_load_info(pipeline.run(resource))
    print_extract_speed(
        pipeline,
        f"servicenow {TABLE} {api}, partitioned {partitioned}, {workers} workers",
        perf_http,
    )
    assert_perf_rows(pipeline, TABLE)


@pytest.mark.parametrize(
    "backend,api", [("pyarrow", "table"), ("pyarrow", "csv")], ids=["pyarrow", "csv"]
)
def test_performance_incremental(
    perf_http: HttpLog, backend: TTableBackend, api: TReadApi
) -> None:
    values = [record[data.PERF_CURSOR] for record in data.perf_records(TABLE)]
    pipeline = dlt.pipeline(
        pipeline_name=f"servicenow_perf_incremental_{backend}_{api}",
        destination="duckdb",
        dev_mode=True,
    )
    assert_load_info(pipeline.run(perf_table(backend, api, partitioned=True)))
    print_extract_speed(
        pipeline, f"servicenow {TABLE} {backend} {api}, incremental", perf_http
    )
    assert last_row_count(pipeline, TABLE) == data.PERF_RECORDS
    assert_perf_rows(pipeline, TABLE)
    pipeline.run(perf_table(backend, api, partitioned=True))
    assert last_row_count(pipeline, TABLE) == 0

    for last_value_func, bounds in seeded_windows(values, timedelta(seconds=1)):
        backfill = dlt.pipeline(
            pipeline_name=f"servicenow_perf_backfill_{backend}_{api}_{last_value_func.__name__}",
            destination="duckdb",
            dev_mode=True,
        )
        for start, end in zip(bounds, bounds[1:]):
            window = perf_table(
                backend,
                api,
                partitioned=True,
                initial_value=start,
                end_value=end,
                last_value_func=last_value_func,
                range_start="closed",
                range_end="open",
            )
            assert_load_info(backfill.run(window))
            assert last_row_count(backfill, TABLE) == sum(
                in_window(value, start, end, last_value_func) for value in values
            )
        assert "sources" not in backfill.state
        assert_perf_rows(backfill, TABLE)

    # `sys_id` ranges are not read with `add_limit`, pages come in the order of the cursor
    chunk_size = 9000
    for last_value_func in (max, min):
        chunks = dlt.pipeline(
            pipeline_name=f"servicenow_perf_chunks_{backend}_{api}_{last_value_func.__name__}",
            destination="duckdb",
            dev_mode=True,
        )
        resource = perf_table(
            backend,
            api,
            partitioned=False,
            page_size=chunk_size,
            last_value_func=last_value_func,
            row_order="asc",
            range_start="open",
        )
        expected = sorted(values, reverse=last_value_func is min)
        for offset in range(0, len(expected), chunk_size):
            chunk = expected[offset : offset + chunk_size]
            assert_load_info(chunks.run(resource.add_limit(1)))
            assert last_row_count(chunks, TABLE) == len(chunk)
            last_value = resource.state["incremental"][data.PERF_CURSOR]["last_value"]
            assert last_value == chunk[-1]
            assert last_value.tzinfo is not None
        chunks.run(resource)
        assert last_row_count(chunks, TABLE) == 0
        assert_perf_rows(chunks, TABLE)

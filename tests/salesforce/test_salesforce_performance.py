from datetime import timedelta  # noqa: I251
from functools import partial
from typing import Any, Iterator

import dlt
import pytest
from dlt.common.configuration.exceptions import ConfigFieldMissingException
from dlt.sources import DltResource

from sources.salesforce import salesforce_table
from sources.salesforce.helpers.credentials import (
    SalesforceAuth,
    make_salesforce_client,
)
from sources.salesforce.helpers.query import TQueryApi
from sources.salesforce.helpers.table import TTableBackend
from tests.perf_utils import assert_loaded_rows, in_window, seeded_windows
from tests.salesforce.seed import data
from tests.salesforce.utils import performance_credentials
from tests.seed_utils import HttpLog, account_lock, print_extract_speed
from tests.utils import assert_load_info, last_row_count, skip_if_not_active

pytestmark = [pytest.mark.performance, skip_if_not_active("duckdb")]

SOBJECT = data.PERF_OBJECTS[0]


@pytest.fixture(scope="module")
def perf_account() -> Iterator[SalesforceAuth]:
    """Credentials of the performance org, held for the performance tests. Skips when it is not
    configured or not seeded
    """
    try:
        credentials = performance_credentials()
    except ConfigFieldMissingException:
        pytest.skip(f"configure `sources.{data.PERF_SECTION}`, see README.md")
    with account_lock(data.PERF_SECTION):
        client = make_salesforce_client(credentials)
        for sobject in data.PERF_OBJECTS:
            count = client.query(f"SELECT COUNT() FROM {sobject}")["totalSize"]
            if count != data.PERF_RECORDS:
                pytest.skip(
                    f"{sobject} has {count} records, run the seed with --performance"
                )
        yield credentials


def assert_perf_rows(pipeline: dlt.Pipeline, sobject: str) -> None:
    assert_loaded_rows(
        pipeline,
        sobject,
        "Number__c",
        partial(data.perf_record, sobject),
        range(data.PERF_RECORDS),
    )


def test_performance_baseline(perf_account: SalesforceAuth, perf_http: HttpLog) -> None:
    pipeline = dlt.pipeline(
        pipeline_name="salesforce_perf_baseline", destination="duckdb", dev_mode=True
    )
    assert_load_info(
        pipeline.run(salesforce_table(credentials=perf_account, sobject=SOBJECT))
    )
    print_extract_speed(pipeline, f"salesforce {SOBJECT}, defaults", perf_http)
    assert_perf_rows(pipeline, SOBJECT)


@pytest.mark.parametrize(
    "backend,api",
    [("pyarrow", "bulk"), ("python", "bulk"), ("pyarrow", "rest")],
    ids=["pyarrow-bulk", "python-bulk", "pyarrow-rest"],
)
def test_performance_parallel_tables(
    perf_account: SalesforceAuth,
    perf_http: HttpLog,
    backend: TTableBackend,
    api: TQueryApi,
) -> None:
    pipeline = dlt.pipeline(
        pipeline_name=f"salesforce_perf_tables_{backend}_{api}",
        destination="duckdb",
        dev_mode=True,
    )
    tables = [
        salesforce_table(
            credentials=perf_account, sobject=sobject, backend=backend, api=api
        )
        for sobject in data.PERF_OBJECTS
    ]
    assert_load_info(pipeline.run(tables))
    print_extract_speed(
        pipeline, f"salesforce {backend} {api}, {len(tables)} objects", perf_http
    )
    for sobject in data.PERF_OBJECTS:
        assert_perf_rows(pipeline, sobject)


@pytest.mark.parametrize(
    "api,chunk_size",
    [("bulk", 10_000), ("rest", 2000)],
    ids=["bulk-10000", "rest-2000"],
)
def test_performance_split_table(
    perf_account: SalesforceAuth, perf_http: HttpLog, api: TQueryApi, chunk_size: int
) -> None:
    pipeline = dlt.pipeline(
        pipeline_name=f"salesforce_perf_split_{api}_{chunk_size}",
        destination="duckdb",
        dev_mode=True,
    )
    table = salesforce_table(
        credentials=perf_account,
        sobject=SOBJECT,
        backend="pyarrow",
        api=api,
        chunk_size=chunk_size,
    )
    assert_load_info(pipeline.run(table))
    print_extract_speed(
        pipeline, f"salesforce {SOBJECT} {api}, pages of {chunk_size}", perf_http
    )
    assert_perf_rows(pipeline, SOBJECT)


@pytest.mark.parametrize("api", ["bulk", "rest"])
def test_performance_incremental(
    perf_account: SalesforceAuth, perf_http: HttpLog, api: TQueryApi
) -> None:
    values = [record[data.PERF_CURSOR] for record in data.perf_records(SOBJECT)]

    def perf_table(chunk_size: int = 50_000, **incremental_args: Any) -> DltResource:
        return salesforce_table(
            credentials=perf_account,
            sobject=SOBJECT,
            backend="pyarrow",
            api=api,
            chunk_size=chunk_size,
            incremental=dlt.sources.incremental(data.PERF_CURSOR, **incremental_args),
        )

    pipeline = dlt.pipeline(
        pipeline_name=f"salesforce_perf_incremental_{api}",
        destination="duckdb",
        dev_mode=True,
    )
    assert_load_info(pipeline.run(perf_table()))
    print_extract_speed(pipeline, f"salesforce {SOBJECT} {api}, incremental", perf_http)
    assert last_row_count(pipeline, SOBJECT) == data.PERF_RECORDS
    assert_perf_rows(pipeline, SOBJECT)
    # merge on a timestamp turns off deduplication, the closed range reads the last value again
    assert_load_info(pipeline.run(perf_table()))
    assert last_row_count(pipeline, SOBJECT) == 1

    for last_value_func, bounds in seeded_windows(values, timedelta(seconds=1)):
        backfill = dlt.pipeline(
            pipeline_name=f"salesforce_perf_backfill_{api}_{last_value_func.__name__}",
            destination="duckdb",
            dev_mode=True,
        )
        for start, end in zip(bounds, bounds[1:]):
            window = perf_table(
                initial_value=start,
                end_value=end,
                last_value_func=last_value_func,
                range_start="closed",
                range_end="open",
            )
            assert_load_info(backfill.run(window))
            assert last_row_count(backfill, SOBJECT) == sum(
                in_window(value, start, end, last_value_func) for value in values
            )
        assert "sources" not in backfill.state
        assert_perf_rows(backfill, SOBJECT)

    # `add_limit` counts pages, REST pages hold at most 2000 records. both leave a partial chunk
    chunk_size = 12_000 if api == "bulk" else 1900
    for last_value_func in (max, min):
        chunks = dlt.pipeline(
            pipeline_name=f"salesforce_perf_chunks_{api}_{last_value_func.__name__}",
            destination="duckdb",
            dev_mode=True,
        )
        table = perf_table(
            chunk_size=chunk_size,
            row_order="asc",
            last_value_func=last_value_func,
            range_start="open",
        )
        expected = sorted(values, reverse=last_value_func is min)
        for offset in range(0, len(expected), chunk_size):
            chunk = expected[offset : offset + chunk_size]
            assert_load_info(chunks.run(table.add_limit(1)))
            assert last_row_count(chunks, SOBJECT) == len(chunk)
            last_value = table.state["incremental"][data.PERF_CURSOR]["last_value"]
            assert last_value == chunk[-1]
            assert last_value.tzinfo is not None
        chunks.run(table)
        assert last_row_count(chunks, SOBJECT) == 0
        assert_perf_rows(chunks, SOBJECT)

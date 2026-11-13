import os
from datetime import timedelta
from functools import partial
from typing import Any, Dict, Iterator, Optional, Sequence

import dlt
import pytest
from dlt.sources.helpers.requests import HTTPError

from sources.jira import jira_issues
from sources.jira.helpers.client import JiraClient

from tests.jira.seed_test_data import search_ids
from tests.jira.utils import (
    PERF_CURSOR_FIELD,
    PERF_ISSUES,
    PERF_NUMBER_FIELD,
    PERF_PROJECTS,
    perf_issue,
    perf_jql,
)
from tests.seed_utils import HttpLog, account_lock, print_extract_speed
from tests.utils import (
    assert_load_info,
    assert_loaded_rows,
    in_window,
    last_row_count,
    seeded_windows,
    skip_if_not_active,
)

pytestmark = [pytest.mark.performance, skip_if_not_active("duckdb")]

PROJECT = PERF_PROJECTS[0]
TABLE = PROJECT.lower()


@pytest.fixture(scope="module")
def perf_account() -> Iterator[None]:
    """Holds the site for the performance tests, skips when the projects are not seeded"""
    with account_lock("jira"):
        client = JiraClient(
            dlt.secrets["sources.jira.subdomain"],
            dlt.secrets["sources.jira.email"],
            dlt.secrets["sources.jira.api_token"],
        )
        for project in PERF_PROJECTS:
            try:
                count = len(search_ids(client, perf_jql(project)))
            except HTTPError:
                # JQL with a missing project is invalid
                count = 0
            if count != PERF_ISSUES:
                pytest.skip(
                    f"{project} has {count} issues, run the seed with --performance"
                )
        yield


def assert_perf_rows(pipeline: dlt.Pipeline, project: str) -> None:
    assert_loaded_rows(
        pipeline,
        project.lower(),
        PERF_NUMBER_FIELD,
        partial(perf_issue, project),
        range(PERF_ISSUES),
    )


def test_performance_baseline(perf_http: HttpLog) -> None:
    pipeline = dlt.pipeline(
        pipeline_name="jira_perf_baseline", destination="duckdb", dev_mode=True
    )
    assert_load_info(pipeline.run(jira_issues(jql=perf_jql(PROJECT), name=TABLE)))
    print_extract_speed(pipeline, f"jira {PROJECT}, defaults", perf_http)
    assert_perf_rows(pipeline, PROJECT)


@pytest.mark.parametrize("bulk_fetch_size", [100, 1000])
def test_performance_parallel_tables(perf_http: HttpLog, bulk_fetch_size: int) -> None:
    pipeline = dlt.pipeline(
        pipeline_name=f"jira_perf_tables_{bulk_fetch_size}",
        destination="duckdb",
        dev_mode=True,
    )
    resources = [
        jira_issues(
            jql=perf_jql(project), name=project.lower(), bulk_fetch_size=bulk_fetch_size
        )
        for project in PERF_PROJECTS
    ]
    assert_load_info(pipeline.run(resources))
    print_extract_speed(
        pipeline,
        f"jira {len(resources)} projects, bulk fetch {bulk_fetch_size}",
        perf_http,
    )
    for project in PERF_PROJECTS:
        assert_perf_rows(pipeline, project)


@pytest.mark.parametrize(
    "workers,columns",
    [(1, {}), (10, {}), (20, {}), (5, {"excluded_columns": ()})],
    ids=["1_worker", "10_workers", "20_workers", "all_fields"],
)
def test_performance_split_table(
    perf_http: HttpLog, workers: int, columns: Dict[str, Sequence[str]]
) -> None:
    # pages of `chunk_size` issues are fetched in parallel by the extract workers
    os.environ["EXTRACT__WORKERS"] = str(workers)
    pipeline = dlt.pipeline(
        pipeline_name=f"jira_perf_split_{workers}_{bool(columns)}",
        destination="duckdb",
        dev_mode=True,
    )
    issues = jira_issues(jql=perf_jql(PROJECT), name=TABLE, **columns)
    assert_load_info(pipeline.run(issues))
    fields = "all fields" if columns else "default fields"
    print_extract_speed(
        pipeline, f"jira {PROJECT}, {workers} workers, {fields}", perf_http
    )
    assert_perf_rows(pipeline, PROJECT)


def test_performance_incremental(perf_http: HttpLog) -> None:
    values = sorted(
        perf_issue(PROJECT, n)[PERF_CURSOR_FIELD] for n in range(PERF_ISSUES)
    )

    def perf_issues(chunk_size: Optional[int] = None, **incremental_args: Any) -> Any:
        return jira_issues(
            jql=perf_jql(PROJECT),
            name=TABLE,
            chunk_size=chunk_size,
            incremental=dlt.sources.incremental(PERF_CURSOR_FIELD, **incremental_args),
        )

    pipeline = dlt.pipeline(
        pipeline_name="jira_perf_incremental", destination="duckdb", dev_mode=True
    )
    assert_load_info(pipeline.run(perf_issues()))
    print_extract_speed(pipeline, f"jira {PROJECT}, incremental", perf_http)
    assert last_row_count(pipeline, TABLE) == PERF_ISSUES
    assert_perf_rows(pipeline, PROJECT)
    pipeline.run(perf_issues())
    assert last_row_count(pipeline, TABLE) == 0

    for last_value_func, bounds in seeded_windows(values, timedelta(minutes=1)):
        backfill = dlt.pipeline(
            pipeline_name=f"jira_perf_backfill_{last_value_func.__name__}",
            destination="duckdb",
            dev_mode=True,
        )
        for start, end in zip(bounds, bounds[1:]):
            window = perf_issues(
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
        assert_perf_rows(backfill, PROJECT)

    # JQL compares whole minutes: an open range lists the issue of the last value again and the
    # incremental drops it, so runs after the first load one issue less than the chunk. 4500
    # leaves a partial last chunk
    chunk_size = 4500
    for last_value_func in (max, min):
        chunks = dlt.pipeline(
            pipeline_name=f"jira_perf_chunks_{last_value_func.__name__}",
            destination="duckdb",
            dev_mode=True,
        )
        issues = perf_issues(
            chunk_size=chunk_size,
            row_order="asc",
            last_value_func=last_value_func,
            range_start="open",
        )
        expected = sorted(values, reverse=last_value_func is min)
        offset = 0
        while offset < len(expected):
            size = chunk_size if offset == 0 else chunk_size - 1
            chunk = expected[offset : offset + size]
            offset += size
            assert_load_info(chunks.run(issues.add_limit(1)))
            assert last_row_count(chunks, TABLE) == len(chunk)
            last_value = next(
                source["resources"][f"{TABLE}_ids"]["incremental"][PERF_CURSOR_FIELD]
                for source in chunks.state["sources"].values()
            )["last_value"]
            assert last_value == chunk[-1]
            assert last_value.tzinfo is not None
        chunks.run(issues)
        assert last_row_count(chunks, TABLE) == 0
        assert_perf_rows(chunks, PROJECT)

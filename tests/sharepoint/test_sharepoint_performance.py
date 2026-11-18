from typing import Iterator

import dlt
import pytest

from sources.sharepoint import sharepoint_list

from tests.perf_utils import assert_loaded_rows
from tests.seed_utils import HttpLog, account_lock, print_extract_speed
from tests.sharepoint.seed.data import PERF_ITEMS, PERF_LIST
from tests.sharepoint.utils import graph_client
from tests.utils import assert_load_info, skip_if_not_active

pytestmark = [pytest.mark.performance, skip_if_not_active("duckdb")]


@pytest.fixture(scope="module")
def perf_account() -> Iterator[None]:
    """Holds the site for the performance tests, skips when the performance list is missing"""
    with account_lock("sharepoint"):
        titles = {
            site_list["displayName"]
            for page in graph_client().paginate(
                "lists", params={"$select": "displayName"}
            )
            for site_list in page
        }
        if PERF_LIST not in titles:
            pytest.skip(f"{PERF_LIST} is missing, run the seed with --performance")
        yield


def assert_perf_rows(pipeline: dlt.Pipeline, table: str) -> None:
    assert_loaded_rows(pipeline, table, "Number", lambda n: {}, range(PERF_ITEMS))


def test_performance_baseline(perf_http: HttpLog) -> None:
    pipeline = dlt.pipeline(
        pipeline_name="sharepoint_perf_baseline", destination="duckdb", dev_mode=True
    )
    assert_load_info(pipeline.run(sharepoint_list(PERF_LIST).with_name("perf")))
    print_extract_speed(pipeline, f"sharepoint {PERF_LIST}, defaults", perf_http)
    assert_perf_rows(pipeline, "perf")


@pytest.mark.parametrize("parallel", [False, True], ids=["serial", "parallel"])
def test_performance_two_lists(perf_http: HttpLog, parallel: bool) -> None:
    # the same list as two resources, read one after another or each on an extract worker
    tables = ["perf_a", "perf_b"]
    pipeline = dlt.pipeline(
        pipeline_name=f"sharepoint_perf_lists_{parallel}",
        destination="duckdb",
        dev_mode=True,
    )
    resources = [sharepoint_list(PERF_LIST).with_name(table) for table in tables]
    if parallel:
        resources = [resource.parallelize() for resource in resources]
    assert_load_info(pipeline.run(resources))
    mode = "parallel" if parallel else "serial"
    print_extract_speed(
        pipeline, f"sharepoint {PERF_LIST} as {len(tables)} lists, {mode}", perf_http
    )
    assert perf_http.peak_concurrency() == (len(tables) if parallel else 1)
    for table in tables:
        assert_perf_rows(pipeline, table)

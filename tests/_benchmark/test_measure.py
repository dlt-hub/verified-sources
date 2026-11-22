from typing import Any, Dict, Iterator, List, Optional

import dlt
import pyarrow as pa
import pytest
import requests_mock
from dlt.sources import DltResource
from dlt.sources.helpers.requests import Session

from tests._benchmark.cases import BenchmarkCase, TLoad, load_args, nothing
from tests._benchmark.measure import CpuSampler, bound, count_rows, measure
from tests._benchmark.report import render_report, run_metadata
from tests.seed_utils import HttpLog, RequestTiming, record_http


def test_record_http_bytes_and_redirects() -> None:
    body = b"x" * 1000
    with requests_mock.Mocker() as m:
        m.get("https://example.com/data", content=body)
        m.get(
            "https://example.com/moved",
            status_code=302,
            headers={"Location": "https://example.com/data"},
        )
        with record_http("test", print_summary=False) as http:
            session = Session()
            assert session.get("https://example.com/data").content == body
            assert session.get("https://example.com/moved").content == body

    # the redirect is a part of the request that followed it
    assert len(http.requests) == 2
    assert http.redirects == 1
    assert dict(http.statuses) == {200: 2}
    assert http.body_bytes() == 2 * len(body)
    assert http.wire_bytes() == 2 * len(body)
    for timing in http.requests:
        assert timing.finished >= timing.started
        assert timing.cpu >= 0


def test_record_http_streamed_body() -> None:
    body = b"x" * 1000
    with requests_mock.Mocker() as m:
        m.get("https://example.com/data", content=body)
        with record_http("test", print_summary=False) as http:
            session = Session()
            # a streamed request ends when the caller read its body or closed the response
            response = session.get("https://example.com/data", stream=True)
            assert http.requests == []
            assert b"".join(response.iter_content(100)) == body
            response.close()
            closed = session.get("https://example.com/data", stream=True)
            next(closed.iter_content(100))
            closed.close()
            closed.close()

    assert len(http.requests) == 2
    assert http.body_bytes() == len(body) + 100


def test_http_log_busy_seconds_and_concurrency() -> None:
    log = HttpLog()
    for started, finished in [(0.0, 2.0), (1.0, 3.0), (5.0, 6.0), (6.0, 7.0)]:
        log.add(
            "https://example.com",
            {},
            RequestTiming(started, finished, 0.1, 10, 5, 0.0, 200),
        )
    assert log.busy_seconds() == 5.0
    # requests that touch do not overlap
    assert log.peak_concurrency() == 2
    assert log.wire_bytes() == 20
    log.add("https://example.com", {}, RequestTiming(8.0, 9.0, 0.1, 10, -1, 0.0, 200))
    assert log.wire_bytes() is None


@pytest.mark.parametrize(
    "cpu_util,busy_share,to_headers_s,transfer_s,download_mb_s,expected",
    [
        (0.9, 1.0, 1.0, 0.0, 50.0, "cpu"),
        (0.2, 0.95, 8.0, 2.0, 50.0, "server"),
        (0.2, 0.95, 2.0, 8.0, 0.5, "server"),
        (0.2, 0.95, 2.0, 8.0, 50.0, "network"),
        (0.2, 0.4, 2.0, 8.0, 50.0, "waits"),
    ],
    ids=["cpu", "server-headers", "server-streamed-body", "network", "waits"],
)
def test_bound(
    cpu_util: float,
    busy_share: float,
    to_headers_s: float,
    transfer_s: float,
    download_mb_s: float,
    expected: str,
) -> None:
    result = {
        "cpu_util": cpu_util,
        "busy_share": busy_share,
        "to_headers_s": to_headers_s,
        "transfer_s": transfer_s,
        "download_mb_s": download_mb_s,
    }
    assert bound(result) == expected


def test_count_rows() -> None:
    assert count_rows(pa.table({"a": [1, 2, 3]})) == 3
    assert count_rows([{"a": 1}, {"a": 2}]) == 2
    assert count_rows({"a": 1}) == 1


def test_cpu_sampler() -> None:
    sampler = CpuSampler()
    sampler.samples = [
        (0.0, 0.0, 0),
        (1.0, 0.5, 0),
        (2.0, 1.5, 2**20),
        (10.0, 2.0, 0),
    ]
    assert sampler.peak_utilization(window=5.0) == 0.75
    assert sampler.peak_rss() == 2**20
    assert sampler.timeline()[2] == (2.0, 1.5, 1.0)

    with CpuSampler(interval=0.01) as running:
        sum(range(1_000_000))
    assert len(running.samples) >= 2
    assert running.peak_rss() > 0


@dlt.resource(name="numbers")
def numbers(
    incremental: Optional[dlt.sources.incremental[int]] = dlt.sources.incremental("n"),
) -> Iterator[List[Dict[str, int]]]:
    yield [{"n": i} for i in range(100)]


def numbers_resource(load: TLoad) -> DltResource:
    return numbers(**load_args(load))


def is_configured() -> bool:
    return True


def test_measure_and_report() -> None:
    case = BenchmarkCase(
        "numbers",
        "jira",
        "benchmark-unit-test",
        numbers_resource,
        100,
        is_configured,
        nothing,
    )
    # an extract leaves no incremental state behind for the following runs
    variants = [
        ("extract", "incremental"),
        ("iterate", "incremental"),
        ("extract", "incremental"),
        ("iterate", "full"),
    ]
    results = [
        measure(case, mode, load, round_no)  # type: ignore[arg-type]
        for round_no, (mode, load) in enumerate(variants, start=1)
    ]
    for result in results:
        assert result["rows"] == 100
        assert result["valid"] is True
        assert result["requests"] == 0
        assert 0 < result["first_item_s"] <= result["wall_s"]
        assert result["timeline"]

    failed = {
        "case": "numbers",
        "mode": "iterate",
        "load": "full",
        "round": 5,
        "error": "HTTPError: 503",
    }
    report = render_report(results + [failed], run_metadata(["jira"]))
    assert "| numbers | incremental | extract | 2 | 100 |" in report
    assert "| numbers | incremental | iterate | 1 | 100 |" in report
    assert "| numbers | full | iterate | 1 | 100 |" in report
    assert "- numbers full iterate round 5: HTTPError: 503" in report

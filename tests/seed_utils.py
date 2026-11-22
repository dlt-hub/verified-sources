"""Helpers of the seed scripts and of the performance tests that read the seeded accounts"""

import json
import os
import tempfile
import threading
import warnings
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone  # noqa: I251
from time import monotonic, thread_time
from typing import (
    Any,
    Callable,
    Dict,
    Hashable,
    Iterable,
    Iterator,
    List,
    Mapping,
    NamedTuple,
    Optional,
    Sequence,
    Tuple,
    TypeVar,
)
from unittest.mock import patch
from urllib.parse import urlsplit

import dlt
from dlt.common import Decimal

# patching the base class records the requests of all clients
from requests import Response, Session  # noqa: I251

TItem = TypeVar("TItem")
TKey = TypeVar("TKey", bound=Hashable)
TResult = TypeVar("TResult")

# coprime with the periods, so `spread` gives each record of a period its own value
SPREAD_PRIME = 1_000_003
# performance data: cursor values over two years, the second table starts half a year later
PERF_PERIOD = timedelta(days=730)
PERF_STARTS = (
    datetime(2023, 1, 1, tzinfo=timezone.utc),
    datetime(2023, 7, 1, tzinfo=timezone.utc),
)
PERF_WORDS = ["alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel"]
SECOND = timedelta(seconds=1)
MINUTE = timedelta(minutes=1)
MB = 1024 * 1024
# wire size of a compressed body sent in chunks
UNKNOWN_BYTES = -1


def spread(
    n: int,
    start: datetime,
    period: timedelta,
    unit: timedelta = SECOND,
    prime: int = SPREAD_PRIME,
) -> datetime:
    """A value in `period` after `start` in whole `unit`s, unique for each `n` and out of order.
    Unlike `random`, stable across Python versions
    """
    return start + unit * (n * prime % (period // unit))


def perf_description(n: int) -> Optional[str]:
    """Text of up to 40 words, empty for every tenth record"""
    if n % 10 == 0:
        return None
    return " ".join(PERF_WORDS[(n + i) % len(PERF_WORDS)] for i in range(n % 40 + 1))


def perf_values(n: int, start: datetime) -> Dict[str, Any]:
    """Values of the record `n` of a performance table, by kind. None is an empty value"""
    return {
        "number": n,
        "cursor": spread(n, start, PERF_PERIOD),
        "description": perf_description(n),
        "code": f"C{n * 7919 % 100_000:05d}",
        "count": n * 31 % 1000 - 500,
        "amount": Decimal(n * 37 % 1_000_000).scaleb(-2),
        # eighths are exact binary floats
        "ratio": n % 1000 / 8,
        "active": n % 3 == 0,
        "due": date(2024, 1, 1) + timedelta(days=n % 730),
        "started_at": start - timedelta(minutes=n),
    }


def map_with_progress(
    func: Callable[[TItem], TResult],
    items: Sequence[TItem],
    label: str,
    workers: int = 8,
    weight: Callable[[TItem], int] = lambda item: 1,
) -> List[TResult]:
    """Calls `func` on `items` in a thread pool and prints the rate every 2000 weighted items"""
    total = sum(weight(item) for item in items)
    results: List[TResult] = []
    done = 0
    printed = 0
    started = monotonic()
    with ThreadPoolExecutor(workers) as pool:
        for item, result in zip(items, pool.map(func, items)):
            results.append(result)
            done += weight(item)
            if done - printed >= 2000 or done == total:
                printed = done
                rate = done / (monotonic() - started)
                left = (total - done) / rate / 60
                print(f"  {label}: {done}/{total}, {rate:.0f}/s, {left:.1f} min left")
    return results


def report_differences(
    label: str, expected: Mapping[TKey, Any], stored: Iterable[Tuple[TKey, Any]]
) -> List[str]:
    """Compares `stored` (key, values) pairs with the `expected` values by key, prints a summary
    and returns the problems
    """
    stored = list(stored)
    counts = Counter(key for key, _ in stored)
    problems = {
        "missing": sorted(expected.keys() - counts.keys(), key=str),
        "unexpected": sorted(counts.keys() - expected.keys(), key=str),
        "duplicate": sorted(
            (key for key, count in counts.items() if count > 1), key=str
        ),
        "different": sorted(
            {
                key
                for key, values in stored
                if key in expected and values != expected[key]
            },
            key=str,
        ),
    }
    print(
        f"  {label}: {len(stored)} records, "
        + ", ".join(f"{len(keys)} {problem}" for problem, keys in problems.items())
    )
    return [
        f"{label}: {problem} {keys[:5]}" for problem, keys in problems.items() if keys
    ]


def raise_differences(errors: List[str]) -> None:
    if errors:
        raise ValueError(
            "Seeded records differ from the seed data, seed again to fix them:\n"
            + "\n".join(errors)
        )


# response headers of the tested APIs that report rate limits and throttling
RATE_LIMIT_HEADERS = (
    "Retry-After",
    "X-RateLimit-Limit",
    "X-RateLimit-Remaining",
    "RateLimit",
    "RateLimit-Reason",
    "Sforce-Limit-Info",
)
THROTTLED_STATUSES = (429, 503)


class RequestTiming(NamedTuple):
    """A request sent and read by a thread: monotonic start and end, time to the response headers,
    decoded body and wire bytes, and the CPU time the sending thread spent in it
    """

    started: float
    finished: float
    to_headers: float
    body_bytes: int
    wire_bytes: int
    cpu: float
    status: int


class HttpLog:
    """Statuses, durations and rate limit headers of HTTP responses. Durations include reading
    the body: streaming APIs send headers before they compute the body
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.started = monotonic()
        self.statuses: Counter[int] = Counter()
        self.durations: List[float] = []
        self.to_headers: List[float] = []
        self.requests: List[RequestTiming] = []
        self.redirects = 0
        self.hosts: Counter[str] = Counter()
        self.throttled: List[str] = []
        self.errors: Counter[str] = Counter()
        self.rate_limits: Dict[str, str] = {}

    def add(self, url: str, headers: Mapping[str, str], timing: RequestTiming) -> None:
        limits = {name: headers[name] for name in RATE_LIMIT_HEADERS if name in headers}
        with self._lock:
            self.statuses[timing.status] += 1
            self.durations.append(timing.finished - timing.started)
            self.to_headers.append(timing.to_headers)
            self.requests.append(timing)
            self.hosts[urlsplit(url).hostname or ""] += 1
            self.rate_limits.update(limits)
            if timing.status in THROTTLED_STATUSES:
                self.throttled.append(
                    f"+{monotonic() - self.started:.1f} s: {timing.status}"
                    f" {urlsplit(url).path} {limits}"
                )

    def body_bytes(self) -> int:
        return sum(timing.body_bytes for timing in self.requests)

    def wire_bytes(self) -> Optional[int]:
        """Bytes received, None if the size of a compressed chunked body is not known"""
        if any(timing.wire_bytes == UNKNOWN_BYTES for timing in self.requests):
            return None
        return sum(timing.wire_bytes for timing in self.requests)

    def busy_seconds(self) -> float:
        """Seconds in which at least one request was in flight"""
        busy, end = 0.0, float("-inf")
        for timing in sorted(self.requests):
            if timing.started > end:
                busy += timing.finished - timing.started
            elif timing.finished > end:
                busy += timing.finished - end
            end = max(end, timing.finished)
        return busy

    def peak_concurrency(self) -> int:
        """The largest number of requests in flight at once"""
        # requests that end at the same moment another starts do not overlap
        events = sorted(
            [(timing.started, 1) for timing in self.requests]
            + [(timing.finished, -1) for timing in self.requests]
        )
        peak = in_flight = 0
        for _, change in events:
            in_flight += change
            peak = max(peak, in_flight)
        return peak

    def summary(self, label: str) -> str:
        def percentiles(values: List[float]) -> str:
            ordered = sorted(values) or [0.0]
            p50, p95 = (
                ordered[min(len(ordered) - 1, int(q * len(ordered)))]
                for q in (0.5, 0.95)
            )
            return f"p50 {p50:.2f} s, p95 {p95:.2f} s, max {ordered[-1]:.2f} s"

        wire_bytes = self.wire_bytes()
        wire = "unknown" if wire_bytes is None else f"{wire_bytes / MB:.1f} MB"
        lines = [
            f"{label}: {len(self.durations)} requests, statuses {dict(sorted(self.statuses.items()))}"
            " (0 is a connection error)",
            f"  duration {percentiles(self.durations)}, sum {sum(self.durations):.1f} s",
            f"  time to headers {percentiles(self.to_headers)}",
            f"  received {self.body_bytes() / MB:.1f} MB, {wire} on the wire, in flight"
            f" {self.busy_seconds():.1f} s, peak concurrency {self.peak_concurrency()}",
            f"  connection errors: {dict(self.errors)}",
            f"  throttled responses: {len(self.throttled)}",
            *(f"    {line}" for line in self.throttled[:20]),
        ]
        if self.rate_limits:
            lines.append(f"  last rate limit headers: {self.rate_limits}")
        return "\n".join(lines)


def response_timing(
    response: Optional[Response],
    started: float,
    cpu: float,
    body_bytes: Optional[int] = None,
) -> RequestTiming:
    """Timing of a request that ends now, `response` is None after a connection error.
    `body_bytes` are those of a streamed body that the caller read
    """
    finished = monotonic()
    if response is None:
        return RequestTiming(started, finished, 0.0, 0, 0, cpu, 0)
    length = int(response.headers.get("Content-Length") or 0)
    if body_bytes is None:
        # a body that the caller did not read yet must stay unread
        body_bytes = (
            len(response.content)
            if getattr(response, "_content_consumed", True)
            else length
        )
    try:
        # urllib3 does not count the bytes of chunked bodies
        wire_bytes = int(response.raw.tell()) or length
    except Exception:
        wire_bytes = length
    if not wire_bytes and body_bytes:
        compressed = "Content-Encoding" in response.headers
        wire_bytes = UNKNOWN_BYTES if compressed else body_bytes
    return RequestTiming(
        started,
        finished,
        response.elapsed.total_seconds(),
        body_bytes,
        wire_bytes,
        cpu,
        response.status_code,
    )


class StreamedTiming:
    """Adds the timing of a streamed response to `log` once the caller read its body or closed
    it. The body is read after `send` returns, possibly on other threads
    """

    def __init__(
        self, log: HttpLog, url: str, response: Response, started: float, cpu: float
    ) -> None:
        self.log, self.url, self.response, self.started = log, url, response, started
        self.cpu, self.body_bytes, self.done = cpu, 0, False
        self._iter_content, self._close = response.iter_content, response.close
        response.iter_content = self.iter_content  # type: ignore[method-assign]
        response.close = self.close  # type: ignore[method-assign]

    def iter_content(self, *args: Any, **kwargs: Any) -> Iterator[Any]:
        blocks = self._iter_content(*args, **kwargs)
        while True:
            cpu_started = thread_time()
            block = next(blocks, None)
            self.cpu += thread_time() - cpu_started
            if block is None:
                self.finish()
                return
            self.body_bytes += len(block)
            yield block

    def close(self) -> None:
        self.finish()
        self._close()

    def finish(self) -> None:
        if self.done:
            return
        self.done = True
        timing = response_timing(self.response, self.started, self.cpu, self.body_bytes)
        self.log.add(self.url, self.response.headers, timing)


@contextmanager
def record_http(label: str, print_summary: bool = True) -> Iterator[HttpLog]:
    """Records the HTTP responses of all threads and prints their summary. Warns when the API
    throttled, the timings of such a run are not comparable
    """
    log = HttpLog()
    send = Session.send
    # redirects call `send` again inside the request that they belong to
    nested = threading.local()

    # dlt sessions call `requests.Session.send` for every attempt, it also reads the body
    def recording_send(session: Session, request: Any, **kwargs: Any) -> Any:
        if getattr(nested, "active", False):
            with log._lock:
                log.redirects += 1
            return send(session, request, **kwargs)
        nested.active = True
        started, cpu_started = monotonic(), thread_time()
        try:
            response = send(session, request, **kwargs)
        except Exception as ex:
            # response hooks raise for error statuses, those requests got a response
            failed = getattr(ex, "response", None)
            log.add(
                request.url,
                {} if failed is None else failed.headers,
                response_timing(failed, started, thread_time() - cpu_started),
            )
            if failed is None:
                with log._lock:
                    log.errors[type(ex).__name__] += 1
            raise
        finally:
            nested.active = False
        cpu = thread_time() - cpu_started
        if kwargs.get("stream") and not getattr(response, "_content_consumed", True):
            StreamedTiming(log, request.url, response, started, cpu)
        else:
            log.add(
                request.url, response.headers, response_timing(response, started, cpu)
            )
        return response

    with patch.object(Session, "send", recording_send):
        try:
            yield log
        finally:
            if print_summary:
                print(log.summary(label))
            if log.throttled:
                warnings.warn(
                    f"{label}: the API throttled {len(log.throttled)} requests, the timings are"
                    " not comparable"
                )


def print_extract_speed(pipeline: dlt.Pipeline, label: str, http: HttpLog) -> None:
    """Prints the rows and extract time of the last run of `pipeline` and the requests in `http`.
    Appends them to the JSON lines file in env `PERF_RESULTS`, if set
    """
    trace = pipeline.last_trace
    extract = next(step for step in trace.steps if step.step == "extract")
    seconds = (extract.finished_at - extract.started_at).total_seconds()
    # rows of the top level tables, child tables hold nested values of the same rows
    tables = pipeline.default_schema.tables
    rows = sum(
        count
        for table, count in trace.last_normalize_info.row_counts.items()
        if not table.startswith("_dlt") and not tables.get(table, {}).get("parent")
    )
    print(
        f"{label}: {rows} rows extracted in {seconds:.1f} s, {rows / seconds:.0f} rows/s,"
        f" {len(http.durations)} requests"
    )
    path = os.environ.get("PERF_RESULTS")
    if path:
        result = {
            # e.g. `tests/jira/test_jira_performance.py::test_performance_baseline (call)`
            "test": os.environ.get("PYTEST_CURRENT_TEST", label).split(" ")[0],
            "label": label,
            "rows": rows,
            "extract_seconds": seconds,
            "requests": len(http.durations),
            "request_seconds": sum(http.durations),
            "throttled": len(http.throttled),
            "connection_errors": sum(http.errors.values()),
        }
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(result) + "\n")


@contextmanager
def account_lock(account: str) -> Iterator[None]:
    """Fails when a seed or a performance test of `account` runs in another process, they would
    slow each other down
    """
    path = os.path.join(tempfile.gettempdir(), f"dlt-perf-{account}.lock")
    for _ in range(2):
        try:
            fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            break
        except FileExistsError:
            with open(path, encoding="utf-8") as f:
                owner = f.read().strip()
            if _is_running(owner):
                raise RuntimeError(
                    f"Process {owner} uses `{account}`, wait for it to finish. Lock: {path}"
                )
            os.remove(path)
    else:
        raise RuntimeError(f"Cannot lock `{account}` with {path}")
    os.write(fd, str(os.getpid()).encode())
    os.close(fd)
    try:
        yield
    finally:
        os.remove(path)


def _is_running(pid: str) -> bool:
    # only POSIX tells if a process exists without side effects, elsewhere delete stale locks by hand
    if not pid.isdigit() or os.name != "posix":
        return True
    try:
        os.kill(int(pid), 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        pass
    return True

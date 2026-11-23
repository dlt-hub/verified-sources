"""Runs a benchmark case once and measures wall time, CPU time and the HTTP requests"""

import os
import shutil
import statistics
import tempfile
import threading
from time import monotonic, process_time, thread_time
from typing import Any, Dict, List, Literal, Optional, Tuple

import dlt

from tests._benchmark.cases import BenchmarkCase, TLoad
from tests._benchmark.machine import connect_rtt_ms
from tests.perf_utils import extracted_rows
from tests.seed_utils import MB, HttpLog, account_lock, record_http

TMode = Literal["iterate", "extract"]
MODES: Tuple[TMode, ...] = ("iterate", "extract")
SAMPLE_INTERVAL = 0.5
PEAK_WINDOW = 5.0
# a resource busy for this share of the wall time is the bottleneck
BOUND_SHARE = 0.85
# bodies read slower than this while in flight are paced by the server, not by the network
NETWORK_MB_S = 10.0


def rss_bytes() -> int:
    """Resident memory of the process, 0 where `/proc` is not available"""
    try:
        with open("/proc/self/statm", encoding="ascii") as f:
            return int(f.read().split()[1]) * os.sysconf("SC_PAGE_SIZE")
    except (OSError, ValueError, IndexError):
        return 0


class CpuSampler:
    """Samples the CPU time and resident memory of the process in a background thread"""

    def __init__(self, interval: float = SAMPLE_INTERVAL) -> None:
        self.interval = interval
        self.samples: List[Tuple[float, float, int]] = []
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def __enter__(self) -> "CpuSampler":
        self._sample()
        self._thread.start()
        return self

    def __exit__(self, *_: Any) -> None:
        self._stop.set()
        self._thread.join()
        self._sample()

    def _run(self) -> None:
        while not self._stop.wait(self.interval):
            self._sample()

    def _sample(self) -> None:
        self.samples.append((monotonic(), process_time(), rss_bytes()))

    def peak_utilization(self, window: float = PEAK_WINDOW) -> float:
        """The highest CPU time per wall second over any `window` seconds"""
        peak, start = 0.0, 0
        for end in range(1, len(self.samples)):
            while self.samples[end][0] - self.samples[start + 1][0] >= window:
                start += 1
            wall = self.samples[end][0] - self.samples[start][0]
            if wall > 0:
                peak = max(peak, (self.samples[end][1] - self.samples[start][1]) / wall)
        return peak

    def peak_rss(self) -> int:
        return max((rss for _, _, rss in self.samples), default=0)

    def timeline(self) -> List[Tuple[float, float, float]]:
        """Seconds since the start, CPU seconds since the start and resident MB of each sample"""
        if not self.samples:
            return []
        wall0, cpu0, _ = self.samples[0]
        return [
            (round(wall - wall0, 2), round(cpu - cpu0, 3), round(rss / MB, 1))
            for wall, cpu, rss in self.samples
        ]


def count_rows(item: Any) -> int:
    """Rows in an item of a resource: Arrow tables are whole, lists of rows are flattened"""
    num_rows = getattr(item, "num_rows", None)
    if num_rows is not None:
        return int(num_rows)
    if isinstance(item, list):
        return len(item)
    return 1


class FirstItem:
    """Records when the first item of a resource arrives, e.g. after a Bulk API job is ready"""

    def __init__(self) -> None:
        self.at: Optional[float] = None

    def __call__(self, item: Any) -> Any:
        if self.at is None:
            self.at = monotonic()
        return item


def iterate(case: BenchmarkCase, load: TLoad, first: FirstItem) -> int:
    rows = 0
    for item in case.resource(load).add_map(first):
        rows += count_rows(item)
    return rows


def extract(case: BenchmarkCase, load: TLoad, round_no: int, first: FirstItem) -> int:
    pipelines_dir = tempfile.mkdtemp(prefix="dlt-benchmark-")
    pipeline = dlt.pipeline(
        pipeline_name=f"benchmark_{case.name}_{load}_{round_no}",
        pipelines_dir=pipelines_dir,
        dev_mode=True,
    )
    try:
        return extracted_rows(pipeline.extract(case.resource(load).add_map(first)))
    finally:
        # an active pipeline gives its incremental state to resources iterated later
        pipeline.deactivate()
        shutil.rmtree(pipelines_dir, ignore_errors=True)


def http_stats(http: HttpLog, wall: float) -> Dict[str, Any]:
    requests = http.requests
    request_s = sum(timing.finished - timing.started for timing in requests)
    to_headers_s = sum(timing.to_headers for timing in requests)
    to_headers = sorted(timing.to_headers for timing in requests) or [0.0]
    busy_s = http.busy_seconds()
    wire_bytes = http.wire_bytes()
    return {
        "requests": len(requests),
        "redirects": http.redirects,
        "statuses": {
            str(status): count for status, count in sorted(http.statuses.items())
        },
        "throttled": len(http.throttled),
        "connection_errors": sum(http.errors.values()),
        "body_mb": http.body_bytes() / MB,
        "wire_mb": None if wire_bytes is None else wire_bytes / MB,
        "request_s": request_s,
        "to_headers_s": to_headers_s,
        "transfer_s": request_s - to_headers_s,
        "to_headers_p50_s": statistics.median(to_headers),
        "to_headers_p95_s": to_headers[
            min(len(to_headers) - 1, int(0.95 * len(to_headers)))
        ],
        "http_cpu_s": sum(timing.cpu for timing in requests),
        "busy_s": busy_s,
        "busy_share": busy_s / wall if wall else 0.0,
        "download_mb_s": http.body_bytes() / MB / busy_s if busy_s else 0.0,
        "peak_concurrency": http.peak_concurrency(),
    }


def bound(result: Dict[str, Any]) -> str:
    """What limited the run: the Python process CPU, the server, the network, or waits outside of
    requests like job polling and backoff
    """
    if result["cpu_util"] >= BOUND_SHARE:
        return "cpu"
    if result["busy_share"] >= BOUND_SHARE:
        # streaming APIs send the headers first and compute the body while sending it
        if (
            result["transfer_s"] > result["to_headers_s"]
            and result["download_mb_s"] >= NETWORK_MB_S
        ):
            return "network"
        return "server"
    return "waits"


def measure(
    case: BenchmarkCase, mode: TMode, load: TLoad, round_no: int
) -> Dict[str, Any]:
    """Runs `case` once in `mode` and `load`. Login and describe calls of the source are measured
    too
    """
    with account_lock(case.account):
        case.prepare()
        with record_http(f"{case.name} {load} {mode}", print_summary=False) as http:
            with CpuSampler() as sampler:
                started, cpu_started, main_cpu_started = (
                    monotonic(),
                    process_time(),
                    thread_time(),
                )
                first = FirstItem()
                if mode == "iterate":
                    rows = iterate(case, load, first)
                else:
                    rows = extract(case, load, round_no, first)
                wall = monotonic() - started
                cpu = process_time() - cpu_started
                main_cpu = thread_time() - main_cpu_started
    expected_rows = (
        case.expected_rows() if callable(case.expected_rows) else case.expected_rows
    )
    result: Dict[str, Any] = {
        "case": case.name,
        "mode": mode,
        "load": load,
        "round": round_no,
        "rows": rows,
        "expected_rows": expected_rows,
        "valid": rows == expected_rows,
        "wall_s": wall,
        "rows_per_min": rows / wall * 60,
        "first_item_s": None if first.at is None else first.at - started,
        "cpu_s": cpu,
        "cpu_util": cpu / wall,
        "main_thread_cpu_s": main_cpu,
        "main_thread_util": main_cpu / wall,
        "peak_cpu_util": sampler.peak_utilization(),
        "peak_rss_mb": sampler.peak_rss() / MB,
        **http_stats(http, wall),
    }
    result["mb_per_min"] = result["body_mb"] / wall * 60
    # the host that served most requests, measured after the run
    hosts = http.hosts.most_common(1)
    result["rtt_ms"] = connect_rtt_ms(hosts[0][0]) if hosts else None
    result["bound"] = bound(result)
    result["timeline"] = sampler.timeline()
    return result

"""Markdown summary of benchmark results: medians over the rounds of each case and mode"""

import importlib
import os
import platform
import statistics
import subprocess
from datetime import datetime, timezone  # noqa: I251
from typing import Any, Dict, List, Sequence, Tuple

import dlt

from tests._benchmark.machine import location, machine_stats

COLUMNS = (
    "case",
    "load",
    "mode",
    "runs",
    "rows",
    "wall s",
    "first item s",
    "rows/min",
    "MB/min",
    "download MB/s",
    "CPU %",
    "main thread %",
    "peak CPU %",
    "in flight %",
    "peak concurrency",
    "requests",
    "RTT ms",
    "headers p50 s",
    "transfer %",
    "throttled",
    "bound",
)


def run_metadata(sources: Sequence[str]) -> Dict[str, Any]:
    """Versions, machine and commit of a benchmark run"""
    try:
        commit = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        commit = os.environ.get("GITHUB_SHA", "")[:7]
    return {
        "started_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "dlt": dlt.__version__,
        "sources": {
            name: importlib.import_module(f"sources.{name}.version").__version__
            for name in sources
        },
        "python": platform.python_version(),
        "commit": commit,
        **machine_stats(),
        "location": location(),
    }


def median(results: List[Dict[str, Any]], key: str) -> float:
    return float(statistics.median(result[key] for result in results))


def summary_row(
    case: str, load: str, mode: str, results: List[Dict[str, Any]]
) -> List[str]:
    walls = [result["wall_s"] for result in results]
    request_s = median(results, "request_s")
    transfer = median(results, "transfer_s") / request_s if request_s else 0.0
    bounds = sorted({result["bound"] for result in results})
    rtts = [result["rtt_ms"] for result in results if result.get("rtt_ms") is not None]
    rtt = f"{statistics.median(rtts):.0f}" if rtts else "n/a"
    return [
        case,
        load,
        mode,
        str(len(results)),
        f"{median(results, 'rows'):.0f}",
        f"{statistics.median(walls):.1f} ({min(walls):.1f}-{max(walls):.1f})",
        f"{median(results, 'first_item_s'):.1f}",
        f"{median(results, 'rows_per_min'):,.0f}",
        f"{median(results, 'mb_per_min'):.1f}",
        f"{median(results, 'download_mb_s'):.1f}",
        f"{median(results, 'cpu_util'):.0%}",
        f"{median(results, 'main_thread_util'):.0%}",
        f"{median(results, 'peak_cpu_util'):.0%}",
        f"{median(results, 'busy_share'):.0%}",
        f"{median(results, 'peak_concurrency'):.0f}",
        f"{median(results, 'requests'):.0f}",
        rtt,
        f"{median(results, 'to_headers_p50_s'):.2f}",
        f"{transfer:.0%}",
        f"{sum(result['throttled'] for result in results)}",
        "/".join(bounds),
    ]


def render_report(results: List[Dict[str, Any]], metadata: Dict[str, Any]) -> str:
    groups: Dict[Tuple[str, str, str], List[Dict[str, Any]]] = {}
    failed: List[Dict[str, Any]] = []
    for result in results:
        if result.get("error") or not result.get("valid"):
            failed.append(result)
        else:
            groups.setdefault(
                (result["case"], result.get("load", "incremental"), result["mode"]), []
            ).append(result)
    versions = ", ".join(
        f"{name} {version}" for name, version in metadata["sources"].items()
    )
    place = metadata["location"]
    where = ", ".join(
        str(place[key])
        for key in ("city", "region", "country", "org")
        if place.get(key)
    )
    azure = " ".join(
        str(place[key]) for key in ("azure_region", "vm_size") if place.get(key)
    )
    image = f", image {metadata['runner_image']}" if metadata["runner_image"] else ""
    lines = [
        "## Extraction benchmark",
        "",
        f"{metadata['started_at']}, commit {metadata['commit']}, dlt {metadata['dlt']},"
        f" Python {metadata['python']}. Sources: {versions}.",
        "",
        f"Machine: {metadata['machine']}, {metadata['cpu_model']},"
        f" {metadata['cpus_available']} of {metadata['cpus']} CPUs,"
        f" {metadata['memory_gb']} GB, {metadata['os']}{image}",
        "",
        f"Location: {where or 'unknown'}" + (f" (Azure {azure})" if azure else ""),
        "",
        "Medians over rounds, wall time with min-max. RTT is the fastest TCP connect to the host"
        " that served most requests.",
        "",
        "| " + " | ".join(COLUMNS) + " |",
        "|" + "---|" * len(COLUMNS),
    ]
    for (case, load, mode), group in sorted(groups.items()):
        lines.append("| " + " | ".join(summary_row(case, load, mode, group)) + " |")
    if failed:
        lines += ["", "Failed or invalid runs:", ""]
        for result in failed:
            reason = result.get("error") or (
                f"{result['rows']} rows, expected {result['expected_rows']}"
            )
            lines.append(
                f"- {result['case']} {result.get('load', '')} {result['mode']}"
                f" round {result['round']}: {reason}"
            )
    return "\n".join(lines) + "\n"

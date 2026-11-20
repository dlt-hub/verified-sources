"""Hardware, location and network latency of the machine that runs the benchmark"""

import json
import os
import platform
import socket
from time import perf_counter
from typing import Any, Dict, Optional
from urllib.request import Request, urlopen

# instance metadata of Azure VMs, GitHub hosted runners are Azure VMs
AZURE_METADATA_URL = "http://169.254.169.254/metadata/instance/compute?api-version=2021-02-01&format=json"
IP_LOCATION_URL = "https://ipinfo.io/json"
LOOKUP_TIMEOUT = 2.0


def read_proc(path: str, key: str) -> Optional[str]:
    """Value of the first `key: value` line of a `/proc` file"""
    try:
        with open(path, encoding="ascii", errors="replace") as f:
            for line in f:
                name, _, value = line.partition(":")
                if name.strip() == key:
                    return value.strip()
    except OSError:
        pass
    return None


def machine_stats() -> Dict[str, Any]:
    memory_kb = read_proc("/proc/meminfo", "MemTotal")
    try:
        cpus_available = len(os.sched_getaffinity(0))
    except AttributeError:
        cpus_available = os.cpu_count()
    image = " ".join(
        filter(None, (os.environ.get("ImageOS"), os.environ.get("ImageVersion")))
    )
    return {
        "cpu_model": read_proc("/proc/cpuinfo", "model name") or platform.processor(),
        "cpus": os.cpu_count(),
        "cpus_available": cpus_available,
        "memory_gb": round(int(memory_kb.split()[0]) / 1024**2, 1)
        if memory_kb
        else None,
        "os": platform.platform(),
        "machine": os.environ.get("RUNNER_NAME") or platform.node(),
        "runner_image": image or None,
    }


def fetch_json(request: Request) -> Optional[Dict[str, Any]]:
    try:
        with urlopen(request, timeout=LOOKUP_TIMEOUT) as response:
            return json.loads(response.read())  # type: ignore[no-any-return]
    except Exception:
        return None


def location() -> Dict[str, Any]:
    """Azure region and VM size of the machine. In CI also its city, looked up by public IP, which
    is never done on a developer machine
    """
    result: Dict[str, Any] = {}
    azure = fetch_json(Request(AZURE_METADATA_URL, headers={"Metadata": "true"}))
    if azure:
        result["azure_region"] = azure.get("location")
        result["vm_size"] = azure.get("vmSize")
    if os.environ.get("GITHUB_ACTIONS") == "true":
        ip_location = fetch_json(Request(IP_LOCATION_URL))
        if ip_location:
            for key in ("city", "region", "country", "org"):
                result[key] = ip_location.get(key)
    return result


def connect_rtt_ms(host: str, port: int = 443, attempts: int = 3) -> Optional[float]:
    """Fastest TCP connect to `host` in ms, about one network round trip"""
    best: Optional[float] = None
    for _ in range(attempts):
        try:
            started = perf_counter()
            with socket.create_connection((host, port), timeout=5):
                elapsed = (perf_counter() - started) * 1000
        except OSError:
            continue
        best = elapsed if best is None else min(best, elapsed)
    return best

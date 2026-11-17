"""Benchmarks the raw extraction of a large seeded resource of each source, see README.md

    uv run python -m tests._benchmark.run [--cases salesforce,servicenow] [--loads full] [--rounds 3]
"""

import argparse
import json
import os
import traceback
from time import sleep
from typing import Any, Dict, List, Optional

# resolve configuration from sources/.dlt like the test harness
os.environ.setdefault("DLT_PROJECT_DIR", "sources")

# pyarrow is imported on first use, the first run must not pay for it
from dlt.common.libs import pyarrow  # noqa: F401

from tests._benchmark.cases import CASES, LOADS
from tests._benchmark.measure import MODES, measure
from tests._benchmark.report import render_report, run_metadata


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cases", default=",".join(CASES))
    parser.add_argument("--modes", default="iterate")
    parser.add_argument("--loads", default=",".join(LOADS))
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument(
        "--pause", type=float, default=10.0, help="seconds between two runs"
    )
    parser.add_argument("--output", default="_storage/benchmark/results.jsonl")
    args = parser.parse_args(argv)
    modes = args.modes.split(",")
    unknown = set(modes) - set(MODES)
    if unknown:
        parser.error(f"unknown modes {sorted(unknown)}, pick from {MODES}")
    loads = args.loads.split(",")
    unknown = set(loads) - set(LOADS)
    if unknown:
        parser.error(f"unknown loads {sorted(unknown)}, pick from {LOADS}")
    variants = [(load, mode) for load in loads for mode in modes]
    cases = []
    for name in args.cases.split(","):
        if name not in CASES:
            parser.error(f"unknown case `{name}`, pick from {list(CASES)}")
        if CASES[name].is_configured():
            cases.append(CASES[name])
        else:
            print(f"Skipping {name}: its credentials are not configured, see README.md")
    metadata = run_metadata(sorted({case.source for case in cases}))
    os.makedirs(os.path.dirname(os.path.abspath(args.output)), exist_ok=True)

    results: List[Dict[str, Any]] = []
    with open(args.output, "a", encoding="utf-8") as output:
        for round_no in range(1, args.rounds + 1):
            # runs of a case are spread over the benchmark and alternate their order
            for case in cases:
                for load, mode in variants if round_no % 2 else variants[::-1]:
                    if results:
                        sleep(args.pause)
                    print(f"Round {round_no}: {case.name} {load} {mode}", flush=True)
                    try:
                        result = measure(case, mode, load, round_no)
                    except Exception as ex:
                        traceback.print_exc()
                        result = {
                            "case": case.name,
                            "mode": mode,
                            "load": load,
                            "round": round_no,
                            "error": f"{type(ex).__name__}: {ex}",
                        }
                    result["metadata"] = metadata
                    output.write(json.dumps(result) + "\n")
                    output.flush()
                    results.append(result)
                    if "error" not in result:
                        print(
                            f"  {result['rows']} rows in {result['wall_s']:.1f} s, CPU"
                            f" {result['cpu_util']:.0%}, in flight {result['busy_share']:.0%},"
                            f" {result['requests']} requests, bound: {result['bound']}",
                            flush=True,
                        )

    report = render_report(results, metadata)
    print(report)
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a", encoding="utf-8") as f:
            f.write(report)
    return int(any(result.get("error") or not result["valid"] for result in results))


if __name__ == "__main__":
    raise SystemExit(main())

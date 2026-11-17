# Extraction benchmark

Measures how fast the sources read their large seeded performance data, without normalize and load,
and what limits them: the CPU of the Python process, the server, the transfer of the response
bodies, or waits outside of requests.

## Cases

Each case reads one resource with default settings, like the `test_performance_baseline` of its
source. `salesforce_rest` reads the same object with the REST API, which has no job to wait for:

| Case | Resource | Rows | Seed |
|---|---|---|---|
| `salesforce` | `salesforce_table(sobject="DltPerfA__c")`, Bulk API 2.0 | 50,000 | `tests/salesforce/README.md`, performance org |
| `salesforce_rest` | `salesforce_table(sobject="DltPerfA__c", api="rest")` | 50,000 | same |
| `servicenow` | `servicenow_table(table="u_dlt_perf_a")` | 50,000 | `tests/servicenow/README.md` |
| `jira` | `jira_issues(jql=perf_jql("DLTPA"))` | 20,000 | `tests/jira/README.md` |

Each case runs in two modes:

- `iterate`: iterates the resource and drops the items. The extract workers still read pages and
  partitions in parallel.
- `extract`: `pipeline.extract` into a temporary pipelines dir, which also writes the items to
  files.

The time includes creating the resource, so logins and describe calls count.

## Running

```sh
uv run python -m tests._benchmark.run                      # all configured cases, 3 rounds
uv run python -m tests._benchmark.run --cases jira --rounds 1 --modes iterate
```

Rounds go over all cases, so the runs of one case are minutes apart, and the order of the modes
alternates between rounds. Run it in CI: on a flaky network the timings say more about the network
than about the sources. Cases without credentials are skipped. Results are appended to
`_storage/benchmark/results.jsonl` (`--output`). The markdown summary is printed and appended to
`$GITHUB_STEP_SUMMARY` when set. The exit code is 1 when a run failed or read a different number
of rows than seeded.

The Salesforce case reads `[sources.salesforce_performance.credentials]`. Locally you can use the
session of the sf CLI:

```sh
export SOURCES__SALESFORCE_PERFORMANCE__CREDENTIALS__SESSION_ID=$(sf org auth show-access-token -o dlt-perf --json | jq -r .result.accessToken)
export SOURCES__SALESFORCE_PERFORMANCE__CREDENTIALS__INSTANCE_URL=https://connect-app-6623-dev-ed.scratch.my.salesforce.com
```

In CI, the `benchmark | extraction` workflow runs on `workflow_dispatch` and on pull requests
labeled `benchmark`, also for new commits on such a pull request. It uploads the JSON lines as the `benchmark-results` artifact.

`pytest.ini` keeps folders starting with `_` out of test discovery and the CI test jobs skip
`tests/_*`, so the benchmark runs only in its own workflow. Its unit tests run with
`uv run pytest tests/_benchmark`.

## Machine and location

The report header and the `metadata` of each JSON line hold:
- the CPU model, the CPUs available to the process, memory, OS and the runner image
- the Azure region and VM size from the instance metadata service: GitHub hosted runners are
  Azure VMs
- in CI only, the city, region, country and network of the runner's public IP, from ipinfo.io

Each run also records `RTT ms`: the fastest of 3 TCP connects to the host that served most
requests. It is about one network round trip from the runner to the upstream. The host itself is not
recorded, the job summary of the public repository would show it.

## Metrics

| Column | Meaning |
|---|---|
| `wall s` | median wall time over the rounds, with min and max |
| `first item s` | time until the first rows arrive: logins, describe calls, a Bulk API job or the first page |
| `rows/min`, `MB/min` | rows and decoded response bodies per minute of wall time |
| `download MB/s` | decoded response bodies per second while requests are in flight |
| `CPU %` | CPU time of the process, all threads, per wall second. 100% is one core |
| `main thread %` | CPU time of the main thread, which runs the pipe steps, e.g. the incremental |
| `peak CPU %` | the highest CPU % over 5 s, samples every 0.5 s |
| `in flight %` | share of the wall time with at least one request in flight |
| `peak concurrency` | most requests in flight at once |
| `headers p50 s` | median time to the response headers: server time and latency |
| `transfer %` | share of request time spent reading bodies after the headers |
| `throttled` | responses with 429 or 503, runs with them are not comparable |

The JSON lines also hold:
- the bytes received on the wire, compressed when the server sent gzip. They are `null` when a
  compressed body came in chunks, which urllib3 does not count.
- the CPU time of the threads inside requests (TLS, socket reads, gzip)
- peak memory and statuses
- a timeline of CPU and memory samples

`bound` names what limited the run:

- `cpu`: process CPU % is 85% or more. Python threads share one interpreter lock, so a run near
  100% is limited by Python code even with free cores.
- `server`: requests are in flight 85% or more of the time, and the server paces them. Either
  waiting for headers takes longer than reading bodies, or the bodies come slower than 10 MB/s.
  Streaming APIs send headers first and compute the body while sending it.
- `network`: requests are in flight 85% or more of the time and bodies come at 10 MB/s or faster.
- `waits`: neither. The source waits outside of requests, e.g. on retry backoff, on the main thread,
  or between the status checks of a Bulk API job that Salesforce prepares. A long `first item s`
  points to the job.

## Adding a case

Add a `BenchmarkCase` to `CASES` in `cases.py`: a factory of the resource, the expected rows, the
`account_lock` key shared with the seeds and performance tests of the account, and a check that its
credentials resolve.

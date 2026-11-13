# Jira tests

`test_issue_comments_changed` writes to the live site: it adds, edits and deletes its own comments
on the issue labeled `dlt-seed-comment-changes`. `test_jira_performance.py` reads the
[performance data](#performance-data), its tests are marked `performance` and `pytest.ini` deselects them.

## Seeding

Run from `sources/` so the script reads credentials from `sources/.dlt`:

1. `uv run python ../tests/jira/seed_test_data.py` lists the missing cases.
2. `uv run python ../tests/jira/seed_test_data.py --apply` creates them. `bulk_issues` adds the
   missing issues when `BULK_ISSUES` grows, other cases are created only when no issue has their
   label.
3. `uv run python ../tests/jira/seed_test_data.py --capture` rewrites
   `tests/jira/captured_responses.json`. Run it again after seeding or editing seeded issues.

The test account cannot delete issues and cannot restrict comments to a group or role, so `--delete`
fails and restricted comments are tested offline only.

## Performance data

The projects `DLTPA` and `DLTPB` hold 20,000 issues each, labeled `dlt-perf`, so the tests of the seeded cases do
not see them. `[dummy] Number` (`customfield_10047`) numbers them from 0 and `[dummy] Date time`
(`customfield_10050`) has unique minutes over two years, in an order unrelated to the number. The values come from
`perf_issue` in `utils.py`, which keeps them out of the hour that the end of DST repeats in `Europe/Zurich`, the
time zone of the account. Seed them from `sources/`:

```sh
uv run python ../tests/jira/seed_test_data.py --performance            # shows the missing issues
uv run python ../tests/jira/seed_test_data.py --performance --apply    # creates them, then verifies
uv run python ../tests/jira/seed_test_data.py --performance --verify
```

`--apply` creates the projects with the issue type screen scheme of `TST`, which has both custom fields, and
repairs duplicates and cursors. `--verify` compares every issue with `perf_issue`. JQL compares whole minutes, so
in the chunked incremental test each chunk after the first loads one issue less than `chunk_size`.

```sh
ALL_DESTINATIONS='["duckdb"]' uv run pytest -m performance -s tests/jira/test_jira_performance.py
```

Performance tests and `--performance` seeds print a summary of their HTTP requests and warn when the API
throttled any, the timings of such a run are not comparable. They lock the account in the temp directory, so a
second run on the same machine fails at once. Set `PERF_RESULTS` to an absolute path to append the results as JSON
lines.

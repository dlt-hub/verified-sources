# Jira tests

`test_issue_comments_changed` writes to the live site: it adds, edits and deletes its own comments
on the issue labeled `dlt-seed-comment-changes`.

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

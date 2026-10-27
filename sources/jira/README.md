# Jira

Maintenance notes of the [Jira Cloud](https://www.atlassian.com/software/jira) source for developers
and coding agents. Setup and usage are in the
[dlt docs](https://dlthub.com/docs/dlt-ecosystem/verified-sources/jira).

## Changelog

### 1.0.0
- **breaking** `issues`: loaded incrementally on `updated` with a 10 minute lag and merged on `id`
  instead of replaced. Upgrade: run once with `refresh="drop_resources"`. If a run already failed
  on load with an error about `_dlt_root_id` (`No root_key column` or a missing column), run
  `dlt pipeline <name> abort-packages` first
- **breaking** `issues`: fields are top level columns named by the field id (`fields__summary` is
  `summary`), nested tables are `issues__<field>`, rich text is wiki markup (API v2), no
  `changelog`, `operations`, `transitions`, `comment` and `worklog` data
- **breaking** `jira_search`: issues load like in `jira_issues` with a `query_index` column, the
  primary key is `id` and `query_index`, an issue found by two queries loads twice.
  `jql_queries` and `page_limit` are source arguments. Upgrade: run once with
  `refresh="drop_resources"`
- **breaking** `workflows`: read from `rest/api/3/workflows/search`, `id` is text and the primary
  key, `id__name`, `id__entity_id`, `has_draft_workflow`, `is_default`, `projects` and `schemes` are
  gone. Upgrade: run once with `refresh="drop_resources"` to drop the empty columns
- new resources `issue_comments`, `issue_changelogs`, `worklogs` and `workflow_statuses`
- new `jira_issues`: issues matching JQL with any incremental
- changed `users`, `workflows`, `projects`: primary keys, the largest page each endpoint serves
- changed: `get_paginated_data` is removed, `JiraClient.paginate_endpoint` reads the same endpoints

Versions before 1.0.0 were not numbered. Copies taken with `dlt init` have no `version.py`,
`.dlt/.sources` records the repository commit they came from.

## Extension points

- A replaced endpoint: an entry in `settings.DEFAULT_ENDPOINTS` with its key and page size.
- A field type: its shape in `SYSTEM_FIELD_TYPES` or `CUSTOM_FIELD_TYPES`. Unknown types load as
  text.
- A transformer on issues: pipe it from `issues`, it receives pages of issue rows and the
  `IssuesReader` in the `IssuesPage` meta, like `issue_comments` and `issue_changelogs`.
- A new API: a typed call on `JiraClient`, with a paginator from `paginators.py` or dlt.

## Seeding

Seeded issues are labeled `dlt-seed-<case>`, a case is skipped when its label exists. Run from
`sources/` so the script reads credentials from `sources/.dlt`:

1. `uv run python ../tests/jira/seed_test_data.py` lists the missing cases.
2. `uv run python ../tests/jira/seed_test_data.py --apply` creates them.
3. `uv run python ../tests/jira/seed_test_data.py --capture` rewrites
   `tests/jira/captured_responses.json` with anonymized responses for the unit tests and the
   manifest of seeded issues (`seed`: ids, labels, `created`, `updated`) from which the
   end-to-end tests predict the counts of full, windowed and incremental loads. Run it again
   after seeding or editing seeded issues.

The test account cannot delete issues and cannot restrict comments to a group or role, so `--delete`
fails and restricted comments are tested offline only.

## Tests

- `JiraMock` in `tests/jira/utils.py` serves search, bulk fetch, changelogs, comments and worklog
  feeds from in-memory issues.
- `test_issue_comments_changed` writes to the live site: it adds, edits and deletes its own comments
  on the issue labeled `dlt-seed-comment-changes`.

## Operations

- **Page and request sizes**: resources yield pages of `chunk_size` rows (1000 by default) of
  issues, comments and changelogs, independent of the size of API requests. Requests take what the
  API serves: search lists up to 5000 ids, bulk fetch takes 1000 issues for at most 100 single
  value fields and 100 otherwise, changelogs are fetched for 1000 issues and worklogs by 1000 ids.
  Pages of issues are fetched in parallel by the dlt extract `workers` (5 by default), each page in
  sequential requests, so runs with fewer pages than workers fetch with less parallelism.
- **Comments**: `issue_comments` bulk fetches the `comment` field of each page of issues, 100
  issues per request, which embeds the latest 20 comments of an issue. Issues with more are read
  again from `startAt=0` with the comment endpoint, which returns about 100 per page. An issue left
  without comments gets a row flagged `deleted` that deletes its comments loaded before, except on
  the first run. Selecting `issue_comments` alone still runs `issues`, its parent.
- **State**: the incremental state of `issues` is kept by its parent `issues_ids`, which lists the
  ids (`<name>_ids` for `jira_issues(name=...)`). `refresh="drop_resources"` resets it with the
  table, the CLI matches exact names: `dlt pipeline <name> drop issues issues_ids`. Load several
  resources of issues with `jira_issues(name=...)`, not with `with_name`, which shares the state.
- **Large loads**: set `[sources.data_writer] file_max_items` (e.g. 100000) so extract rotates
  files and normalize and load run in parallel. `included_columns` with at most 100 single value
  fields fetches 1000 issues per request instead of 100.
- **Throttling**: 429 and 5xx are retried with the `runtime.request_*` settings of dlt.
- **Incremental**: JQL compares datetimes with minute precision in the time zone of the user
  (`/myself`, UTC if not set). Search is eventually consistent, so the default 10 minute lag reads
  recent issues again and merge absorbs them. `/search/jql` rejects a query without a filtering
  clause, `created >= "1970-01-01"` bounds a full load.
- **Limits**: `add_limit` on a transformer is ignored. Limit the `issues_ids` resource, the parent
  of `issues`, or use `page_limit` of `jira_search`.
- **Worklogs**: the feeds page by the youngest change of a page. More than 1000 worklogs changed in
  the same millisecond stop the load with an error.
- **Workflow statuses**: `workflows__statuses` holds a `status_reference` and no name, join it to
  `workflow_statuses.status_reference`. `rest/api/3/status` omits unused statuses and
  `rest/api/3/statuses/search` omits project scoped ones, so the `statuses` key of
  `workflows/search` is read instead. Selecting both resources walks that endpoint twice. Do not
  follow its `nextPage` link, it points to an internal Atlassian host.

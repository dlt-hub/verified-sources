# Jira

Maintenance notes of the [Jira Cloud](https://www.atlassian.com/software/jira) source for developers
and coding agents. Setup and usage are in the
[dlt docs](https://dlthub.com/docs/dlt-ecosystem/verified-sources/jira).

## Changelog

### 2.0.0
**Upgrade**: replace `jira_search(jql_queries=[...])` with a `jira_issues(jql=..., name=...)` per
query, without ORDER BY, and drop the `jira_search` table. Move `included_columns` and
`excluded_columns` of `jira` from the `[sources.jira.issues]` config section to `[sources.jira]`.
Set `excluded_columns = []` there to keep loading `attachment`, `issuelinks`, `votes` and `watches`:
without it these columns and the `issues__attachment` and `issues__issuelinks` tables stop getting
new values. Remove `incremental=None` from `jira` and `jira_issues` to keep the default incremental.
The incremental state carries over. Requires dlt 1.32 or later.
- **breaking** `jira_search`: removed. Several `jira_issues` load several queries incrementally, add
  a column with `add_map` to load them into one table
- new `jira_issues`: reads its config from the `sources.jira.<name>` section
- **breaking** `issues`: `incremental=None` loads without incremental, the JQL runs as it is and may
  contain ORDER BY. The default incremental is the default value of `incremental`, an incremental
  without `cursor_path` sets its values, e.g. `end_value`. `start_date` wins over its
  `initial_value`
- new `jira`: `included_columns` and `excluded_columns`
- changed `issues`: `attachment`, `issuelinks`, `votes` and `watches` are not loaded by default,
  they cap bulk fetch at 100 issues. Default loads make 8 times fewer requests
- changed `issues`: bulk fetch requests 1000 issues unless one of the fields Jira caps at 100 is
  loaded, also with multi-value fields like `labels`
- changed `issues`: `excluded_columns` applies only when `included_columns` is not set
- fixed `issues`: `add_limit` with `row_order` lost issues. All pages were fetched in parallel and
  the first one fetched could be the last page, moving the cursor past the others

### 1.1.0
**Upgrade**: safe for `jira` and `jira_issues`, the incremental state carries over. `jira_search`
loads into a new `jira_search` table, drop the `issues` table it loaded before.
- **breaking** `jira_search`: loads into `jira_search` instead of `issues`, `with_name` sets another
  table
- new `jira_search`: `chunk_size`, `bulk_fetch_size`, `included_columns` and `excluded_columns`
- fixed `issues`: issues updated during a run are loaded by the next run, and deleted issues no
  longer move the cursor

### 1.0.0
**Upgrade**: run once with `refresh="drop_resources"`, e.g.
`pipeline.run(jira(), refresh="drop_resources")`: it drops the tables loaded before and loads them
again. Without it the load of `issues` fails on `_dlt_root_id` and the columns of earlier versions
stay empty. After such a failed run, run `dlt pipeline <name> abort-packages` first. Earlier
versions kept no incremental state: the first run loads all issues, or those updated since
`start_date`.
- **breaking** `issues`: loaded incrementally and merged on `id` instead of replaced
- **breaking** `issues`: fields are top level columns named by the field id, without `changelog`,
  `operations`, `transitions`, `comment` and `worklog`
- **breaking** `jira_search`: `jql_queries` and `page_limit` are source arguments, the primary key
  is `id` and `query_index`, `page_size` counts ids: set `page_size=100` to keep the old limits
- **breaking** `workflows`: `id` is text and the primary key, several columns stay empty
- **breaking** `get_paginated_data` is removed, use `JiraClient.paginate_endpoint`
- new resources `issue_comments`, `issue_changelogs`, `worklogs`, `workflow_statuses` and
  `issue_fields`
- new `jira_issues`: issues matching JQL
- new `jira` arguments: `jql`, `start_date`, `incremental` and `write_disposition`
- new `drop_keys`: removes links and avatar and icon urls from rows
- changed `users`, `workflows`, `projects`: primary keys

Versions before 1.0.0 were not numbered. Copies taken with `dlt init` have no `version.py`,
`.dlt/.sources` records the repository commit they came from.

## Optimization & Configuration

Learned on the test site, a Free plan site, in the performance tests on two projects of 20,000 issues
(performance tests), and from the Jira Cloud OpenAPI spec (docs).

- **Extract workers** (`workers` in the `[extract]` config section): requests dominate the run time,
  and pages of `chunk_size` issues are fetched in parallel by the extract workers. Throughput grew
  about 4 times from 1 to 5 workers and doubled again from 5 to 20, without throttling (performance
  tests). Queries loaded in one run share the workers, raise them for several `jira_issues` too. A
  run with fewer pages than workers is less parallel.
- **Columns**: `attachment`, `issuelinks`, `votes`, `watches`, `comment`, `worklog` and more than 100
  fields cap a bulk fetch at 100 issues instead of 1000, about 8 times the requests (see Request
  costs). The first four are excluded by default, `excluded_columns=[]` loads them at about three
  quarters of the throughput (performance tests).
- **Child resources**: comments, changelogs and worklogs cost requests of their own (see Request
  costs and Comments), select only the resources you need.
- **Large loads**: set `[sources.data_writer] file_max_items` (e.g. 100000) so extract rotates files
  and normalize and load run in parallel. Without it normalize was several times slower on 1 million
  synthetic rows.
- **Chunked loads**: `add_limit` with `row_order`, and `max_time`, fetch pages one after another.

## Code Customization

- An endpoint: `DEFAULT_ENDPOINTS` in `settings.py`.
- A field type: `helpers/schema_types.py`.
- A transformer on issues: follow `issue_comments` in `__init__.py`.
- A new API: `helpers/client.py`, with a paginator from `helpers/paginators.py`.

## Maintenance & Troubleshooting

Facts seen on the test site, a Free plan site, or taken from the
[Jira Cloud OpenAPI spec](https://dac-static.atlassian.com/cloud/jira/platform/swagger-v3.v3.json)
(docs).

- **Request costs**: issues cost one search request per 5000 ids (docs) and one bulk fetch per 1000
  issues, or per 100 issues when `attachment`, `issuelinks`, `votes`, `watches`, `comment` or
  `worklog` or more than 100 fields are fetched: Jira answers larger requests with HTTP 400 "You may
  only load a maximum of 100 issues" (test site). Changelogs cost one request per 1000 issues and
  worklogs one list request per 1000 ids (docs).
- **Comments**: bulk fetch embeds only the latest 20 comments of an issue, issues with more are
  read again with the comment endpoint, which returns about 100 comments per page even when more
  are requested (test site). Adding, editing and deleting a comment bumps `updated` of its issue,
  so comment changes are loaded with the issue (test site). Selecting only `issue_comments` still
  searches and fetches the issues.
- **Changelogs**: changelog bulk fetch has no date filter (docs), so the whole changelog of each
  issue in the incremental range is fetched again. Changelogs of issues not updated since
  `issue_changelogs` was added are not loaded: reset the state of `issues` to load them.
- **Search**: `/search/jql` is eventually consistent (docs), the lag reads recent issues again on
  the next run. It serves up to 5000 ids per page, but only 100 issues when it also returns a field
  (docs, test site), so the cursor comes from fetched issues. Issues updated after the run starts
  are left to the next run, so a late fetch does not move the cursor past updates search did not
  list. Cursors on custom fields, which may hold future values, and replace loads have no such
  cutoff. It rejects a query without a filtering clause (docs), the test site accepts
  `created >= "1970-01-01"` as a bound. JQL compares datetimes with minute precision in the time
  zone of the user, seen with Europe/Zurich on the test site.
- **Datetime custom fields**: Jira keeps them as wall time in the time zone of the user. When DST
  ends, an hour repeats and Jira stores the first pass: a value written as `01:40Z` on 2024-10-27
  came back as `02:40+0200`, one hour earlier (test site, Europe/Zurich). Two values one hour apart
  in that night load as one, which a cursor on such a field cannot tell apart.
- **Worklogs**: the feeds list worklogs of all issues the user can see, `jql` does not restrict
  them, and the first run reads all of them. The updated feed omits worklogs changed in the minute
  before the request (docs), the lag reads them on the next run. A feed page holds 1000 changes
  and the next page starts at the youngest change of the page (docs), so more than 1000 worklogs
  changed in the same millisecond stop the load with an error, not seen live.
- **Throttling**: no 429 or 5xx seen on the test site. API token traffic has only burst rate
  limits, 100 requests per second in the `RateLimit-Policy` header of the test site, the hourly
  points quota applies to apps
  ([rate limiting](https://developer.atlassian.com/cloud/jira/platform/rate-limiting/)). dlt
  retries 429 and 5xx with the `runtime.request_*` settings.
- **Links and images**: `self` links and `avatarUrls` and `iconUrl` urls are a quarter of the
  columns of `issues` in the responses captured from the test site, `drop_keys` removes them.
- **State**: the incremental state of `issues` is kept by its parent `issues_ids` (`<name>_ids`
  for `jira_issues(name=...)`, which reads config from `sources.jira.<name>`). The CLI matches exact names:
  `dlt pipeline <name> drop issues issues_ids`. `with_name` shares the state of `issues`.
  `apply_hints(write_disposition="replace")` on `issues` does not reach `issues_ids`, so it does
  not reset the state.
- **Limits**: `add_limit` on `issues` limits the search: only the issue ids of the limited pages are
  listed and all of them load, so chunked loads with `row_order` load pages in order. JQL compares
  whole minutes, so with `range_start="open"` a run lists the issues of the last value again and
  drops them, a chunk then holds fewer new issues than `chunk_size` (test site). `max_time` cannot
  limit the search, then pages are fetched one after another.
- **Workflow statuses**: `workflows/search` returns the statuses of a workflow as
  `statusReference`, their names are in its sibling `statuses` list, so selecting `workflows` and
  `workflow_statuses` reads the endpoint twice. `rest/api/3/status` omits unused statuses and
  `rest/api/3/statuses/search` omits project scoped ones (found in #714). Do not follow the
  `nextPage` link of `workflows/search`, it points to an internal Atlassian host (test site).

# Jira

Maintenance notes of the [Jira Cloud](https://www.atlassian.com/software/jira) source for developers
and coding agents. Setup and usage are in the
[dlt docs](https://dlthub.com/docs/dlt-ecosystem/verified-sources/jira).

## Changelog

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

## Extension points

- A replaced endpoint: an entry in `settings.DEFAULT_ENDPOINTS` with its key and page size.
- A field type: its dlt data type, `object` or `array` in `SCHEMA_TYPES`, `SYSTEM_FIELD_TYPES` or
  `CUSTOM_FIELD_TYPES` of `schema_types.py`. Fields of unknown type load as JSON text.
- A transformer on issues: pipe it from `issues`, it receives pages of issue rows and the
  `IssuesReader` in the `IssuesPage` meta, like `issue_comments` and `issue_changelogs`. `jira`
  gives it its `write_disposition`.
- A new API: a typed call on `JiraClient`, with a paginator from `paginators.py` or dlt.

## Maintenance & Troubleshooting

Facts seen on the test site, about 300 issues, or taken from the
[Jira Cloud OpenAPI spec](https://dac-static.atlassian.com/cloud/jira/platform/swagger-v3.v3.json)
(docs).

- **Run time**: requests dominate. On the test site 545 issues load in 3 to 5 s with 9 requests:
  fields, user, one search and 6 bulk fetches. Issues cost one search request per 5000 ids (docs)
  and one bulk fetch per 100 issues, or per 1000 issues when at most 100 fields without multiple
  values are fetched, e.g. with `included_columns` (docs, a larger request was rejected on the
  test site).
  Changelogs cost one request per 1000 issues and worklogs one list request per 1000 ids (docs).
  Pages are fetched in parallel by the dlt extract `workers`, a run with fewer pages than workers
  is less parallel.
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
- **Worklogs**: the feeds list worklogs of all issues the user can see, `jql` does not restrict
  them, and the first run reads all of them. The updated feed omits worklogs changed in the minute
  before the request (docs), the lag reads them on the next run. A feed page holds 1000 changes
  and the next page starts at the youngest change of the page (docs), so more than 1000 worklogs
  changed in the same millisecond stop the load with an error, not seen live.
- **Throttling**: no 429 or 5xx seen on the test site. API token traffic has only burst rate
  limits, the hourly points quota applies to apps
  ([rate limiting](https://developer.atlassian.com/cloud/jira/platform/rate-limiting/)). dlt
  retries 429 and 5xx with the `runtime.request_*` settings.
- **Large loads**: set `[sources.data_writer] file_max_items` (e.g. 100000) so extract rotates
  files and normalize and load run in parallel. Without it normalize was 2.5 to 3 times slower on
  1 million synthetic rows.
- **Links and images**: `self` links and `avatarUrls` and `iconUrl` urls are a quarter of the
  columns of `issues` in the responses captured from the test site, `drop_keys` removes them.
- **State**: the incremental state of `issues` is kept by its parent `issues_ids` (`<name>_ids`
  for `jira_issues(name=...)`). The CLI matches exact names:
  `dlt pipeline <name> drop issues issues_ids`. `with_name` shares the state of `issues`.
  `apply_hints(write_disposition="replace")` on `issues` does not reach `issues_ids`, so it does
  not reset the state.
- **Limits**: `add_limit` on a transformer is ignored. Limit `issues_ids`, the parent of `issues`,
  or use `page_limit` of `jira_search`.
- **Workflow statuses**: `workflows/search` returns the statuses of a workflow as
  `statusReference`, their names are in its sibling `statuses` list, so selecting `workflows` and
  `workflow_statuses` reads the endpoint twice. `rest/api/3/status` omits unused statuses and
  `rest/api/3/statuses/search` omits project scoped ones (found in #714). Do not follow the
  `nextPage` link of `workflows/search`, it points to an internal Atlassian host (test site).

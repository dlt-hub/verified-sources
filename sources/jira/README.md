# Jira

Loads issues, their comments, changelogs and worklogs, users, workflows and projects from a
[Jira Cloud](https://www.atlassian.com/software/jira) site. This file is for developers and coding
agents that change the source. Setup and usage are in the
[dlt docs](https://dlthub.com/docs/dlt-ecosystem/verified-sources/jira).

| Resource          | Loads                                                                | Disposition |
| ----------------- | -------------------------------------------------------------------- | ----------- |
| issues            | issues matching JQL, a column per field                              | merge on `id`, incremental on `updated` |
| issue_comments    | all comments of the loaded issues, piped from `issues`               | merge, merge key `issueId` |
| issue_changelogs  | all changes of the loaded issues, piped from `issues`                | merge on `id` |
| worklogs          | worklogs changed since the last run, deleted worklogs are removed    | merge on `id`, incremental on `updated` |
| users             | all users on the site, including inactive and deleted accounts       | replace |
| workflows         | workflows, `workflows__statuses` holds status references             | replace |
| workflow_statuses | names and categories of the statuses the workflows reference         | replace |
| projects          | projects                                                             | replace |

`jira()` composes them. `jira_issues` loads issues matching one JQL query with any incremental, and
`jira_search` loads issues matching several queries, replaced on each run.

## Changelog

### 1.0.0

First versioned release.

- **Breaking**: `issues` are read incrementally on `updated` with a 10 minute lag and merged on
  `id`, instead of replaced on each run. Fields are top level columns named by the field id
  (`fields__summary` is now `summary`), nested tables are renamed (`issues__fields__labels` is now
  `issues__labels`), the `changelog`, `operations` and `transitions` tables are gone, rich text is
  wiki markup (API v2) instead of Atlassian Document Format, comments and worklogs are not columns.
  A dataset loaded before fails on merge with `No root_key column (e.g. _dlt_root_id)` and the
  failed package stays pending. To upgrade, abort it with
  `dlt pipeline <pipeline_name> abort-packages` and run once with
  `pipeline.run(source, refresh="drop_resources")`.
- **Breaking**: `jira_search` loads issues like `jira_issues`, with the columns above. Each row
  has the `query_index` of the query that found it and the primary key is `id` and `query_index`:
  an issue found by two queries is loaded twice instead of once. Queries and `page_limit` are
  arguments of `jira_search`, `jira_search().issues.bind(jql_queries=...)` no longer works. Load
  into a fresh dataset or run once with `refresh="drop_resources"`.
- **Breaking**: `workflows` reads `rest/api/3/workflows/search`, which replaces
  `rest/api/3/workflow/search`, removed on June 1, 2026. Its `id` was an object and arrived as
  `id__name` and `id__entity_id`, it is now a text `id` and the primary key. `name`,
  `is_editable`, `scope` and `version` are new. `has_draft_workflow`, `is_default`, `projects` and
  `schemes` are gone. The load succeeds on an existing dataset but the old columns stay empty, run
  once with `refresh="drop_resources"` to drop them.
- New resources: `issue_comments` fetched with the issues, `issue_changelogs`, `worklogs` and
  `workflow_statuses`. Users, workflows and projects have primary keys and request the largest
  page each endpoint serves.
- `jira_issues` loads issues matching JQL with a custom incremental, as dicts or Arrow tables.

Versions before 1.0.0 were not numbered. A copy of the source taken with `dlt init` before it has
no `version.py`, `.dlt/.sources` records the repository commit it came from. All its breaking
changes are listed in 1.0.0.

## Architecture

All endpoints are REST APIs called by one client. Layers, bottom up, in `helpers/`:

- `paginators.py`: `JiraCursorPaginator` (`nextPageToken`, stops on `isLast`),
  `JiraOffsetPaginator` (`startAt`, advances by the rows a page held because Jira clamps
  `maxResults` silently), `WorklogFeedPaginator` (`since`/`until` feeds), `JiraResponseError` and
  the response hook that raises it with Jira's `errorMessages`.
- `client.py`: `JiraClient` on dlt `RESTClient` with basic auth. Each thread gets its own
  `RESTClient` on a thread local session of the dlt requests `Client`, which retries 429 and 5xx.
  Typed calls: `fields`, `time_zone`, `search`, `bulk_fetch` (splits into 100s after a 400),
  `changelogs`, `comments`, `changed_worklogs`, `worklogs`, and `paginate` for anything else.
- `jql.py`: `CursorRange` from an incremental and `make_jql`, which widens the range to whole
  minutes in the time zone of the user. The incremental filters the exact range.
- `schema_types.py`: maps `/field` metadata to dlt types and nested shapes. Shapes trim nested
  values to useful keys. The python backend flattens them into columns and nested tables, the
  pyarrow backend loads them as json or as nested types (`x-nested-type`).
- `issues.py`: `IssuesReader` selects fields, plans what to fetch and converts issues to rows or
  Arrow tables. `issue_ids` (holds the incremental) lists ids with search,
  `issues` (parallelized transformer) fetches pages with bulk fetch. `make_issues_resource` wires
  both, `jira_issues` and `jira_search` use it.
- `comments.py`, `worklogs.py`: conversion of comments and worklogs, `endpoints.py`: users,
  workflows, workflow statuses and projects from `settings.DEFAULT_ENDPOINTS`.

Issues are fetched once for all consumers. `plan_fetch` looks at the selected resources when
`issue_ids` starts: issue fields are fetched when `issues` are loaded or consumed by an unknown
transformer, the `comment` field when `issue_comments` consumes them, nothing for
`issue_changelogs` alone. Comments reach `issue_comments` in the meta of each page (`IssuesPage`),
so they never land in the `issues` table.

Extension points:
- A replaced endpoint: add an entry to `DEFAULT_ENDPOINTS` with its key and page size.
- A field type: add its shape to `SYSTEM_FIELD_TYPES` or `CUSTOM_FIELD_TYPES`. Unknown types load as
  text.
- A transformer on issues: pipe it from `issues`, it receives issue pages and `IssuesPage` meta.
  Name it in `plan_fetch` if it needs less than all fields.
- A new API: a typed call on `JiraClient`, with a paginator from `paginators.py` or dlt.

## Tests and seeding

- `tests/jira/test_helpers.py` and `tests/jira/test_jira_issues.py` run without credentials. They
  mock the REST API with `JiraMock` (`tests/jira/utils.py`), which serves search, bulk fetch,
  changelogs, comments and worklog feeds from in-memory issues, and with real responses captured
  from the test site in `tests/jira/captured_responses.json`.
- `test_issues_nested_types_snowflake` loads nested types to Snowflake and skips without the
  connector or credentials.
- `tests/jira/test_jira_source.py` runs against a live site and fails in CI without
  credentials. `test_load_all_endpoints` loads seeded issues only. `test_issue_comments_changed`
  adds, edits and deletes its own comments on the issue labeled `dlt-seed-comment-changes`.
- `tests/jira/seed_test_data.py` seeds the test site with cases labeled `dlt-seed-<case>`: more
  issues than a bulk fetch batch, long changelogs, many worklogs and comments, deleted worklogs and
  comments, all field types, attachments, links and subtasks. A case is skipped when its label
  exists. `--capture` rewrites the captured responses, anonymized.

```sh
cd sources
uv run python ../tests/jira/seed_test_data.py            # shows missing cases
uv run python ../tests/jira/seed_test_data.py --apply    # creates missing cases
uv run python ../tests/jira/seed_test_data.py --capture  # captures responses for offline tests
```

On the current test site the account cannot delete issues and cannot restrict comments to a group
or role, so `--delete` fails and restricted comments are only tested offline.

## Operations

- **Request sizes**: search lists up to 5000 ids per page. Bulk fetch takes 1000 issues for at most
  100 single value fields, 100 otherwise. Pages of `chunk_size` issues (at least 200) are fetched in
  parallel by the dlt extract `workers` (5 by default). Changelogs are fetched for 1000 issues per
  request, worklogs by 1000 ids.
- **Comments**: bulk fetch embeds the latest 20 comments of an issue. Issues with more are read
  again from `startAt=0` with the comment endpoint, which returns about 100 per page. An issue left
  without comments gets a row flagged `deleted` that deletes its comments loaded before, except on
  the first run.
- **Throttling**: 429 and 5xx are retried with the `runtime.request_*` settings of dlt.
- **Incremental**: JQL compares datetimes with minute precision in the time zone of the user
  (`/myself`, UTC if not set). Search is eventually consistent, so the default 10 minute lag reads
  recent issues again and merge absorbs them. `/search/jql` rejects a query without a filtering
  clause, `created >= "1970-01-01"` bounds a full load.
- **Limits**: `add_limit` on a transformer is ignored. Limit the `issue_ids` resource, the parent
  of `issues`, or use `page_limit` of `jira_search`.
- **Worklogs**: the feeds page by the youngest change of a page. More than 1000 worklogs changed in
  the same millisecond stop the load with an error.
- **Workflow statuses**: `workflows__statuses` holds a `status_reference` and no name, join it to
  `workflow_statuses.status_reference`. `rest/api/3/status` omits unused statuses and
  `rest/api/3/statuses/search` omits project scoped ones, so the `statuses` key of
  `workflows/search` is read instead. Selecting both resources walks that endpoint twice. Do not
  follow its `nextPage` link, it points to an internal Atlassian host.
- **dlt**: a resource function that returns a resource breaks extraction when it is bound inside a
  source (dlt 1.31), so `jira_search` takes its queries as arguments.

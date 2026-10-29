""" This source uses Jira API and dlt to load data such as Issues, Users, Workflows and Projects to the database. """

from datetime import datetime
from typing import Annotated, Any, Iterable, Iterator, List, Optional

import dlt
from dlt.common.configuration import NotResolved, with_config
from dlt.common.configuration.specs import ConfigSectionContext, known_sections
from dlt.common.schema.typing import TTableSchemaColumns, TWriteDispositionConfig
from dlt.common.time import ensure_datetime_in_tz
from dlt.common.typing import TAnyDateTime, TColumnNames, TDataItem
from dlt.sources import DltResource

from .helpers.client import JiraClient, JiraResponseError
from .helpers.comments import COMMENT_COLUMNS, comment_rows
from .helpers.issues import (
    CHANGELOGS_RESOURCE,
    COMMENTS_RESOURCE,
    QUERY_INDEX,
    IssuesPage,
    IssuesReader,
    changelog_rows,
    is_replace,
    make_issues_resource,
)
from .helpers.jql import ORDER_BY
from .helpers.schema_types import drop_keys
from .helpers.worklogs import WORKLOG_COLUMNS, worklog_rows
from .settings import (
    DEFAULT_ENDPOINTS,
    DEFAULT_LAG,
    DEFAULT_PAGE_SIZE,
    MAX_SEARCH_PAGE_SIZE,
    SOURCE_SECTION,
    TEndpoint,
)

__all__ = [
    "DEFAULT_ENDPOINTS",
    "DEFAULT_PAGE_SIZE",
    "JiraClient",
    "JiraResponseError",
    "drop_keys",
    "issue_changelogs",
    "issue_comments",
    "issue_fields",
    "jira",
    "jira_issues",
    "jira_search",
    "worklogs",
]


@dlt.source(max_table_nesting=2)
def jira(
    subdomain: str = dlt.secrets.value,
    email: str = dlt.secrets.value,
    api_token: str = dlt.secrets.value,
    page_size: Optional[int] = None,
    jql: Optional[str] = None,
    start_date: Optional[TAnyDateTime] = None,
    incremental: Optional[dlt.sources.incremental[Any]] = None,
    # Optional, so dlt resolves it from config
    write_disposition: Optional[TWriteDispositionConfig] = "merge",
) -> Iterable[DltResource]:
    """
    Jira source function that generates a list of resource functions based on endpoints.

    Issues, their comments, changelogs and worklogs are loaded incrementally with
    `write_disposition`. Issue fields, users, workflows, workflow statuses and projects are
    replaced on each run.

    Args:
        subdomain: The subdomain for the Jira instance.
        email: The email to authenticate with.
        api_token: The API token to authenticate with.
        page_size: Rows per request of users, workflows and projects. Defaults to the largest
            page each endpoint serves.
        jql: JQL restricting loaded issues and their comments and changelogs, e.g.
            `project = KAN`. Worklogs are read for all issues of the site.
        start_date: Loads issues and worklogs updated since this date on the first run, all of
            them if not set.
        incremental: Your own incremental of issues instead of `start_date`, see `jira_issues`.
        write_disposition: Write disposition of issues, their comments and changelogs and of
            worklogs. "replace" loads all of them, updated since `start_date`, on each run.
    Returns:
        Iterable[DltResource]: List of resource functions.
    """
    start = None if start_date is None else ensure_datetime_in_tz(start_date)
    issues = jira_issues(
        subdomain=subdomain,
        email=email,
        api_token=api_token,
        jql=jql,
        start_date=start,
        incremental=incremental,
        write_disposition=write_disposition,
    )
    # client of the endpoint resources, comments and changelogs use the client of `issues`
    client = JiraClient(subdomain, email, api_token)
    resources = [
        issues,
        # copies of the transformers, the ones of the module keep merge
        (issues | issue_comments()).apply_hints(write_disposition=write_disposition),
        (issues | issue_changelogs()).apply_hints(write_disposition=write_disposition),
        # merged into the default incremental of worklogs, no start date keeps it as is
        worklogs(
            subdomain=subdomain,
            email=email,
            api_token=api_token,
            updated=dlt.sources.incremental(initial_value=start),
        ).apply_hints(write_disposition=write_disposition),
        issue_fields(subdomain=subdomain, email=email, api_token=api_token),
    ]
    for endpoint_name, endpoint in DEFAULT_ENDPOINTS.items():
        res_function = dlt.resource(
            endpoint_rows,
            name=endpoint_name,
            write_disposition="replace",
            primary_key=endpoint["primary_key"],
            # a row without the key lands instead of failing the load package
            columns={endpoint["primary_key"]: {"nullable": True}},
        )(client, endpoint, page_size or endpoint.get("page_size", DEFAULT_PAGE_SIZE))
        resources.append(res_function)

    return resources


def endpoint_rows(
    client: JiraClient, endpoint: TEndpoint, page_size: int
) -> Iterator[TDataItem]:
    """Yields pages of rows of a paginated endpoint, e.g. users or projects"""
    yield from client.paginate_endpoint(endpoint, page_size)


@with_config(
    sections=(known_sections.SOURCES, SOURCE_SECTION, "issues"),
    sections_merge_style=ConfigSectionContext.resource_merge_style,
)
def jira_issues(
    subdomain: str = dlt.secrets.value,
    email: str = dlt.secrets.value,
    api_token: str = dlt.secrets.value,
    jql: Optional[str] = None,
    start_date: Optional[TAnyDateTime] = None,
    incremental: Optional[dlt.sources.incremental[Any]] = None,
    lag: Optional[float] = None,
    chunk_size: Optional[int] = None,
    bulk_fetch_size: Optional[int] = None,
    search_page_size: int = MAX_SEARCH_PAGE_SIZE,
    included_columns: Optional[List[str]] = None,
    excluded_columns: Optional[List[str]] = None,
    write_disposition: Optional[TWriteDispositionConfig] = "merge",
    primary_key: TColumnNames = None,
    merge_key: TColumnNames = None,
    name: Annotated[str, NotResolved()] = "issues",
) -> DltResource:
    """Returns the `issues` resource loading issues matching `jql` incrementally.

    Ids of matching issues are listed with JQL search and pages of `chunk_size` issues are fetched
    with bulk fetch by a parallelized transformer: the number of parallel pages is set by dlt
    `workers` config of the extract step (5 by default). Fields and their types are taken from
    the `/field` metadata: each field is a column named by the field id, e.g. `summary` or
    `customfield_10016`. Comments and worklogs are not loaded into this table: pipe the issues
    into `issue_comments` to load comments, use the `worklogs` resource for worklogs.

    Args:
        subdomain (str): The subdomain for the Jira instance.
        email (str): The email to authenticate with.
        api_token (str): The API token to authenticate with.
        jql (Optional[str]): JQL restricting loaded issues, e.g. `project = KAN`. It must not
            contain ORDER BY.
        start_date (Optional[TAnyDateTime]): Loads issues updated since this date on the first
            run, all issues if not set. It is the initial value of the default incremental
            `dlt.sources.incremental("updated", initial_value=start_date, lag=lag)`, with
            `dedup_sort="desc"` on `updated` so the latest version of an issue wins the merge.
        incremental (Optional[dlt.sources.incremental[Any]]): Your own incremental instead of the
            default one, used as is, without lag and dedup sort. Its cursor must be a date or
            datetime field, e.g. `created` or a custom field id.
        lag (Optional[float]): Seconds re-read on each run by the default incremental, to catch
            issues missing in search results. 600 if not set.
        chunk_size (Optional[int]): Rows in a yielded page of issues and of the comments and
            changelogs piped from them, 1000 if not set. Pages of issues are fetched in parallel,
            each in requests of `bulk_fetch_size` issues.
        bulk_fetch_size (Optional[int]): Issues fetched in a single request, at most 1000. If not
            set: 1000 if at most 100 fields with a single value are loaded, 100 otherwise.
        search_page_size (int): Issue ids listed in a single search request, at most 5000.
        included_columns (Optional[List[str]]): Field ids to load, all fields if not set. Included
            fields get typed columns, also before they have values.
        excluded_columns (Optional[List[str]]): Field ids to skip.
        write_disposition (Optional[TWriteDispositionConfig]): "replace" loads all issues,
            updated since `start_date`, on each run.
        primary_key (TColumnNames): Primary key, the issue `id` if not set.
        merge_key (TColumnNames): Merge key of the resource.
        name (str): Name of the resource and of its table. Its incremental state is kept by the
            `<name>_ids` resource, the parent listing the ids. Use it to load several resources
            of issues in one pipeline.

    Returns:
        DltResource: The dlt resource loading issues.
    """
    if jql and ORDER_BY.search(jql):
        raise ValueError(
            f"JQL `{jql}` must not contain ORDER BY, issues are ordered by cursor"
        )
    if start_date is not None and incremental is not None:
        raise ValueError("Pass `start_date` or `incremental`, not both")
    columns: Optional[TTableSchemaColumns] = None
    if incremental is None:
        start = None if start_date is None else ensure_datetime_in_tz(start_date)
        incremental = dlt.sources.incremental(
            "updated", initial_value=start, lag=DEFAULT_LAG if lag is None else lag
        )
        # the latest version of an issue read more than once in a run wins
        columns = {"updated": {"name": "updated", "dedup_sort": "desc"}}

    reader = IssuesReader(
        JiraClient(subdomain, email, api_token),
        jql=jql,
        chunk_size=chunk_size,
        bulk_fetch_size=bulk_fetch_size,
        search_page_size=search_page_size,
        included_columns=included_columns,
        excluded_columns=excluded_columns,
    )
    return make_issues_resource(
        reader,
        incremental,
        write_disposition or "merge",
        primary_key or "id",
        merge_key=merge_key,
        columns=columns,
        name=name,
    )


@dlt.transformer(
    name=COMMENTS_RESOURCE,
    write_disposition="merge",
    primary_key="id",
    merge_key="issueId",
    columns=COMMENT_COLUMNS,
    parallelized=True,
)
def issue_comments(issues: TDataItem, meta: Any = None) -> Iterator[TDataItem]:
    """Loads all comments of issues piped into it from `jira_issues`, e.g.
    `jira_issues() | issue_comments`.

    The `comment` field of each page of issues is bulk fetched, comments of an issue are paged only
    if Jira does not embed all of them. Comments of each loaded issue replace its comments loaded
    before, so edited and deleted comments are updated. Merge key is `issueId`.
    """
    if isinstance(meta, IssuesPage):
        yield from comment_rows(issues, meta)
    elif issues:
        raise ValueError(
            "issue_comments must be piped from jira_issues, e.g. jira_issues() | issue_comments"
        )


@dlt.transformer(
    name=CHANGELOGS_RESOURCE,
    write_disposition="merge",
    primary_key="id",
    parallelized=True,
)
def issue_changelogs(issues: TDataItem, meta: Any = None) -> Iterator[TDataItem]:
    """Loads all changelogs of issues piped into it from `jira_issues`, e.g.
    `jira_issues() | issue_changelogs`.

    The whole changelog of each issue is fetched, since Jira cannot filter it by date. Changes
    loaded before are merged on `id`.
    """
    if isinstance(meta, IssuesPage):
        yield from changelog_rows(meta.reader.client, issues, meta.reader.chunk_size)
    elif issues:
        raise ValueError(
            "issue_changelogs must be piped from jira_issues,"
            " e.g. jira_issues() | issue_changelogs"
        )


@dlt.resource(
    name="worklogs",
    write_disposition="merge",
    primary_key="id",
    columns=WORKLOG_COLUMNS,
)
def worklogs(
    subdomain: str = dlt.secrets.value,
    email: str = dlt.secrets.value,
    api_token: str = dlt.secrets.value,
    updated: dlt.sources.incremental[datetime] = dlt.sources.incremental(
        "updated", lag=DEFAULT_LAG
    ),
) -> Iterator[TDataItem]:
    """Loads worklogs changed since the last run and removes the deleted ones.

    Worklogs of all issues of the site are read, not only of issues loaded by `jira_issues`. The
    first run reads all worklogs, set `initial_value` of `updated` to start later. Deleted
    worklogs are not read when the resource replaces its table.
    """
    # deleted worklogs are removed on merge, a replaced table would hold them as rows
    read_deleted = not is_replace(dlt.current.resource().write_disposition)
    yield from worklog_rows(
        JiraClient(subdomain, email, api_token), updated, read_deleted
    )


@dlt.resource(name="issue_fields", write_disposition="replace", primary_key="id")
def issue_fields(
    subdomain: str = dlt.secrets.value,
    email: str = dlt.secrets.value,
    api_token: str = dlt.secrets.value,
) -> Iterator[TDataItem]:
    """Loads the names, schemas and JQL clause names of all issue fields, e.g. of custom fields.
    Columns of `issues` are named by the field `id`.
    """
    yield JiraClient(subdomain, email, api_token).fields()


@dlt.source(max_table_nesting=2)
def jira_search(
    subdomain: str = dlt.secrets.value,
    email: str = dlt.secrets.value,
    api_token: str = dlt.secrets.value,
    page_size: Optional[int] = None,
    jql_queries: List[str] = dlt.config.value,
    page_limit: Optional[int] = None,
) -> Iterable[DltResource]:
    """
    Jira search source with an `issues` resource that loads issues matching several JQL queries,
    replaced on each run.

    Issues are loaded like `jira_issues` loads them, each row with the `query_index` of the query
    that found it: an issue found by several queries is loaded once for each of them, and the
    primary key is `id` and `query_index`.

    Args:
        subdomain: The subdomain for the Jira instance.
        email: The email to authenticate with.
        api_token: The API token to authenticate with.
        page_size: Issue ids listed in a single search request, at most 5000 (the default).
        jql_queries: JQL queries to run. A query needs a filtering clause; Jira rejects
            one that only sorts.
        page_limit: Maximum number of search pages of `page_size` issues to read per query,
            since all queries share one resource and `add_limit` would bound them together.

    Returns:
        Iterable[DltResource]: Resource function for searching issues.
    """
    reader = IssuesReader(
        JiraClient(subdomain, email, api_token),
        queries=jql_queries,
        page_limit=page_limit,
        search_page_size=page_size or MAX_SEARCH_PAGE_SIZE,
    )
    return [make_issues_resource(reader, None, "replace", ["id", QUERY_INDEX])]

""" This source uses Jira API and dlt to load data such as Issues, Users, Workflows and Projects to the database. """

from datetime import datetime
from typing import Annotated, Any, Iterable, Iterator, Optional, Sequence

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
    DEFAULT_INCREMENTAL,
    IssuesPage,
    IssuesReader,
    changelog_rows,
    is_default_incremental,
    is_replace,
    make_default_incremental,
    make_issues_resource,
)
from .helpers.jql import ORDER_BY
from .helpers.schema_types import drop_keys
from .helpers.worklogs import WORKLOG_COLUMNS, worklog_rows
from .settings import (
    DEFAULT_ENDPOINTS,
    DEFAULT_EXCLUDED_COLUMNS,
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
    incremental: Optional[dlt.sources.incremental[Any]] = DEFAULT_INCREMENTAL,
    included_columns: Optional[Sequence[str]] = None,
    excluded_columns: Sequence[str] = DEFAULT_EXCLUDED_COLUMNS,
    # Optional, so dlt resolves it from config
    write_disposition: Optional[TWriteDispositionConfig] = "merge",
) -> Iterable[DltResource]:
    """Loads issues, their comments and changelogs, worklogs, issue fields, users, workflows,
    workflow statuses and projects.

    Issues, comments, changelogs and worklogs load incrementally with `write_disposition`, the
    other resources are replaced on each run.

    Args:
        subdomain: The subdomain of the Jira site.
        email: The email to authenticate with.
        api_token: The API token to authenticate with.
        page_size: Rows per request of users, workflows and projects, the largest page of each
            endpoint if not set.
        jql: JQL selecting the issues and their comments and changelogs, e.g. `project = KAN`.
            Worklogs are read for all issues.
        start_date: Loads issues and worklogs updated since this date on the first run, with
            the default incremental.
        incremental: See `jira_issues`.
        included_columns: See `jira_issues`.
        excluded_columns: See `jira_issues`.
        write_disposition: Of issues, comments, changelogs and worklogs. "replace" loads all of
            them, updated since `start_date`, on each run.
    """
    start = None if start_date is None else ensure_datetime_in_tz(start_date)
    issues = jira_issues(
        subdomain=subdomain,
        email=email,
        api_token=api_token,
        jql=jql,
        start_date=start,
        incremental=incremental,
        included_columns=included_columns,
        excluded_columns=excluded_columns,
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
    sections=(known_sections.SOURCES, SOURCE_SECTION),
    section_arg_name="name",
    sections_merge_style=ConfigSectionContext.resource_merge_style,
)
def jira_issues(
    subdomain: str = dlt.secrets.value,
    email: str = dlt.secrets.value,
    api_token: str = dlt.secrets.value,
    jql: Optional[str] = None,
    start_date: Optional[TAnyDateTime] = None,
    incremental: Optional[dlt.sources.incremental[Any]] = DEFAULT_INCREMENTAL,
    lag: Optional[float] = None,
    chunk_size: Optional[int] = None,
    bulk_fetch_size: Optional[int] = None,
    search_page_size: int = MAX_SEARCH_PAGE_SIZE,
    included_columns: Optional[Sequence[str]] = None,
    excluded_columns: Sequence[str] = DEFAULT_EXCLUDED_COLUMNS,
    write_disposition: Optional[TWriteDispositionConfig] = "merge",
    primary_key: TColumnNames = None,
    merge_key: TColumnNames = None,
    name: Annotated[str, NotResolved()] = "issues",
) -> DltResource:
    """Returns the `issues` resource loading issues matching `jql` incrementally.

    Search lists the ids of the issues, pages of `chunk_size` issues are fetched in parallel by the
    extract workers. Each field is a column named by the field id, e.g. `customfield_10016`. Pipe
    the issues into `issue_comments` and `issue_changelogs` to load those.

    Args:
        subdomain (str): The subdomain of the Jira site.
        email (str): The email to authenticate with.
        api_token (str): The API token to authenticate with.
        jql (Optional[str]): JQL selecting the issues, e.g. `project = KAN`. ORDER BY only
            without incremental.
        start_date (Optional[TAnyDateTime]): Loads issues updated since this date on the first
            run. The initial value of the default incremental on `updated`, it wins over the
            `initial_value` of an incremental without `cursor_path`.
        incremental (Optional[dlt.sources.incremental[Any]]): The incremental on `updated` with
            lag and dedup sort by default, an incremental without `cursor_path` sets its values.
            Your own incremental is used as it is, its cursor must be a date or datetime field.
            `None` loads all issues matching `jql` on each run.
        lag (Optional[float]): Seconds the default incremental reads again on each run, 600 if
            not set.
        chunk_size (Optional[int]): Issues in a page, 1000 if not set.
        bulk_fetch_size (Optional[int]): Issues fetched in a request, at most 1000. 100 if
            `attachment`, `issuelinks`, `votes`, `watches`, `comment`, `worklog` or more than 100
            fields are loaded, 1000 otherwise.
        search_page_size (int): Issue ids listed in a search request, at most 5000.
        included_columns (Optional[Sequence[str]]): Field ids to load, they get typed columns
            before they have values. All fields if not set.
        excluded_columns (Sequence[str]): Field ids to skip when `included_columns` is not set.
            Pass `[]` to load `attachment`, `issuelinks`, `votes` and `watches`, which cap bulk
            fetch at 100 issues.
        write_disposition (Optional[TWriteDispositionConfig]): "replace" loads all issues,
            updated since `start_date`, on each run.
        primary_key (TColumnNames): The issue `id` if not set.
        merge_key (TColumnNames): Merge key of the resource.
        name (str): Name of the resource, its table and its config section
            `sources.jira.<name>`. The `<name>_ids` resource keeps the incremental state.
    """
    is_default = is_default_incremental(incremental)
    if jql and incremental is not None and ORDER_BY.search(jql):
        raise ValueError(
            f"JQL `{jql}` must not contain ORDER BY, issues are ordered by cursor. Pass"
            " `incremental=None` to run it as it is"
        )
    if start_date is not None and not is_default:
        raise ValueError(
            "`start_date` sets the default incremental, do not pass `incremental`"
        )
    columns: Optional[TTableSchemaColumns] = None
    if is_default:
        start = None if start_date is None else ensure_datetime_in_tz(start_date)
        incremental = make_default_incremental(
            incremental,
            dlt.sources.incremental[datetime](
                "updated", initial_value=start, lag=DEFAULT_LAG if lag is None else lag
            ),
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

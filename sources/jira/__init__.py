""" This source uses Jira API and dlt to load data such as Issues, Users, Workflows and Projects to the database. """

from datetime import datetime
from typing import Any, Iterable, Iterator, List, Literal, Optional, Union

import dlt
from dlt.common import logger
from dlt.common.schema.typing import TTableSchemaColumns, TWriteDispositionConfig
from dlt.common.typing import TColumnNames, TDataItem
from dlt.sources import DltResource

from .helpers.client import JiraClient, JiraResponseError
from .helpers.comments import COMMENT_COLUMNS, comment_rows
from .helpers.endpoints import get_paginated_data, nullable_key_columns
from .helpers.issues import (
    CHANGELOG_COLUMNS,
    CHANGELOGS_RESOURCE,
    COMMENTS_RESOURCE,
    QUERY_INDEX,
    IssuesPage,
    IssuesReader,
    JiraIssuesResourceConfiguration,
    changelog_rows,
    make_issues_resource,
)
from .helpers.jql import ORDER_BY
from .helpers.schema_types import TBackend
from .helpers.worklogs import WORKLOG_COLUMNS, worklog_rows
from .settings import (
    DEFAULT_ENDPOINTS,
    DEFAULT_LAG,
    DEFAULT_PAGE_SIZE,
    MAX_SEARCH_PAGE_SIZE,
)

__all__ = [
    "DEFAULT_ENDPOINTS",
    "DEFAULT_PAGE_SIZE",
    "JiraClient",
    "JiraResponseError",
    "get_paginated_data",
    "issue_changelogs",
    "issue_comments",
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
    backend: TBackend = "python",
) -> Iterable[DltResource]:
    """
    Jira source function that generates a list of resource functions based on endpoints.

    Issues, their comments, changelogs and worklogs are loaded incrementally and merged. Users,
    workflows, workflow statuses and projects are replaced on each run. Issues and comments are
    fetched with the same requests when both are selected.

    Args:
        subdomain: The subdomain for the Jira instance.
        email: The email to authenticate with.
        api_token: The API token to authenticate with.
        page_size: Rows per request of users, workflows and projects. Defaults to the largest
            page each endpoint serves.
        jql: JQL restricting loaded issues, e.g. `project = KAN`.
        backend: "python" yields issues as dicts, "pyarrow" as Arrow tables.
    Returns:
        Iterable[DltResource]: List of resource functions.
    """
    issues = jira_issues(
        subdomain=subdomain, email=email, api_token=api_token, jql=jql, backend=backend
    )
    resources = [
        issues,
        issues | issue_comments,
        issues
        | issue_changelogs(subdomain=subdomain, email=email, api_token=api_token),
        worklogs(subdomain=subdomain, email=email, api_token=api_token),
    ]
    for endpoint_name, endpoint in DEFAULT_ENDPOINTS.items():
        res_function = dlt.resource(
            get_paginated_data,
            name=endpoint_name,
            write_disposition="replace",
            primary_key=endpoint["primary_key"],
            columns=nullable_key_columns(endpoint["primary_key"]),
        )(
            api_path=endpoint["api_path"],
            data_path=endpoint.get("data_path"),
            offset_path=endpoint.get("offset_path"),
            params=endpoint.get("params"),
            key_field=endpoint["key_field"],
            deduplicate=endpoint.get("deduplicate", False),
            subdomain=subdomain,
            email=email,
            api_token=api_token,
            page_size=page_size or endpoint.get("page_size", DEFAULT_PAGE_SIZE),
        )
        resources.append(res_function)

    return resources


@dlt.resource(name="issues", spec=JiraIssuesResourceConfiguration)
def jira_issues(
    subdomain: str = dlt.secrets.value,
    email: str = dlt.secrets.value,
    api_token: str = dlt.secrets.value,
    jql: Optional[str] = None,
    # "auto" goes first so dlt does not bind the incremental to this factory and replace "auto"
    # with None. the incremental is bound by the resource returned below
    incremental: Union[Literal["auto"], dlt.sources.incremental[Any], None] = "auto",
    lag: Optional[float] = None,
    backend: TBackend = "python",
    nested_types: bool = False,
    chunk_size: Optional[int] = None,
    bulk_fetch_size: Optional[int] = None,
    search_page_size: int = MAX_SEARCH_PAGE_SIZE,
    included_columns: Optional[List[str]] = None,
    excluded_columns: Optional[List[str]] = None,
    write_disposition: Optional[TWriteDispositionConfig] = None,
    primary_key: TColumnNames = None,
    merge_key: TColumnNames = None,
) -> DltResource:
    """Loads issues matching `jql` and the `incremental` range quickly.

    Ids of matching issues are listed with JQL search and pages of `chunk_size` issues are fetched
    with bulk fetch by a parallelized transformer: the number of parallel pages is set by dlt
    `workers` config of the extract step (5 by default). Fields and their types are taken from
    the `/field` metadata: each field is a column named by the field id, e.g. `summary` or
    `customfield_10016`. Comments and worklogs are not loaded into this table: pipe the issues
    into `issue_comments` to load comments, use the `worklogs` resource for worklogs.

    Requests fetch only the data of the selected resources: comments are fetched with the issues
    when both are loaded, only comments when the issues are not loaded, e.g.
    `pipeline.run(jira_issues() | issue_comments)`.

    Args:
        subdomain (str): The subdomain for the Jira instance.
        email (str): The email to authenticate with.
        api_token (str): The API token to authenticate with.
        jql (Optional[str]): JQL restricting loaded issues, e.g. `project = KAN`. It must not
            contain ORDER BY.
        incremental (Union[Literal["auto"], dlt.sources.incremental[Any], None]): Generates
            the JQL range. "auto" loads issues updated since the last run, None loads all issues.
            The cursor must be a date or datetime field, e.g. `created` or a custom field id.
        lag (Optional[float]): Seconds re-read on each run by the "auto" incremental, to catch
            issues missing in search results. 600 if not set.
        backend (TBackend): "python" yields dicts and loads nested values into flattened columns
            and nested tables, "pyarrow" yields Arrow tables and loads nested values as json.
        nested_types (bool): Loads nested values with the pyarrow backend as nested types
            (structs and lists) on destinations that support them.
        chunk_size (Optional[int]): Issues in a yielded page (list of dicts or Arrow table),
            pages are fetched in parallel. If not set: the bulk fetch size, at least 200.
        bulk_fetch_size (Optional[int]): Issues fetched in a single request, at most 1000. If not
            set: 1000 if at most 100 fields with a single value are loaded, 100 otherwise.
        search_page_size (int): Issue ids listed in a single search request, at most 5000.
        included_columns (Optional[List[str]]): Field ids to load, all fields if not set.
        excluded_columns (Optional[List[str]]): Field ids to skip.
        write_disposition (Optional[TWriteDispositionConfig]): "merge" with incremental,
            "replace" without if not set.
        primary_key (TColumnNames): Primary key, the issue `id` if not set.
        merge_key (TColumnNames): Merge key of the resource.

    Returns:
        DltResource: The dlt resource loading issues.
    """
    if jql and ORDER_BY.search(jql):
        raise ValueError(
            f"JQL `{jql}` must not contain ORDER BY, issues are ordered by cursor"
        )
    if backend == "pyarrow":
        # raises with installation instructions when pyarrow is missing
        from dlt.common.libs import pyarrow
    table_incremental: Optional[dlt.sources.incremental[Any]]
    columns: Optional[TTableSchemaColumns] = None
    if incremental == "auto":
        table_incremental = dlt.sources.incremental(
            "updated", lag=DEFAULT_LAG if lag is None else lag
        )
        # the latest version of an issue read more than once in a run wins
        columns = {"updated": {"name": "updated", "dedup_sort": "desc"}}
    else:
        table_incremental = incremental
    if write_disposition is None:
        write_disposition = "merge" if table_incremental else "replace"

    reader = IssuesReader(
        JiraClient(subdomain, email, api_token),
        backend,
        nested_types,
        jql=jql,
        chunk_size=chunk_size,
        bulk_fetch_size=bulk_fetch_size,
        search_page_size=search_page_size,
        included_columns=included_columns,
        excluded_columns=excluded_columns,
    )
    return make_issues_resource(
        reader,
        table_incremental,
        write_disposition,
        primary_key or "id",
        merge_key=merge_key,
        columns=columns,
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

    Comments are fetched with the issues, comments of an issue are paged only if Jira does not
    return all of them. Comments of each loaded issue replace its comments loaded before, so edited
    and deleted comments are updated. Merge key is `issueId`.
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
    columns=CHANGELOG_COLUMNS,
)
def issue_changelogs(
    issues: TDataItem,
    subdomain: str = dlt.secrets.value,
    email: str = dlt.secrets.value,
    api_token: str = dlt.secrets.value,
) -> Iterator[TDataItem]:
    """Loads all changelogs of issues piped into it, e.g. `jira_issues() | issue_changelogs`"""
    yield from changelog_rows(JiraClient(subdomain, email, api_token), issues)


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
    """Loads worklogs changed since the last run and removes the deleted ones"""
    yield from worklog_rows(JiraClient(subdomain, email, api_token), updated)


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
        page_size: Issue ids listed in a single search request, at most 5000.
        jql_queries: JQL queries to run. A query needs a filtering clause; Jira rejects
            one that only sorts.
        page_limit: Maximum number of search pages of `page_size` issues to read per query,
            since all queries share one resource and `add_limit` would bound them together.

    Returns:
        Iterable[DltResource]: Resource function for searching issues.
    """
    reader = IssuesReader(
        JiraClient(subdomain, email, api_token),
        "python",
        False,
        queries=jql_queries,
        page_limit=page_limit,
        search_page_size=page_size or MAX_SEARCH_PAGE_SIZE,
    )
    return [make_issues_resource(reader, None, "replace", ["id", QUERY_INDEX])]

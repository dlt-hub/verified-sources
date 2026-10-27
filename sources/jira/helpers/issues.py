"""Reads Jira issues incrementally with JQL search and bulk fetch"""

from datetime import datetime, timezone
from itertools import chain, islice
from threading import Lock
from typing import Any, Dict, Iterator, List, NamedTuple, Optional, Sequence, Set, Tuple

import dlt
from dlt.common import logger
from dlt.common.configuration.container import Container
from dlt.common.configuration.specs import BaseConfiguration, configspec
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.exceptions import ResourceNameNotAvailable
from dlt.common.json import json
from dlt.common.schema.typing import (
    TColumnSchema,
    TTableSchemaColumns,
    TWriteDispositionConfig,
    TWriteDispositionDict,
)
from dlt.common.schema.utils import merge_columns
from dlt.common.time import ensure_date, ensure_datetime_in_tz, to_tzinfo
from dlt.common.typing import DictStrAny, TColumnNames, TDataItem, TSecretStrValue
from dlt.extract.exceptions import CurrentSourceNotAvailable
from dlt.extract.incremental import Incremental
from dlt.extract.items import DataItemWithMeta
from dlt.sources import DltResource

from .client import JiraClient, batched
from .jql import CursorRange, make_cursor_range, make_jql
from .schema_types import (
    ID_COLUMNS,
    TBackend,
    TShape,
    fields_to_columns,
    get_field_type,
    trim_to_shape,
)
from ..settings import (
    DEFAULT_BULK_ISSUES,
    DEFAULT_CHUNK_SIZE,
    EXCLUDED_FIELDS,
    MAX_BULK_ISSUES,
    MAX_BULK_ISSUES_FIELDS,
    MAX_CHANGELOG_ISSUES,
    MAX_SEARCH_PAGE_SIZE,
    MULTI_VALUE_FIELDS,
)

COMMENTS_RESOURCE = "issue_comments"
CHANGELOGS_RESOURCE = "issue_changelogs"
# position of the query that found an issue in the queries of `jira_search`
QUERY_INDEX = "query_index"
QUERY_INDEX_COLUMN: TColumnSchema = {
    "name": QUERY_INDEX,
    "data_type": "bigint",
    "nullable": False,
}
CHANGELOG: TShape = {
    "id": "text",
    "created": "text",
    "author": {"accountId": "text", "displayName": "text"},
    "items": [
        {
            "field": "text",
            "fieldId": "text",
            "fieldtype": "text",
            "from": "text",
            "fromString": "text",
            "to": "text",
            "toString": "text",
        }
    ],
}
CHANGELOG_COLUMNS: TTableSchemaColumns = {
    "id": {"name": "id", "data_type": "text", "nullable": False},
    "issueId": {"name": "issueId", "data_type": "text"},
    "created": {"name": "created", "data_type": "timestamp"},
}


@configspec
class JiraIssuesResourceConfiguration(BaseConfiguration):
    # `incremental` is not configurable: dlt would parse "auto" as an incremental initial value
    subdomain: str = None
    email: str = None
    api_token: TSecretStrValue = None
    jql: Optional[str] = None
    lag: Optional[float] = None
    backend: TBackend = "python"
    nested_types: bool = False
    chunk_size: Optional[int] = None
    bulk_fetch_size: Optional[int] = None
    search_page_size: int = MAX_SEARCH_PAGE_SIZE
    included_columns: Optional[List[str]] = None
    excluded_columns: Optional[List[str]] = None
    write_disposition: Optional[TWriteDispositionDict] = None
    primary_key: Optional[TColumnNames] = None
    merge_key: Optional[TColumnNames] = None


class FetchPlan(NamedTuple):
    """Data fetched by the `issues` transformer for the resources that consume it"""

    issues_selected: bool
    fields: bool
    comments: bool


FETCH_ALL = FetchPlan(issues_selected=True, fields=True, comments=True)


class IssuesPage(NamedTuple):
    """Meta of a page of issues received by transformers piped from `issues`"""

    reader: "IssuesReader"
    # comments embedded in the issues by issue id, None if comments were not fetched
    comments: Optional[Dict[str, Optional[DictStrAny]]]


def select_fields(
    fields: Dict[str, DictStrAny],
    included_columns: Optional[List[str]],
    excluded_columns: Optional[List[str]],
    required_columns: List[str],
) -> List[DictStrAny]:
    """Selects fields to fetch: all fields except the ones loaded by other resources or the
    `included_columns`. `required_columns` are always fetched.
    """
    if included_columns:
        unknown = [
            name
            for name in included_columns
            if name not in fields and name not in ID_COLUMNS
        ]
        if unknown:
            logger.warning(
                f"Columns {unknown} do not match any Jira field and will be ignored"
            )
        names = [name for name in included_columns if name in fields]
    else:
        names = [field_id for field_id in fields if field_id not in EXCLUDED_FIELDS]
    if excluded_columns:
        names = [name for name in names if name not in excluded_columns]
    names.extend(name for name in required_columns if name not in names)
    return [fields[name] for name in names]


def plan_fetch() -> FetchPlan:
    """Plans what the `issues` transformer fetches for the resources selected in the current run.

    Must be called by the `issue_ids` resource. Issue fields are fetched when `issues` are loaded
    or consumed by transformers other than `issue_comments` and `issue_changelogs`, which needs
    issue ids only. Comments are fetched with issues when `issue_comments` consumes them.
    """
    try:
        ids_pipe = dlt.current.pipe()
        resources = dlt.current.source().resources
    except (ResourceNameNotAvailable, CurrentSourceNotAvailable):
        return FETCH_ALL
    pipes = resources.extracted_pipes
    issues_pipes = {
        pipe.instance_id
        for pipe in pipes
        if pipe.parent is not None and pipe.parent.instance_id == ids_pipe.instance_id
    }
    consumers = {
        pipe.name
        for pipe in pipes
        if pipe.parent is not None and pipe.parent.instance_id in issues_pipes
    }
    issues_selected = any(
        pipe.instance_id in issues_pipes for pipe in resources.selected_pipes
    )
    return FetchPlan(
        issues_selected=issues_selected,
        fields=issues_selected
        or bool(consumers - {COMMENTS_RESOURCE, CHANGELOGS_RESOURCE}),
        comments=COMMENTS_RESOURCE in consumers,
    )


def get_bulk_size(field_ids: List[str]) -> int:
    """Returns the max number of issues in a bulk fetch request for `field_ids`"""
    if len(field_ids) <= MAX_BULK_ISSUES_FIELDS and not MULTI_VALUE_FIELDS.intersection(
        field_ids
    ):
        return MAX_BULK_ISSUES
    return DEFAULT_BULK_ISSUES


class IssuesReader:
    """Reads issues like `sql_table` reads a table, shared by the `issue_ids` resource and the
    `issues` transformer.

    JQL is generated from the incremental of the `issue_ids` resource, which yields pages of
    `chunk_size` ids and cursor values of matching issues. The parallelized `issues` transformer
    fetches each page in batches with bulk fetch, together with the comments when
    `issue_comments` consumes them.

    With `queries`, each query runs as is, without incremental, and rows get the `query_index` of
    the query that found them.
    """

    def __init__(
        self,
        client: JiraClient,
        backend: TBackend,
        nested_types: bool,
        jql: Optional[str] = None,
        queries: Optional[Sequence[str]] = None,
        page_limit: Optional[int] = None,
        chunk_size: Optional[int] = None,
        bulk_fetch_size: Optional[int] = None,
        search_page_size: int = MAX_SEARCH_PAGE_SIZE,
        included_columns: Optional[List[str]] = None,
        excluded_columns: Optional[List[str]] = None,
    ) -> None:
        self.client = client
        self.backend = backend
        self.nested_types = nested_types
        self.jql = jql
        self.queries = queries
        self.page_limit = page_limit
        # requested sizes, the effective sizes depend on the fetched fields
        self._chunk_size = chunk_size
        self._bulk_fetch_size = bulk_fetch_size
        self.chunk_size = chunk_size or DEFAULT_CHUNK_SIZE
        self.bulk_fetch_size = bulk_fetch_size or DEFAULT_BULK_ISSUES
        self.search_page_size = search_page_size
        self.included_columns = included_columns
        self.excluded_columns = excluded_columns
        # resolved from `/field` metadata when ids are listed
        self.plan = FETCH_ALL
        self.selected: List[DictStrAny] = []
        self.field_types: Dict[str, Optional[TShape]] = {}
        self.fetched_fields: List[str] = []
        self.columns: TTableSchemaColumns = {}
        # column hints are computed once per run by the first of the parallel transformer calls
        self._hints_lock = Lock()
        self._column_hints: Optional[TTableSchemaColumns] = None
        self.incremental: Optional[Incremental[Any]] = None
        self.cursor: Optional[CursorRange] = None

    def bind(
        self, incremental: Optional[Incremental[Any]], plan: FetchPlan = FETCH_ALL
    ) -> Optional[str]:
        """Reads field metadata, binds `incremental` and selects data to fetch according to `plan`,
        returns the cursor field id
        """
        fields = {field["id"]: field for field in self.client.fields()}
        self.incremental = incremental
        self.plan = plan
        cursor_name: Optional[str] = None
        self.cursor = None
        if incremental:
            self.cursor = make_cursor_range(
                incremental, fields, to_tzinfo(self.client.time_zone())
            )
            cursor_name = incremental.get_cursor_column_name()
        self.selected = (
            select_fields(
                fields,
                self.included_columns,
                self.excluded_columns,
                [cursor_name] if cursor_name else [],
            )
            if plan.fields
            else []
        )
        self.field_types = {
            field["id"]: get_field_type(field) for field in self.selected
        }
        self.fetched_fields = list(self.field_types)
        if plan.comments and "comment" not in self.field_types:
            self.fetched_fields.append("comment")
        self.bulk_fetch_size = self._bulk_fetch_size or get_bulk_size(
            self.fetched_fields
        )
        # a page fetched with a single request when possible, more pages are fetched in parallel
        self.chunk_size = self._chunk_size or max(
            self.bulk_fetch_size, DEFAULT_CHUNK_SIZE
        )
        self.client.split_batches = False
        self._column_hints = None
        return cursor_name

    @property
    def initial_load(self) -> bool:
        """Tells if this is the first run of an incremental load"""
        return self.cursor is not None and self.cursor.last_value is None

    def make_jqls(self) -> List[Tuple[Optional[int], str]]:
        """Returns the JQL queries to run with their query index, None without `queries`"""
        if self.queries is not None:
            return [
                (index, make_jql(jql, None)) for index, jql in enumerate(self.queries)
            ]
        return [(None, make_jql(self.jql, self.cursor))]

    def column_hints(self) -> TTableSchemaColumns:
        """Maps selected fields to column hints merged with hints applied by the user"""
        with self._hints_lock:
            if self._column_hints is None:
                columns = fields_to_columns(
                    self.selected, self.backend, self.nested_types
                )
                # transformers consuming `issues` run them as a parent without resource
                resource = dlt.current.resource() if self.plan.issues_selected else None
                if resource is not None and not callable(resource.columns):
                    source_columns = {
                        **ID_COLUMNS,
                        **{f["id"]: f for f in self.selected},
                    }
                    unmatched = [
                        name for name in resource.columns if name not in source_columns
                    ]
                    if unmatched and self.backend == "pyarrow":
                        logger.warning(
                            f"Resource columns {unmatched} do not match any queried Jira field"
                            " and will be ignored. Use the original Jira field ids in"
                            " apply_hints."
                        )
                    columns = merge_columns(columns, resource.columns)
                if self.queries is not None:
                    columns[QUERY_INDEX] = dict(QUERY_INDEX_COLUMN)  # type: ignore[assignment]
                # queried columns in the order of values in Arrow rows, without user columns
                queried_columns = [*ID_COLUMNS, *self.field_types, QUERY_INDEX]
                self.columns = {
                    name: columns[name] for name in queried_columns if name in columns
                }
                self._column_hints = columns
            return self._column_hints

    def fetch(self, ids: List[str]) -> List[DictStrAny]:
        """Fetches issues with `ids` in batches of `bulk_fetch_size`. Issues are not fetched when
        no data is needed, e.g. when only changelogs are loaded.
        """
        if not self.fetched_fields:
            return [{"id": issue_id} for issue_id in ids]
        return self.client.bulk_fetch(ids, self.fetched_fields, self.bulk_fetch_size)

    def fetch_comments(self, ids: List[str]) -> Dict[str, Optional[DictStrAny]]:
        """Fetches comments embedded in issues with `ids` when they were not fetched with issues"""
        return {
            issue["id"]: (issue.get("fields") or {}).get("comment")
            for issue in self.client.bulk_fetch(ids, ["comment"], DEFAULT_BULK_ISSUES)
        }

    def search(
        self, jql: str, cursor_name: Optional[str]
    ) -> Iterator[List[DictStrAny]]:
        """Yields pages of ids of issues matching `jql` with values of the `cursor_name` field"""
        for page in self.client.search(jql, cursor_name or "id", self.search_page_size):
            items: List[DictStrAny] = []
            for issue in page:
                item = {"id": issue["id"]}
                if cursor_name and self.cursor:
                    value = (issue.get("fields") or {}).get(cursor_name)
                    if value is not None:
                        # the incremental compares datetimes, not strings with offsets
                        value = (
                            ensure_date(value)
                            if self.cursor.cursor_type == "date"
                            else ensure_datetime_in_tz(value)
                        )
                    item[cursor_name] = value
                items.append(item)
            yield items

    def convert(
        self, issues: List[DictStrAny], query_index: Optional[int] = None
    ) -> TDataItem:
        invalid_fields: Set[str] = set()
        rows = [self._to_row(issue, invalid_fields) for issue in issues]
        if query_index is not None:
            for row in rows:
                row[QUERY_INDEX] = query_index
        if invalid_fields:
            logger.warning(
                f"Jira fields {sorted(invalid_fields)} have values of unexpected structure that"
                " are loaded as null"
            )
        return rows_to_items(rows, self.columns, self.backend)

    def _to_row(self, issue: DictStrAny, invalid_fields: Set[str]) -> DictStrAny:
        row: DictStrAny = {"id": issue["id"], "key": issue.get("key")}
        fields = issue.get("fields") or {}
        for field_id, field_type in self.field_types.items():
            try:
                value = trim_to_shape(fields.get(field_id), field_type)
            except ValueError:
                invalid_fields.add(field_id)
                value = None
            # empty fields are skipped, the normalizer would create columns without type for them
            if value is None:
                continue
            # values of unknown structure are loaded as text
            if field_type is None and not isinstance(value, str):
                value = json.dumps(value)
            # the incremental compares datetimes, not strings with offsets
            elif field_type == "timestamp":
                value = ensure_datetime_in_tz(value)
            elif field_type == "date":
                value = ensure_date(value)
            row[field_id] = value
        return row


def rows_to_items(
    rows: List[DictStrAny], columns: TTableSchemaColumns, backend: TBackend
) -> TDataItem:
    """Returns `rows` as a list or as an Arrow table with `columns`, in the order of values"""
    if backend == "python":
        return rows
    from dlt.common.libs.pyarrow import (
        get_py_arrow_datatype,
        pyarrow,
        row_tuples_to_arrow,
    )

    # generate arrow types compatible with the destination when running in a pipeline
    caps = (
        Container().get(DestinationCapabilitiesContext)
        or DestinationCapabilitiesContext.generic_capabilities()
    )
    # serialize nested values that the destination does not load as nested types
    json_columns = [
        name
        for name, col in columns.items()
        if col.get("data_type") == "json"
        and pyarrow.types.is_string(get_py_arrow_datatype(col, caps, "UTC"))
    ]
    for row in rows:
        for name in json_columns:
            if name in row:
                row[name] = json.dumps(row[name])
    return row_tuples_to_arrow(
        [tuple(row.get(name) for name in columns) for row in rows],
        caps,
        columns,
        "UTC",
    )


def page_ids(issues: TDataItem) -> List[str]:
    """Returns ids of a page of issues, a list of dicts or an Arrow table"""
    if isinstance(issues, list):
        return [issue["id"] for issue in issues]
    ids: List[str] = issues.column("id").to_pylist()
    return ids


def issue_ids(
    reader: IssuesReader, incremental: Optional[Incremental[Any]] = None
) -> Iterator[TDataItem]:
    """Yields pages of `chunk_size` ids and cursor values of issues matching JQL generated from
    `incremental`. With queries of `jira_search`, ids are tagged with the query index and each
    query reads at most `page_limit` search pages.
    """
    cursor_name = reader.bind(incremental, plan_fetch())
    for query_index, jql in reader.make_jqls():
        logger.info(f"Reading Jira issues matching JQL: {jql}")
        search_pages = reader.search(jql, cursor_name)
        if query_index is not None:
            search_pages = islice(search_pages, reader.page_limit)
        # each data item has a fixed cost, e.g. schema of every Arrow table is merged on extract
        for page in batched(chain.from_iterable(search_pages), reader.chunk_size):
            if query_index is not None:
                page = [{**item, QUERY_INDEX: query_index} for item in page]
            yield page


def issue_details(ids: TDataItem, reader: IssuesReader) -> Iterator[TDataItem]:
    """Fetches issues in a page of ids.

    Calls run in parallel and items of different calls reach the extractor in any order, so each
    call yields the column hints before its page. Comments fetched with the issues are passed in
    the meta of the page to the transformers piped from `issues`.
    """
    yield dlt.mark.with_hints([], dlt.mark.make_hints(columns=reader.column_hints()))
    # a page of ids holds issues found by one query
    query_index: Optional[int] = ids[0].get(QUERY_INDEX) if ids else None
    issues = reader.fetch([item["id"] for item in ids])
    if issues:
        comments = (
            {
                issue["id"]: (issue.get("fields") or {}).get("comment")
                for issue in issues
            }
            if reader.plan.comments
            else None
        )
        yield DataItemWithMeta(
            IssuesPage(reader, comments), reader.convert(issues, query_index)
        )


def make_issues_resource(
    reader: IssuesReader,
    incremental: Optional[Incremental[Any]],
    write_disposition: TWriteDispositionConfig,
    primary_key: TColumnNames,
    merge_key: TColumnNames = None,
    columns: Optional[TTableSchemaColumns] = None,
) -> DltResource:
    """Returns the parallelized `issues` transformer of the `issue_ids` resource, which lists
    ids of issues in the incremental range and holds the incremental state
    """
    ids = dlt.resource(issue_ids, name="issue_ids", primary_key="id")(
        reader, incremental=incremental
    )
    return dlt.transformer(
        issue_details,
        name="issues",
        data_from=ids,
        write_disposition=write_disposition,  # type: ignore[arg-type]
        primary_key=primary_key,
        merge_key=merge_key,
        columns=columns,
        parallelized=True,
    )(reader)


def changelog_rows(client: JiraClient, issues: TDataItem) -> Iterator[TDataItem]:
    """Fetches all changelogs of `issues`, a page of issues as list or Arrow table"""
    for batch in batched(page_ids(issues), MAX_CHANGELOG_ISSUES):
        for page in client.changelogs(batch):
            rows: List[DictStrAny] = []
            for issue_changelog in page:
                for history in issue_changelog.get("changeHistories") or []:
                    row = trim_to_shape(history, CHANGELOG)
                    row["issueId"] = issue_changelog["issueId"]
                    created = row.get("created")
                    if isinstance(created, (int, float)):
                        # bulk fetch returns UNIX timestamps in milliseconds
                        row["created"] = datetime.fromtimestamp(
                            created / 1000, timezone.utc
                        )
                    elif created:
                        row["created"] = ensure_datetime_in_tz(created)
                    rows.append(row)
            if rows:
                yield rows

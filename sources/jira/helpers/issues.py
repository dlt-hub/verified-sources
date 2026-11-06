"""Reads Jira issues incrementally with JQL search and bulk fetch"""

from datetime import datetime, timezone
from itertools import chain, islice
from threading import Lock
from typing import Any, Dict, Iterator, List, NamedTuple, Optional, Sequence, Set, Tuple

import dlt
from dlt.common import logger
from dlt.common.exceptions import ResourceNameNotAvailable
from dlt.common.schema.typing import TTableSchemaColumns, TWriteDispositionConfig
from dlt.common.schema.utils import merge_columns
from dlt.common.time import to_tzinfo
from dlt.common.typing import DictStrAny, TColumnNames, TDataItem, TTableHintTemplate
from dlt.extract.exceptions import CurrentSourceNotAvailable, ResourceNotFoundError
from dlt.extract.incremental import Incremental
from dlt.extract.items import DataItemWithMeta
from dlt.sources import DltResource

from .client import JiraClient, batched
from .jql import CursorRange, make_cursor_range, make_jql
from .schema_types import (
    ID_COLUMNS,
    convert_record,
    convert_value,
    fields_to_columns,
    get_field_type,
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
    SOURCE_SECTION,
)

# resources built here resolve config under the section of the source, not of this module
__source_name__ = SOURCE_SECTION

COMMENTS_RESOURCE = "issue_comments"
CHANGELOGS_RESOURCE = "issue_changelogs"
# position of the query that found an issue in the queries of `jira_search`
QUERY_INDEX = "query_index"
CHANGELOG_TYPES = {"created": "timestamp"}


class IssuesPage(NamedTuple):
    """Meta of a page of issues received by transformers piped from `issues`"""

    reader: "IssuesReader"


def is_replace(write_disposition: TTableHintTemplate[TWriteDispositionConfig]) -> bool:
    """True if `write_disposition` replaces the table, as shorthand or dict"""
    if callable(write_disposition):
        return False
    if isinstance(write_disposition, dict):
        return write_disposition["disposition"] == "replace"
    return write_disposition == "replace"


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


def get_bulk_size(field_ids: List[str]) -> int:
    """Returns the max number of issues in a bulk fetch request for `field_ids`"""
    if len(field_ids) <= MAX_BULK_ISSUES_FIELDS and not MULTI_VALUE_FIELDS.intersection(
        field_ids
    ):
        return MAX_BULK_ISSUES
    return DEFAULT_BULK_ISSUES


class IssuesReader:
    """Reads issues, shared by the ids resource and the issues transformer built by
    `make_issues_resource`.

    Search lists the ids of issues matching JQL generated from the incremental, and pages of
    issues are fetched in parallel.

    With `queries`, each query runs as is, without incremental, and rows get the `query_index` of
    the query that found them.
    """

    def __init__(
        self,
        client: JiraClient,
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
        self.selected: List[DictStrAny] = []
        self.field_types: Dict[str, Optional[str]] = {}
        # computed once per run by the first of the parallel transformer calls
        self._hints_lock = Lock()
        self._column_hints: Optional[TTableSchemaColumns] = None
        self.incremental: Optional[Incremental[Any]] = None
        self.cursor: Optional[CursorRange] = None
        # issues updated after this time are loaded by the next run
        self.read_until: Optional[datetime] = None
        # first run of an incremental load, nothing loaded before needs to be deleted
        self.initial_load = False
        # write disposition of the issues, set by `make_issues_resource`
        self.write_disposition: TWriteDispositionConfig = "merge"

    def bind(self, incremental: Optional[Incremental[Any]]) -> None:
        """Reads field metadata, binds `incremental` and selects the fields to fetch"""
        fields = {field["id"]: field for field in self.client.fields()}
        self.incremental = incremental
        cursor_name: Optional[str] = None
        self.cursor = None
        self.read_until = None
        self.initial_load = False
        if incremental:
            self.cursor = make_cursor_range(
                incremental, fields, to_tzinfo(self.client.time_zone())
            )
            cursor_name = incremental.get_cursor_column_name()
            # later updates go to the next run, custom fields may hold future values
            if (
                self.cursor.cursor_type == "timestamp"
                and self.cursor.last_value_func is max
                and self.cursor.end_value is None
                and not fields[cursor_name].get("custom")
                and not is_replace(self.write_disposition)
            ):
                self.read_until = datetime.now(timezone.utc)
            # the stored last value advances past the initial value once issues are loaded
            self.initial_load = (
                incremental.end_value is None
                and incremental.get_state()["last_value"] == incremental.initial_value
            )
        self.selected = select_fields(
            fields,
            self.included_columns,
            self.excluded_columns,
            [cursor_name] if cursor_name else [],
        )
        self.field_types = {
            field["id"]: get_field_type(field) for field in self.selected
        }
        self.bulk_fetch_size = self._bulk_fetch_size or get_bulk_size(
            list(self.field_types)
        )
        self.chunk_size = self._chunk_size or DEFAULT_CHUNK_SIZE
        self.client.split_batches = False
        self._column_hints = None

    def make_jqls(self) -> List[Tuple[Optional[int], str]]:
        """Returns the JQL queries to run with their query index, None without `queries`"""
        if self.queries is not None:
            return [
                (index, make_jql(jql, None)) for index, jql in enumerate(self.queries)
            ]
        return [(None, make_jql(self.jql, self.cursor))]

    def column_hints(self) -> Optional[TTableSchemaColumns]:
        """Returns typed columns of the included columns merged with the hints applied by the
        user, None if `included_columns` is not set
        """
        if not self.included_columns:
            return None
        with self._hints_lock:
            if self._column_hints is None:
                columns = fields_to_columns(self.selected)
                # hints yielded with data override the hints of the resource, so the hints of the
                # user are merged on top
                try:
                    resource: Optional[DltResource] = dlt.current.resource()
                except (
                    CurrentSourceNotAvailable,
                    ResourceNameNotAvailable,
                    ResourceNotFoundError,
                ):
                    resource = None
                if resource is not None and not callable(resource.columns):
                    columns = merge_columns(columns, resource.columns)
                self._column_hints = columns
            return self._column_hints

    def fetch(self, ids: List[str]) -> List[DictStrAny]:
        """Fetches issues with `ids` in batches of `bulk_fetch_size`"""
        return self.client.bulk_fetch(ids, list(self.field_types), self.bulk_fetch_size)

    def fetch_comments(self, ids: List[str]) -> Dict[str, Optional[DictStrAny]]:
        """Fetches the comments embedded in issues with `ids`, by issue id"""
        return {
            issue["id"]: (issue.get("fields") or {}).get("comment")
            for issue in self.client.bulk_fetch(ids, ["comment"], DEFAULT_BULK_ISSUES)
        }

    def search(self, jql: str) -> Iterator[List[str]]:
        """Yields pages of ids of issues matching `jql`"""
        # search serves 5000 ids per page, but only 100 issues when it also returns a field
        for page in self.client.search(jql, "id", self.search_page_size):
            yield [issue["id"] for issue in page]

    def convert(
        self, issues: List[DictStrAny], query_index: Optional[int] = None
    ) -> List[DictStrAny]:
        """Converts `issues` to rows with the selected fields as top level values"""
        invalid_fields: Set[str] = set()
        rows = [self._to_row(issue, invalid_fields) for issue in issues]
        if query_index is not None:
            for row in rows:
                row[QUERY_INDEX] = query_index
        if invalid_fields:
            logger.warning(
                f"Jira fields {sorted(invalid_fields)} have values of unexpected structure or"
                " type that are loaded as null"
            )
        return rows

    def _to_row(self, issue: DictStrAny, invalid_fields: Set[str]) -> DictStrAny:
        row: DictStrAny = {"id": issue["id"], "key": issue.get("key")}
        fields = issue.get("fields") or {}
        for field_id, field_type in self.field_types.items():
            try:
                value = convert_value(fields.get(field_id), field_type)
            except ValueError:
                invalid_fields.add(field_id)
                value = None
            # empty fields are skipped, the normalizer would create columns without type for them
            if value is not None:
                row[field_id] = value
        return row


def issue_ids(
    reader: IssuesReader, incremental: Optional[Incremental[Any]] = None
) -> Iterator[TDataItem]:
    """Yields deferred fetches of pages of issues matching JQL generated from `incremental`"""
    reader.bind(incremental)
    for query_index, jql in reader.make_jqls():
        logger.info(f"Reading Jira issues matching JQL: {jql}")
        search_pages = reader.search(jql)
        if query_index is not None:
            search_pages = islice(search_pages, reader.page_limit)
        # fetched issues carry the cursor values, the incremental reads them from there
        for ids in batched(chain.from_iterable(search_pages), reader.chunk_size):
            yield fetch_issues(reader, ids, query_index)


@dlt.defer
def fetch_issues(
    reader: IssuesReader, ids: List[str], query_index: Optional[int]
) -> List[DictStrAny]:
    """Fetches a page of issues and converts them to rows"""
    rows = reader.convert(reader.fetch(ids), query_index)
    if reader.read_until is None or reader.incremental is None:
        return rows
    cursor = reader.incremental.get_cursor_column_name()
    return [
        row
        for row in rows
        if row.get(cursor) is None or row[cursor] <= reader.read_until
    ]


def issue_details(issues: TDataItem, reader: IssuesReader) -> Iterator[TDataItem]:
    """Passes fetched issues on with the reader in their meta, for transformers piped from them"""
    columns = reader.column_hints()
    if columns:
        yield dlt.mark.with_hints([], dlt.mark.make_hints(columns=columns))
    if issues:
        yield DataItemWithMeta(IssuesPage(reader), issues)


def make_issues_resource(
    reader: IssuesReader,
    incremental: Optional[Incremental[Any]],
    write_disposition: TWriteDispositionConfig,
    primary_key: TColumnNames,
    merge_key: TColumnNames = None,
    columns: Optional[TTableSchemaColumns] = None,
    name: str = "issues",
) -> DltResource:
    """Returns the `name` transformer of the `<name>_ids` resource, which lists and fetches the
    issues in the incremental range and holds the incremental state
    """
    reader.write_disposition = write_disposition
    # dlt resets the state of a resource with the `replace` shorthand, here the incremental state
    ids = dlt.resource(
        issue_ids,
        name=f"{name}_ids",
        primary_key="id",
        write_disposition=write_disposition,
    )(reader, incremental=incremental)
    return dlt.transformer(
        issue_details,
        name=name,
        data_from=ids,
        write_disposition=write_disposition,  # type: ignore[arg-type,unused-ignore]
        primary_key=primary_key,
        merge_key=merge_key,
        columns=columns,
    )(reader)


def changelog_rows(
    client: JiraClient, issues: List[DictStrAny], chunk_size: int = DEFAULT_CHUNK_SIZE
) -> Iterator[TDataItem]:
    """Fetches all changelogs of a page of `issues` and yields them in pages of `chunk_size`
    changes
    """
    ids = [issue["id"] for issue in issues]
    yield from batched(_changelog_rows(client, ids), chunk_size)


def _changelog_rows(client: JiraClient, ids: List[str]) -> Iterator[DictStrAny]:
    for batch in batched(ids, MAX_CHANGELOG_ISSUES):
        for page in client.changelogs(batch):
            for issue_changelog in page:
                for history in issue_changelog.get("changeHistories") or []:
                    created = history.get("created")
                    if isinstance(created, (int, float)):
                        # bulk fetch returns UNIX timestamps in milliseconds
                        history["created"] = datetime.fromtimestamp(
                            created / 1000, timezone.utc
                        )
                    row = convert_record(history, CHANGELOG_TYPES)
                    row["issueId"] = issue_changelog["issueId"]
                    yield row

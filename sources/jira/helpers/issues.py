"""Reads Jira issues incrementally with JQL search and bulk fetch"""

from datetime import datetime, timezone
from itertools import chain
from threading import Lock
from typing import Any, Dict, Iterator, List, NamedTuple, Optional, Sequence, Set

import dlt
from dlt.common import logger
from dlt.common.exceptions import ResourceNameNotAvailable
from dlt.common.schema.typing import TTableSchemaColumns, TWriteDispositionConfig
from dlt.common.schema.utils import merge_columns
from dlt.common.time import ensure_datetime_in_tz, to_tzinfo
from dlt.common.typing import DictStrAny, TColumnNames, TDataItem, TTableHintTemplate
from dlt.extract.exceptions import CurrentSourceNotAvailable, ResourceNotFoundError
from dlt.extract.incremental import Incremental
from dlt.extract.items import DataItemWithMeta
from dlt.extract.items_transform import LimitItem
from dlt.sources import DltResource

from .client import JiraClient, batched
from .jql import make_cursor_range, make_jql
from .schema_types import (
    ID_COLUMNS,
    convert_record,
    convert_value,
    fields_to_columns,
    get_field_type,
)
from ..settings import (
    BOUNDING_JQL,
    BULK_CAPPED_FIELDS,
    DEFAULT_BULK_ISSUES,
    DEFAULT_CHUNK_SIZE,
    DEFAULT_EXCLUDED_COLUMNS,
    EXCLUDED_FIELDS,
    MAX_BULK_ISSUES,
    MAX_BULK_ISSUES_FIELDS,
    MAX_CHANGELOG_ISSUES,
    MAX_SEARCH_PAGE_SIZE,
    SOURCE_SECTION,
)

# resources built here resolve config under the section of the source, not of this module
__source_name__ = SOURCE_SECTION

# cursor that marks the default of `incremental` arguments, no Jira field has this id. The default
# is replaced by the incremental on `updated`, `None` loads without incremental
DEFAULT_INCREMENTAL_MARKER = "_dlt_default_incremental_marker"
DEFAULT_INCREMENTAL: Incremental[Any] = dlt.sources.incremental(
    DEFAULT_INCREMENTAL_MARKER
)

COMMENTS_RESOURCE = "issue_comments"
CHANGELOGS_RESOURCE = "issue_changelogs"
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


def is_default_incremental(incremental: Optional[Incremental[Any]]) -> bool:
    """True if `incremental` is a copy of `DEFAULT_INCREMENTAL`, dlt copies argument defaults"""
    return (
        incremental is not None
        and incremental.cursor_path == DEFAULT_INCREMENTAL_MARKER
    )


def make_default_incremental(
    incremental: Incremental[Any], initial_value: Any, lag: float
) -> Incremental[Any]:
    """Returns the incremental on `updated` with the values set on the copy of
    `DEFAULT_INCREMENTAL`, e.g. its `end_value` from config
    """
    values = incremental.copy()
    values.cursor_path = None
    # values from config are strings
    if values.initial_value is not None:
        values.initial_value = ensure_datetime_in_tz(values.initial_value)
    if values.end_value is not None:
        values.end_value = ensure_datetime_in_tz(values.end_value)
    default: Incremental[Any] = dlt.sources.incremental(
        "updated", initial_value=initial_value, lag=lag
    )
    return default.merge(values)


def select_fields(
    fields: Dict[str, DictStrAny],
    included_columns: Optional[Sequence[str]],
    excluded_columns: Sequence[str],
    required_columns: List[str],
) -> List[DictStrAny]:
    """Selects fields to fetch: the `included_columns`, or all fields except the ones loaded by
    other resources and `excluded_columns`. `required_columns` are always fetched.
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
        names = [
            field_id
            for field_id in fields
            if field_id not in EXCLUDED_FIELDS and field_id not in excluded_columns
        ]
    names.extend(name for name in required_columns if name not in names)
    return [fields[name] for name in names]


def current_resource() -> Optional[DltResource]:
    """The resource being extracted, None outside of a pipe"""
    try:
        return dlt.current.resource()
    except (CurrentSourceNotAvailable, ResourceNameNotAvailable, ResourceNotFoundError):
        return None


def get_bulk_size(field_ids: List[str]) -> int:
    """Returns the max number of issues in a bulk fetch request for `field_ids`"""
    if len(field_ids) <= MAX_BULK_ISSUES_FIELDS and not BULK_CAPPED_FIELDS.intersection(
        field_ids
    ):
        return MAX_BULK_ISSUES
    return DEFAULT_BULK_ISSUES


class IssuesReader:
    """Lists ids of issues in the incremental range and fetches them, shared by the ids resource
    and the issues transformer of `make_issues_resource`
    """

    def __init__(
        self,
        client: JiraClient,
        jql: Optional[str] = None,
        chunk_size: Optional[int] = None,
        bulk_fetch_size: Optional[int] = None,
        search_page_size: int = MAX_SEARCH_PAGE_SIZE,
        included_columns: Optional[Sequence[str]] = None,
        excluded_columns: Sequence[str] = DEFAULT_EXCLUDED_COLUMNS,
    ) -> None:
        self.client = client
        self.jql = jql
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
        self.search_jql = ""
        self.cursor_name: Optional[str] = None
        # issues updated after this time are loaded by the next run
        self.read_until: Optional[datetime] = None
        # first run of an incremental load, nothing loaded before needs to be deleted
        self.initial_load = False
        # write disposition of the issues, set by `make_issues_resource`
        self.write_disposition: TWriteDispositionConfig = "merge"

    def bind(self, incremental: Optional[Incremental[Any]]) -> None:
        """Reads field metadata, binds `incremental` and selects the fields to fetch. Without
        `incremental` the JQL runs as it is
        """
        fields = {field["id"]: field for field in self.client.fields()}
        self.search_jql = self.jql or BOUNDING_JQL
        self.cursor_name = None
        self.read_until = None
        self.initial_load = False
        if incremental is not None:
            cursor = make_cursor_range(
                incremental, fields, to_tzinfo(self.client.time_zone())
            )
            self.search_jql = make_jql(self.jql, cursor)
            self.cursor_name = cursor_name = incremental.get_cursor_column_name()
            # later updates go to the next run, custom fields may hold future values
            if (
                cursor.cursor_type == "timestamp"
                and cursor.last_value_func is max
                and cursor.end_value is None
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
            [self.cursor_name] if self.cursor_name else [],
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
                resource = current_resource()
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

    def search(self, jql: str, max_ids: Optional[int] = None) -> Iterator[List[str]]:
        """Yields pages of ids of issues matching `jql`, at most `max_ids` ids"""
        # search serves 5000 ids per page, but only 100 issues when it also returns a field
        page_size = self.search_page_size
        if max_ids is not None:
            page_size = min(page_size, max_ids)
        listed = 0
        for page in self.client.search(jql, "id", page_size):
            ids = [issue["id"] for issue in page]
            if max_ids is not None:
                ids = ids[: max_ids - listed]
            listed += len(ids)
            yield ids
            if max_ids is not None and listed >= max_ids:
                return

    def convert(self, issues: List[DictStrAny]) -> List[DictStrAny]:
        """Converts `issues` to rows with the selected fields as top level values"""
        invalid_fields: Set[str] = set()
        rows = [self._to_row(issue, invalid_fields) for issue in issues]
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
    """Yields deferred fetches of pages of issues in the range of `incremental`, of all issues
    matching the JQL without it.

    `add_limit` limits the search like a LIMIT clause, so chunked loads with `row_order` load the
    first pages in order. With `max_time` pages are fetched one after another.
    """
    reader.bind(incremental)
    resource = current_resource()
    limit: Optional[LimitItem] = resource.limit if resource is not None else None
    remaining = limit.limit(reader.chunk_size) if limit is not None else None
    in_order = limit is not None and bool(limit.max_time)
    logger.info(f"Reading Jira issues matching JQL: {reader.search_jql}")
    # fetched issues carry the cursor values, the incremental reads them from there
    for ids in batched(
        chain.from_iterable(reader.search(reader.search_jql, remaining)),
        reader.chunk_size,
    ):
        yield issue_rows(reader, ids) if in_order else fetch_issues(reader, ids)


def issue_rows(reader: IssuesReader, ids: List[str]) -> List[DictStrAny]:
    """Fetches a page of issues and converts them to rows"""
    rows = reader.convert(reader.fetch(ids))
    if reader.read_until is None:
        return rows
    cursor = reader.cursor_name
    return [
        row
        for row in rows
        if row.get(cursor) is None or row[cursor] <= reader.read_until
    ]


fetch_issues = dlt.defer(issue_rows)


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

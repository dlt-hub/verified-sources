"""Builds dlt resources that read ServiceNow tables with the Table API or the CSV export"""

import importlib.util
import math
from copy import copy
from datetime import date, datetime  # noqa: I251
from typing import (
    Any,
    Dict,
    Generator,
    Iterator,
    List,
    Literal,
    NamedTuple,
    Optional,
    Sequence,
    Set,
    Tuple,
)

import dlt
from dlt.common import Decimal, logger
from dlt.common.metrics import TDataLocation
from dlt.common.schema.typing import (
    TColumnSchema,
    TTableSchemaColumns,
    TWriteDispositionConfig,
)
from dlt.common.schema.utils import merge_columns
from dlt.common.time import ensure_datetime_in_tz
from dlt.common.typing import TAnyDateTime, TColumnNames, TDataItem
from dlt.extract.incremental import Incremental
from dlt.extract.resource import DltResource, wrap_parallel_iterator
from dlt.sources.helpers.requests import HTTPError
from dlt.sources.helpers.rest_client.auth import APIKeyAuth

from .client import CSV_EXPORT_LIMIT, ServiceNowClient, TRecord, page_keys
from .convert import (
    records_to_arrow,
    records_to_rows,
    strings_to_arrow,
    unescape_formulas,
)
from .query import (
    TableQuery,
    has_literals,
    incremental_query,
    is_hex_key,
    partition_conditions,
    to_glide_literal,
    upper_side_open,
)
from .schema import (
    CREATED_AT_FIELD,
    DEFAULT_LAG,
    DELETED_COLUMN,
    PRIMARY_KEY,
    UPDATED_AT_FIELD,
    DeferredReflect,
    LazyDictionary,
    descendant_tables,
    dictionary_to_columns,
    get_cursor_field,
    get_write_disposition,
    read_dictionary,
    reflect_columns,
    reflect_when_extracting,
    table_chain,
)

TTableBackend = Literal["auto", "python", "pyarrow"]
TReadApi = Literal["table", "csv"]

SOURCE_SECTION = "servicenow"
# resources built here resolve config under the section of the source, not of this module
__source_name__ = SOURCE_SECTION

# longer table lists are not sent: the deleted records of all tables are filtered by table name
MAX_TABLENAMES_LENGTH = 2000
AUDIT_DELETE_FIELDS = ["documentkey", "tablename", "sys_created_on"]
# `sys_created_on` of `sys_audit_delete` is the time of the delete
AUDIT_CREATED_COLUMN: TColumnSchema = {
    "name": CREATED_AT_FIELD,
    "data_type": "timestamp",
}
# python types of the loaded values of cursors, by dlt data type
CURSOR_VALUE_TYPES: Dict[str, Tuple[type, ...]] = {
    "timestamp": (datetime,),
    "date": (date,),
    "bigint": (int,),
    "double": (float, int),
    "decimal": (Decimal, int),
    "text": (str,),
    "bool": (bool,),
}


# cursor that marks the default of `incremental` arguments, no ServiceNow field has this name. The
# default is replaced by the default incremental of the table, `None` loads without incremental
DEFAULT_INCREMENTAL_MARKER = "_dlt_default_incremental_marker"
DEFAULT_INCREMENTAL: Incremental[Any] = dlt.sources.incremental(
    DEFAULT_INCREMENTAL_MARKER
)


def is_default_incremental(incremental: Optional[Incremental[Any]]) -> bool:
    """True if `incremental` is a copy of `DEFAULT_INCREMENTAL`, dlt copies argument defaults"""
    return (
        incremental is not None
        and incremental.cursor_path == DEFAULT_INCREMENTAL_MARKER
    )


def make_default_incremental(
    incremental: Incremental[Any], default: Incremental[Any]
) -> Incremental[Any]:
    """Returns `default` with the values set on `incremental`, the copy of `DEFAULT_INCREMENTAL`
    that dlt merged with config or a partial incremental, e.g. an `end_value`. `default` keeps its
    generic type and its `initial_value`, the `start_date`
    """
    values = incremental.copy()
    # `merge` applies the values that are not None
    for key, value in DEFAULT_INCREMENTAL.items():
        if values[key] == value:
            setattr(values, key, None)
    if default.initial_value is not None:
        values.initial_value = None
    # values from config are strings
    for key in ("initial_value", "end_value"):
        if values[key] is not None:
            setattr(values, key, ensure_datetime_in_tz(values[key]))
    return default.merge(values)


def resolve_backend(backend: TTableBackend) -> Literal["python", "pyarrow"]:
    """Picks pyarrow for "auto" when it is installed, without importing it"""
    if backend == "auto":
        return "pyarrow" if importlib.util.find_spec("pyarrow") else "python"
    return backend


def _primary_key(resource: DltResource) -> List[str]:
    """Returns the primary key set in static resource hints"""
    if resource.has_dynamic_table_name or resource.has_other_dynamic_hints:
        return []
    columns = resource.compute_table_schema()["columns"]
    return [name for name, column in columns.items() if column.get("primary_key")]


def check_cursor_types(
    incremental: Incremental[Any], column: TColumnSchema, table: str
) -> None:
    """Raises if a bound of the range of `incremental` cannot be compared with the loaded cursor
    values. An example is a string `initial_value` of a timestamp cursor.
    """
    data_type = column.get("data_type")
    types = CURSOR_VALUE_TYPES.get(data_type or "")
    if types is None:
        return
    for value in incremental.get_current_range():
        if value is None:
            continue
        if (
            not isinstance(value, types)
            or (data_type == "date" and isinstance(value, datetime))
            or (data_type != "bool" and isinstance(value, bool))
        ):
            raise ValueError(
                f"The incremental of `{table}` has the {type(value).__name__} value"
                f" `{value!r}`, but its cursor `{column['name']}` holds {data_type} values. Set"
                f" `initial_value` and `end_value` to {data_type} values."
            )


def hidden_fields(page: Any, fields: Sequence[str]) -> List[str]:
    """Returns `fields` missing from all records of `page`, a list of records or an Arrow table of
    strings. The Table API returns empty fields as empty strings, missing fields are not readable.
    """
    if isinstance(page, list):
        return [name for name in fields if all(name not in record for record in page)]
    return [
        name
        for name in fields
        if name not in page.column_names or page.column(name).null_count == len(page)
    ]


def check_hidden_fields(
    page: Any, fields: Sequence[str], table: str, cursor: Optional[str]
) -> None:
    """Raises if the `cursor` field is missing from all records of `page`, warns about other
    missing `fields`
    """
    hidden = hidden_fields(page, fields)
    if cursor in hidden:
        # the incremental does not move and reads all records on every run
        raise ValueError(
            f"Cursor field `{cursor}` of `{table}` is not returned, for example because"
            " of a field ACL. Grant read access to this field."
        )
    if hidden:
        logger.warning(
            f"Fields {hidden} of `{table}` are not returned, for example because of"
            " field ACLs. dlt loads them as nulls that replace the values loaded before."
            " Grant read access to these fields, or skip them with `excluded_columns` of"
            " `servicenow_table`."
        )


def deleted_records(
    client: ServiceNowClient,
    table: str,
    conditions: List[str],
    page_size: int,
) -> Iterator[List[TRecord]]:
    """Yields pages of records of `table` deleted when `sys_audit_delete` records match
    `conditions`. Records of tables that extend `table` are included.
    """
    names = [table, *descendant_tables(client.tables(), table)]
    conditions = list(conditions)
    tablenames = f"tablenameIN{','.join(names)}"
    if len(tablenames) <= MAX_TABLENAMES_LENGTH:
        conditions.append(tablenames)
    try:
        for page in client.pages(
            "sys_audit_delete", TableQuery(conditions), AUDIT_DELETE_FIELDS, page_size
        ):
            if any(
                name not in record for record in page for name in AUDIT_DELETE_FIELDS
            ):
                raise ValueError(
                    f"Records of `sys_audit_delete` are returned without {AUDIT_DELETE_FIELDS},"
                    " for example because of field ACLs. Grant read access to these fields, or"
                    " pass `include_deleted=False`."
                )
            yield [
                {
                    PRIMARY_KEY: record["documentkey"],
                    UPDATED_AT_FIELD: record["sys_created_on"],
                    DELETED_COLUMN: "true",
                }
                for record in page
                if record["tablename"] in names
            ]
    except HTTPError as ex:
        if ex.response is not None and ex.response.status_code == 403:
            raise ValueError(
                f"dlt cannot read the deleted records of `{table}` from `sys_audit_delete`."
                " Grant read access to this table, or pass `include_deleted=False`."
            ) from ex
        raise


def _write_disposition(resource: DltResource) -> Optional[str]:
    write_disposition = resource.write_disposition
    if isinstance(write_disposition, dict):
        return write_disposition.get("disposition")
    return write_disposition  # type: ignore[return-value]


def _warn_if_cursor_not_moved(
    table: str,
    column: TColumnSchema,
    started_at: Any,
    last_value: Any,
) -> None:
    """Warns when all records that `add_limit` let a run read have the cursor value that the run
    started from
    """
    if started_at is None or last_value is None:
        return
    if to_glide_literal(last_value, column) == to_glide_literal(started_at, column):
        logger.warning(
            f"The cursor `{column['name']}` of `{table}` did not move past {started_at}:"
            " all records read have this value. Increase `add_limit` or `page_size`."
        )


def column_hints(
    resource: DltResource,
    table: str,
    columns: TTableSchemaColumns,
    hints: TTableSchemaColumns,
) -> TTableSchemaColumns:
    """Returns the hints of `columns` in their order, with `hints` applied by the user in place of
    the reflected ones
    """
    unmatched = [name for name in hints if name not in columns]
    if unmatched:
        logger.warning(
            f"Resource columns {unmatched} of `{table}` do not match any field, and dlt ignores"
            " them. Use the ServiceNow field names in `apply_hints`."
        )
    columns_hints = {name: hints.get(name, columns[name]) for name in columns}
    # the primary key hint makes the key not nullable, arrow tables must agree with it
    for name in _primary_key(resource):
        if name in columns_hints:
            key_column = copy(columns_hints[name])
            key_column["nullable"] = False
            columns_hints[name] = key_column
    return columns_hints


def plan_partitions(
    client: ServiceNowClient,
    table: str,
    query: TableQuery,
    first_page: Any,
    page_size: int,
    max_partitions: int,
) -> List[List[str]]:
    """Returns the conditions of `sys_id` ranges that split the records after `first_page`, a list
    of records or an Arrow table, to read them in parallel. Returns none when the records fit in a
    page or cannot be split.
    """
    # ACLs hide records after the limit is applied, so a full page can come back shorter
    if len(first_page) * 2 <= page_size:
        return []
    after = page_keys(first_page, ["sys_id"])[-1][0]
    if not after or not is_hex_key(after):
        return []
    remaining = client.count(table, query.page_query([after]))
    if remaining is None or remaining <= page_size:
        return []
    ranges = partition_conditions(
        after, min(math.ceil(remaining / page_size), max_partitions)
    )
    if len(ranges) < 2:
        return []
    logger.info(
        f"dlt reads {remaining} records of `{table}` in {len(ranges)} `sys_id` ranges in"
        " parallel"
    )
    return ranges


class IncrementalPlan(NamedTuple):
    """Conditions and order of the records in the current range of an incremental, and the
    conditions of the records deleted in it
    """

    cursor: Optional[str] = None
    conditions: Sequence[str] = ()
    order: Optional[Literal["asc", "desc"]] = None
    delete_conditions: Optional[List[str]] = None


def plan_incremental(
    client: ServiceNowClient,
    table: str,
    resource: DltResource,
    incremental: Incremental[Any],
    columns: TTableSchemaColumns,
    durations: Set[str],
    include_deleted: bool,
) -> IncrementalPlan:
    """Returns the queries of the current range of `incremental` on `columns` of `table`. Raises if
    the instance cannot read the cursor in the order or in the chunks that `resource` asks for.

    With `include_deleted`, the records deleted in the range of a `max` incremental on
    `sys_updated_on` are read too. The resource must merge and have the hard delete column.
    """
    cursor = incremental.get_cursor_column_name()
    if cursor is None or cursor not in columns:
        raise ValueError(
            f"Cursor field `{incremental.cursor_path}` is not a field of table `{table}`"
        )
    # the conversion follows the hints, so do the literals
    cursor_column = columns[cursor]
    is_duration = cursor in durations
    check_cursor_types(incremental, cursor_column, table)
    upper_bound = None
    if not has_literals(cursor_column, is_duration):
        if incremental.row_order or resource.limit:
            raise ValueError(
                f"The instance cannot compare `{cursor}` values of `{table}` in a query, so"
                " every run reads all records. `row_order` and `add_limit` cannot read the"
                " table in chunks."
            )
        logger.warning(
            f"The instance cannot compare `{cursor}` values of `{table}` in a query. dlt"
            " reads all records, and the incremental filters them."
        )
    elif cursor_column.get("data_type") == "text" and incremental.row_order:
        raise ValueError(
            f"The instance orders text case-insensitively, so dlt cannot read `{table}` in"
            f" the order of the text cursor `{cursor}`. Remove `row_order` from the"
            " incremental."
        )
    elif cursor_column.get("data_type") == "timestamp" and upper_side_open(incremental):
        # records changed while reading get a later timestamp and are read on the next run,
        # so pages and `sys_id` ranges select the same records
        upper_bound = client.server_time(table)
    conditions, order = incremental_query(
        incremental, cursor_column, is_duration, upper_bound
    )
    delete_conditions = None
    # deletes are read for the records changed since the last run, a range going back has none
    if (
        include_deleted
        and incremental.last_value_func is max
        and cursor == UPDATED_AT_FIELD
        and _write_disposition(resource) == "merge"
        and columns.get(DELETED_COLUMN, {}).get("hard_delete")
        and incremental.get_current_range()[0] is not None
    ):
        delete_conditions, _ = incremental_query(
            incremental, AUDIT_CREATED_COLUMN, upper_bound=upper_bound
        )
        if resource.limit:
            logger.warning(
                f"dlt reads the records deleted from `{table}` after all pages, so a run"
                " that `add_limit` stops reads no deleted records."
            )
    if resource.limit and incremental.row_order is None:
        logger.warning(
            f"dlt reads `{table}` with `add_limit` in the order of `sys_id`. The next run"
            f" starts after the max `{cursor}`, so dlt can skip records. Set"
            " `row_order='asc'` on the incremental to read the records ordered by the"
            " cursor."
        )
    return IncrementalPlan(cursor, conditions, order, delete_conditions)


class PageReader(NamedTuple):
    """Reads pages of `fields` of `table` with the Table API or the CSV export, and converts them to
    lists of rows or Arrow tables with the types of `columns`
    """

    client: ServiceNowClient
    table: str
    fields: List[str]
    columns: TTableSchemaColumns
    durations: Set[str]
    page_size: int
    api: TReadApi
    backend: Literal["python", "pyarrow"]

    def raw_pages(self, query: TableQuery) -> Iterator[Any]:
        """Yields pages of records as returned by the instance"""
        if self.backend == "python":
            return self.client.pages(self.table, query, self.fields, self.page_size)
        read = self.client.csv_pages if self.api == "csv" else self.client.arrow_pages
        return read(self.table, query, self.fields, self.page_size)

    def convert(self, page: Any) -> TDataItem:
        if self.backend == "python":
            return records_to_rows(page, self.columns, self.durations)
        if self.api == "csv":
            page = unescape_formulas(page, self.columns)
        return strings_to_arrow(page, self.columns, self.durations)

    def pages(self, query: TableQuery) -> Generator[TDataItem, None, None]:
        for page in self.raw_pages(query):
            yield self.convert(page)

    def convert_records(self, records: List[TRecord]) -> TDataItem:
        """Converts records built by dlt, like the deleted records"""
        convert = records_to_arrow if self.backend == "pyarrow" else records_to_rows
        return convert(records, self.columns, self.durations)


def read_table(
    reader: PageReader,
    plan: IncrementalPlan,
    encoded_query: Optional[str],
    max_partitions: int,
) -> Iterator[TDataItem]:
    """Yields the pages of the records that `plan` and `encoded_query` select.

    With `max_partitions`, the records after a full first page are split into up to
    `max_partitions` `sys_id` ranges. Each range is yielded as an iterator of pages that dlt reads
    in its worker pool.
    """
    query = TableQuery(
        plan.conditions,
        encoded_query,
        plan.cursor if plan.order else None,
        descending=plan.order == "desc",
    )
    pages = reader.raw_pages(query)
    first_page = next(pages, None)
    if first_page is None:
        return
    check_hidden_fields(first_page, reader.fields, reader.table, plan.cursor)
    first_item = reader.convert(first_page)
    yield first_item
    ranges = (
        plan_partitions(
            reader.client,
            reader.table,
            query,
            first_item,
            reader.page_size,
            max_partitions,
        )
        if max_partitions and query.keys == ["sys_id"]
        else []
    )
    if not ranges:
        for page in pages:
            yield reader.convert(page)
        return
    for range_conditions in ranges:
        range_query = TableQuery([*plan.conditions, *range_conditions], encoded_query)
        # dlt reads iterators yielded by a resource sequentially unless wrapped
        yield wrap_parallel_iterator(reader.pages(range_query))


def table_rows(
    client: ServiceNowClient,
    table: str,
    columns: TTableSchemaColumns,
    durations: Set[str],
    encoded_query: Optional[str],
    page_size: int,
    api: TReadApi,
    backend: Literal["python", "pyarrow"],
    include_deleted: bool,
    allow_partitioned_load: bool,
    max_partitions: int,
    incremental: Optional[Incremental[Any]] = None,
    deferred: Optional[DeferredReflect] = None,
) -> Iterator[TDataItem]:
    """Reads `columns` of `table` and yields pages as Arrow tables or lists of dicts.

    With `deferred`, `columns` are empty and the fields of the table are read first.

    With `include_deleted`, the records deleted in the current range of the incremental follow the
    pages, see `plan_incremental`. With `allow_partitioned_load`, the records are read in parallel
    `sys_id` ranges, see `read_table`.
    """
    resource = dlt.current.resource()
    resource.add_input(
        TDataLocation(
            kind="servicenow", resource_name=resource.name, location=client.base_url
        ),
        replace=True,
    )
    if deferred is not None:
        columns, durations = reflect_when_extracting(
            client,
            table,
            incremental.get_cursor_column_name() if incremental else None,
            include_deleted
            and incremental is not None
            and _write_disposition(resource) == "merge",
            deferred,
        )
        hints = {name: copy(column) for name, column in columns.items()}
        if resource.columns and not callable(resource.columns):
            hints = merge_columns(hints, resource.columns)
        # the empty item sets the hints before the incremental and the schema get records
        yield dlt.mark.with_hints([], dlt.mark.make_hints(columns=hints))
    elif callable(resource.columns):
        hints = columns
    else:
        # resource hints include hints applied by the user
        hints = resource.columns
    columns_hints = column_hints(resource, table, columns, hints)
    plan = (
        plan_incremental(
            client,
            table,
            resource,
            incremental,
            columns_hints,
            durations,
            include_deleted,
        )
        if incremental is not None
        else IncrementalPlan()
    )
    fields = [name for name in columns if name != DELETED_COLUMN]
    reader = PageReader(
        client, table, fields, columns_hints, durations, page_size, api, backend
    )
    # with `row_order` and `add_limit`, a run that reads only records with the cursor value it
    # started from does not move the cursor
    tracks_cursor = bool(
        incremental and plan.order and resource.limit and plan.conditions
    )
    started_at = incremental.last_value if tracks_cursor and incremental else None
    # ranges break the order of the cursor and of the pages that `add_limit` reads
    partitions = max_partitions if allow_partitioned_load and not resource.limit else 0
    try:
        yield from read_table(reader, plan, encoded_query, partitions)
        if plan.delete_conditions is not None:
            for deleted in deleted_records(
                client, table, plan.delete_conditions, page_size
            ):
                if deleted:
                    yield reader.convert_records(deleted)
    except GeneratorExit:
        # `add_limit` closes the generator after the incremental processed the last page
        if tracks_cursor and incremental and plan.cursor:
            _warn_if_cursor_not_moved(
                table, columns_hints[plan.cursor], started_at, incremental.last_value
            )
        raise


def table_resource(
    client: ServiceNowClient,
    table: str,
    dictionary: Optional[List[TRecord]] = None,
    name: Optional[str] = None,
    start_date: Optional[TAnyDateTime] = None,
    incremental: Optional[Incremental[Any]] = DEFAULT_INCREMENTAL,
    lag: Optional[float] = None,
    encoded_query: Optional[str] = None,
    include_deleted: bool = True,
    api: TReadApi = "table",
    backend: TTableBackend = "auto",
    page_size: int = 10000,
    allow_partitioned_load: bool = True,
    max_partitions: int = 10,
    included_columns: Optional[List[str]] = None,
    excluded_columns: Optional[List[str]] = None,
    write_disposition: Optional[TWriteDispositionConfig] = None,
    primary_key: TColumnNames = None,
    merge_key: TColumnNames = None,
    parallelized: bool = False,
    defer_table_reflect: bool = False,
    lazy_dictionary: Optional[LazyDictionary] = None,
) -> DltResource:
    """Creates a resource reading `table` with columns taken from `sys_dictionary`.

    `dictionary` holds the fields of `table` and the tables it extends, read if not passed. With
    `defer_table_reflect`, the fields are read when extracting, from `lazy_dictionary` if passed.
    See `servicenow_table` for the other arguments.
    """
    table_backend = resolve_backend(backend)
    if api == "csv" and table_backend == "python":
        raise ValueError(
            f"dlt reads the CSV export of `{table}` with `pyarrow`. Install `pyarrow`. Then set"
            " `backend` to 'pyarrow' or 'auto'."
        )
    if api == "csv" and isinstance(client.auth, APIKeyAuth):
        raise ValueError(
            f"The CSV export of `{table}` does not accept API keys. Use basic or OAuth"
            " credentials, or set `api='table'`."
        )
    if api == "csv" and page_size > CSV_EXPORT_LIMIT:
        logger.warning(
            f"The CSV export returns at most {CSV_EXPORT_LIMIT} records. dlt reads `{table}` in"
            f" pages of {CSV_EXPORT_LIMIT} records instead of {page_size}."
        )
        page_size = CSV_EXPORT_LIMIT
    primary_keys = (
        [primary_key]
        if isinstance(primary_key, str)
        else list(primary_key or [PRIMARY_KEY])
    )
    merge_keys = [merge_key] if isinstance(merge_key, str) else list(merge_key or [])
    # records are read in the order of `sys_id`, the keys are loaded with `included_columns`
    required_columns = list(dict.fromkeys([PRIMARY_KEY, *primary_keys, *merge_keys]))
    chain: List[str] = []
    if not defer_table_reflect:
        if dictionary is None:
            dictionary = read_dictionary(client, [table])
        chain = table_chain(client.tables(), table)

    is_default = is_default_incremental(incremental)
    cursor_field: Optional[str] = None
    if is_default:
        if write_disposition == "replace" and start_date is None:
            # the "replace" shorthand resets incremental state in dlt, the full table is reloaded
            cursor_field = None
        elif defer_table_reflect:
            # nearly all tables have it, checked when the fields are read
            cursor_field = UPDATED_AT_FIELD
        else:
            cursor_field = get_cursor_field(
                dictionary_to_columns(dictionary or [], chain).columns
            )
            if cursor_field is None and start_date is not None:
                raise ValueError(
                    f"Table `{table}` has no `{UPDATED_AT_FIELD}` or `{CREATED_AT_FIELD}` field"
                    " to load records changed since `start_date`. Pass an explicit incremental."
                )
    elif incremental is not None:
        cursor_field = incremental.get_cursor_column_name()
        if cursor_field is None:
            raise ValueError(
                f"Cursor path `{incremental.cursor_path}` of `{table}` must be a field name, for"
                f" example `{UPDATED_AT_FIELD}`."
            )
    write_disposition = write_disposition or get_write_disposition(cursor_field)
    disposition = (
        write_disposition["disposition"]
        if isinstance(write_disposition, dict)
        else write_disposition
    )

    table_incremental = incremental
    if is_default:
        if lag is None and disposition == "merge":
            lag = DEFAULT_LAG
        table_incremental = (
            make_default_incremental(
                incremental,
                dlt.sources.incremental[datetime](
                    cursor_field,
                    initial_value=(
                        None
                        if start_date is None
                        else ensure_datetime_in_tz(start_date)
                    ),
                    lag=lag,
                    # records imported without timestamps are read on the first run
                    on_cursor_value_missing="include",
                    # timestamps have second precision, so many records can share the last
                    # cursor value. merge deduplicates them, and the state keeps no hashes
                    primary_key=() if disposition == "merge" else None,
                ),
            )
            if cursor_field
            else None
        )

    deferred: Optional[DeferredReflect] = None
    durations: Set[str] = set()
    if defer_table_reflect:
        columns: TTableSchemaColumns = {}
        deferred = DeferredReflect(
            lazy_dictionary or LazyDictionary(client, [table]),
            included_columns,
            excluded_columns,
            required_columns,
            default_cursor=is_default,
        )
    else:
        # records deleted in ServiceNow are deleted from the destination, `table_rows` reads them
        # for the incremental that dlt passes
        columns, durations = reflect_columns(
            table,
            dictionary or [],
            chain,
            included_columns,
            excluded_columns,
            required_columns,
            cursor_field,
            include_deleted
            and disposition == "merge"
            and table_incremental is not None,
        )

    resource: DltResource = dlt.resource(
        table_rows,
        name=name or table,
        write_disposition=write_disposition,
        merge_key=merge_key,
        primary_key=primary_keys,
        columns=columns or None,
    )(
        client,
        table,
        columns,
        durations,
        encoded_query,
        page_size,
        api,
        table_backend,
        include_deleted,
        allow_partitioned_load,
        max_partitions,
        incremental=table_incremental,
        deferred=deferred,
    )
    if parallelized:
        # parallelize the bound resource, the decorator flag does not survive binding the arguments
        resource.parallelize()
    return resource

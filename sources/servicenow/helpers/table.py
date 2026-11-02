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
    to_datetime,
    unescape_formulas,
)
from .query import (
    CursorRange,
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


def cursor_bounds(
    item: Any, cursor: str, casefold: bool = False
) -> Optional[Tuple[Any, Any]]:
    """Returns the lowest and the highest `cursor` value of `item`, a list of rows or an Arrow
    table, None if all values are empty. With `casefold`, text is compared in lower case.
    """
    if isinstance(item, list):
        values = [row.get(cursor) for row in item]
        # values that the conversion failed to type stay strings
        values = [
            value
            for value in values
            if value is not None and (casefold or not isinstance(value, str))
        ]
        if not values:
            return None
        if casefold:
            values = [value.lower() for value in values]
        return min(values), max(values)
    if cursor not in item.column_names:
        return None
    from pyarrow import compute as pc

    column = item.column(cursor)
    if casefold:
        column = pc.utf8_lower(column)
    bounds = pc.min_max(column).as_py()
    if bounds["min"] is None:
        return None
    return bounds["min"], bounds["max"]


def check_cursor_range(item: Any, table: str, cursor_range: CursorRange) -> None:
    """Raises if rows of `item`, a list of rows or an Arrow table, have cursor values outside of
    `cursor_range`
    """
    cursor, lowest, highest, casefold = cursor_range
    bounds = cursor_bounds(item, cursor, casefold)
    if bounds is None:
        return
    if casefold:
        lowest = lowest.lower() if lowest is not None else None
        highest = highest.lower() if highest is not None else None
    if (lowest is not None and bounds[0] < lowest) or (
        highest is not None and bounds[1] > highest
    ):
        # the instance ignores conditions on fields without `query_range` access
        raise ValueError(
            f"Records of `{table}` have `{cursor}` values from {bounds[0]} to {bounds[1]},"
            f" outside of the requested range from {lowest} to {highest}. The instance ignored"
            f" a part of the query. Grant `query_range` access to `{cursor}`."
        )


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


def deleted_records(
    client: ServiceNowClient,
    table: str,
    conditions: List[str],
    page_size: int,
    cursor_range: Optional[CursorRange] = None,
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
            if cursor_range:
                check_cursor_range(
                    [
                        {CREATED_AT_FIELD: to_datetime(record["sys_created_on"])}
                        for record in page
                        if record["sys_created_on"]
                    ],
                    "sys_audit_delete",
                    cursor_range,
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
    last_read: Any,
    rows: int,
) -> None:
    """Warns when all records that `add_limit` let a run read have the cursor value that the run
    started from
    """
    if started_at is None or last_read is None:
        return
    if to_glide_literal(last_read, column) == to_glide_literal(started_at, column):
        logger.warning(
            f"The cursor `{column['name']}` of `{table}` did not move past {started_at}:"
            f" all {rows} records read have this value. Increase `add_limit` or `page_size`."
        )


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

    With `include_deleted`, the records deleted in the current range of a `max` incremental on
    `sys_updated_on` follow the pages. The resource must merge and have the hard delete column.

    With `allow_partitioned_load`, the records after a full first page are split into up to
    `max_partitions` `sys_id` ranges. Each range is yielded as an iterator of pages that dlt reads
    in its worker pool.
    """
    resource = dlt.current.resource()
    resource.add_input(
        TDataLocation(
            kind="servicenow", resource_name=resource.name, location=client.base_url
        ),
        replace=True,
    )
    cursor = incremental.get_cursor_column_name() if incremental else None
    if deferred is not None:
        columns, durations = reflect_when_extracting(
            client,
            table,
            cursor,
            include_deleted
            and incremental is not None
            and _write_disposition(resource) == "merge",
            deferred,
        )
        hints_columns = {name: copy(column) for name, column in columns.items()}
        if resource.columns and not callable(resource.columns):
            hints_columns = merge_columns(hints_columns, resource.columns)
        # the empty item sets the hints before the incremental and the schema get records
        yield dlt.mark.with_hints([], dlt.mark.make_hints(columns=hints_columns))
        columns_hints = hints_columns
    elif callable(resource.columns):
        columns_hints = columns
    else:
        # resource hints include hints applied by the user
        columns_hints = resource.columns
    unmatched = [name for name in columns_hints if name not in columns]
    if unmatched:
        logger.warning(
            f"Resource columns {unmatched} of `{table}` do not match any field, and dlt ignores"
            " them. Use the ServiceNow field names in `apply_hints`."
        )
    columns_hints = {name: columns_hints.get(name, columns[name]) for name in columns}
    # the primary key hint makes the key not nullable, arrow tables must agree with it
    for name in _primary_key(resource):
        if name in columns_hints:
            key_column = copy(columns_hints[name])
            key_column["nullable"] = False
            columns_hints[name] = key_column

    conditions: List[str] = []
    delete_conditions: Optional[List[str]] = None
    cursor_range: Optional[CursorRange] = None
    delete_range: Optional[CursorRange] = None
    order: Optional[str] = None
    if incremental is not None:
        if cursor is None or cursor not in columns:
            raise ValueError(
                f"Cursor field `{incremental.cursor_path}` is not a field of table `{table}`"
            )
        # the conversion follows the hints, so do the literals and the range check
        cursor_column = columns_hints[cursor]
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
        elif cursor_column.get("data_type") == "timestamp" and upper_side_open(
            incremental
        ):
            # records changed while reading get a later timestamp and are read on the next run,
            # so pages and `sys_id` ranges select the same records
            upper_bound = client.server_time(table)
        conditions, cursor_range, order = incremental_query(
            incremental, cursor_column, is_duration, upper_bound
        )
        # deletes are read for the records changed since the last run, a range going back has none
        if (
            include_deleted
            and incremental.last_value_func is max
            and cursor == UPDATED_AT_FIELD
            and _write_disposition(resource) == "merge"
            and columns_hints.get(DELETED_COLUMN, {}).get("hard_delete")
            and incremental.get_current_range()[0] is not None
        ):
            delete_conditions, delete_range, _ = incremental_query(
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
    query = TableQuery(
        conditions, encoded_query, cursor if order else None, descending=order == "desc"
    )
    fields = [name for name in columns if name != DELETED_COLUMN]

    def read_pages(page_query: TableQuery) -> Iterator[Any]:
        if backend == "pyarrow":
            return (client.csv_pages if api == "csv" else client.arrow_pages)(
                table, page_query, fields, page_size
            )
        return client.pages(table, page_query, fields, page_size)

    checked_fields = False

    def convert_page(page: Any) -> TDataItem:
        nonlocal checked_fields
        if not checked_fields:
            checked_fields = True
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
                    " Grant read access to these fields, or skip them with `excluded_columns`."
                )
        if backend == "python":
            item = records_to_rows(page, columns_hints, durations)
        elif api == "csv":
            item = strings_to_arrow(
                unescape_formulas(page, columns_hints), columns_hints, durations
            )
        else:
            item = strings_to_arrow(page, columns_hints, durations)
        if cursor_range is not None:
            check_cursor_range(item, table, cursor_range)
        return item

    def convert_pages(page_query: TableQuery) -> Generator[TDataItem, None, None]:
        for page in read_pages(page_query):
            yield convert_page(page)

    # with `row_order` and `add_limit`, a run that reads only records with the cursor value it
    # started from does not move the cursor
    tracks_cursor = bool(incremental and order and resource.limit and cursor_range)
    started_at = incremental.last_value if tracks_cursor and incremental else None
    last_read: Any = None
    rows_read = 0

    def track(item: TDataItem) -> TDataItem:
        nonlocal last_read, rows_read
        if tracks_cursor and incremental and cursor:
            rows_read += len(item)
            bounds = cursor_bounds(item, cursor)
            if bounds is not None:
                values = [last_read] if last_read is not None else []
                last_read = incremental.last_value_func([*values, *bounds])
        return item

    try:
        pages = convert_pages(query)
        first_page = next(pages, None)
        if first_page is not None:
            yield track(first_page)
            # ranges break the order of the cursor and of the pages that `add_limit` reads
            ranges = (
                plan_partitions(
                    client, table, query, first_page, page_size, max_partitions
                )
                if allow_partitioned_load
                and query.keys == ["sys_id"]
                and not resource.limit
                else []
            )
            if ranges:
                for range_conditions in ranges:
                    range_query = TableQuery(
                        [*conditions, *range_conditions], encoded_query
                    )
                    # dlt reads iterators yielded by a resource sequentially unless wrapped
                    yield wrap_parallel_iterator(convert_pages(range_query))
            else:
                for page in pages:
                    yield track(page)
        if delete_conditions is not None:
            convert = records_to_arrow if backend == "pyarrow" else records_to_rows
            for deleted in deleted_records(
                client, table, delete_conditions, page_size, delete_range
            ):
                if deleted:
                    yield convert(deleted, columns_hints, durations)
    except GeneratorExit:
        # `add_limit` closes the generator after the last page
        if tracks_cursor and cursor:
            _warn_if_cursor_not_moved(
                table, columns_hints[cursor], started_at, last_read, rows_read
            )
        raise


def table_resource(
    client: ServiceNowClient,
    table: str,
    dictionary: Optional[List[TRecord]] = None,
    name: Optional[str] = None,
    start_date: Optional[TAnyDateTime] = None,
    incremental: Optional[Incremental[Any]] = None,
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

    if incremental is None:
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
    else:
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

    table_incremental: Optional[Incremental[Any]]
    if incremental is None:
        if lag is None and disposition == "merge":
            lag = DEFAULT_LAG
        table_incremental = (
            dlt.sources.incremental(
                cursor_field,
                initial_value=(
                    None if start_date is None else ensure_datetime_in_tz(start_date)
                ),
                lag=lag,
                # records imported without timestamps are read on the first run
                on_cursor_value_missing="include",
                # timestamps have second precision, so many records can share the last cursor
                # value. merge deduplicates them, and the state keeps no hashes of the records
                primary_key=() if disposition == "merge" else None,
            )
            if cursor_field
            else None
        )
    else:
        table_incremental = incremental

    deferred: Optional[DeferredReflect] = None
    durations: Set[str] = set()
    if defer_table_reflect:
        columns: TTableSchemaColumns = {}
        deferred = DeferredReflect(
            lazy_dictionary or LazyDictionary(client, [table]),
            included_columns,
            excluded_columns,
            required_columns,
            default_cursor=incremental is None,
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

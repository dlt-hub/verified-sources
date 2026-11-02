"""Builds dlt resources that read ServiceNow tables with the Table API or the CSV export"""

import importlib.util
import math
from copy import copy
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
from dlt.common import logger
from dlt.common.schema.typing import TTableSchemaColumns, TWriteDispositionConfig
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
    incremental_conditions,
    incremental_range,
    is_hex_key,
    partition_conditions,
)
from .schema import (
    CREATED_AT_FIELD,
    DEFAULT_LAG,
    DELETED_COLUMN,
    PRIMARY_KEY,
    UPDATED_AT_FIELD,
    descendant_tables,
    dictionary_to_columns,
    get_cursor_field,
    get_write_disposition,
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


def resolve_backend(backend: TTableBackend) -> Literal["python", "pyarrow"]:
    """Picks pyarrow for "auto" when it is installed, without importing it"""
    if backend == "auto":
        return "pyarrow" if importlib.util.find_spec("pyarrow") else "python"
    return backend


def read_dictionary(client: ServiceNowClient, tables: List[str]) -> List[TRecord]:
    """Reads the `sys_dictionary` fields of `tables` and of the tables they extend"""
    all_tables = client.tables()
    names = {name for table in tables for name in table_chain(all_tables, table)}
    return client.dictionary(sorted(names))


def _is_time_cursor(columns: TTableSchemaColumns, cursor_field: Optional[str]) -> bool:
    return columns.get(cursor_field or "", {}).get("data_type") in ("timestamp", "date")


def _primary_key(resource: DltResource) -> List[str]:
    """Returns the primary key set in static resource hints"""
    if resource.has_dynamic_table_name or resource.has_other_dynamic_hints:
        return []
    columns = resource.compute_table_schema()["columns"]
    return [name for name, column in columns.items() if column.get("primary_key")]


def check_cursor_range(
    page: Any, table: str, cursor: str, cursor_range: Tuple[Optional[str], str]
) -> None:
    """Raises if records of `page`, a list of records or an Arrow table of strings, have `cursor`
    values outside of `cursor_range`
    """
    if isinstance(page, list):
        values = [record.get(cursor) for record in page]
    else:
        values = page.column(cursor).to_pylist()
    values = [value for value in values if value]
    if not values:
        return
    first, last = cursor_range
    if (first is not None and min(values) < first) or max(values) > last:
        # the instance ignores conditions on fields without `query_range` access
        raise ValueError(
            f"Records of `{table}` have `{cursor}` values from {min(values)} to {max(values)},"
            f" outside of the requested range from {first} to {last}. The instance ignored a"
            f" part of the query. Grant `query_range` access to `{cursor}`."
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
    cursor_range: Optional[Tuple[Optional[str], str]] = None,
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
                    page, "sys_audit_delete", CREATED_AT_FIELD, cursor_range
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
) -> Iterator[TDataItem]:
    """Reads `columns` of `table` and yields pages as Arrow tables or lists of dicts.

    With `include_deleted`, the records deleted in the current range of a `max` incremental on
    `sys_updated_on` follow the pages. The resource must merge and have the hard delete column.

    With `allow_partitioned_load`, the records after a full first page are split into up to
    `max_partitions` `sys_id` ranges. Each range is yielded as an iterator of pages that dlt reads
    in its worker pool.
    """
    resource = dlt.current.resource()
    # resource hints include hints applied by the user
    columns_hints = columns if callable(resource.columns) else resource.columns
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

    cursor = incremental.get_cursor_column_name() if incremental else None
    conditions: List[str] = []
    delete_conditions: Optional[List[str]] = None
    cursor_range: Optional[Tuple[Optional[str], str]] = None
    if incremental is not None:
        if cursor not in columns:
            raise ValueError(
                f"Cursor field `{cursor}` is not a field of table `{table}`"
            )
        # records changed while reading have a later timestamp and are read on the next run
        window_end = client.server_time(table)
        # the range is taken before records are read: the incremental then ends it at the
        # max cursor value
        conditions = incremental_conditions(incremental, cursor, window_end)
        cursor_range = incremental_range(incremental, window_end)
        # deletes are read for the records changed since the last run, a range going back has none
        if (
            include_deleted
            and incremental.last_value_func is max
            and cursor == UPDATED_AT_FIELD
            and _write_disposition(resource) == "merge"
            and columns_hints.get(DELETED_COLUMN, {}).get("hard_delete")
            and incremental.get_current_range()[0] is not None
        ):
            delete_conditions = incremental_conditions(
                incremental, CREATED_AT_FIELD, window_end
            )
        if resource.limit and incremental.row_order is None:
            logger.warning(
                f"dlt reads `{table}` with `add_limit` in the order of `sys_id`. The next run"
                f" starts after the max `{cursor}`, so dlt can skip records. Set"
                " `row_order='asc'` on the incremental to read the records ordered by the"
                " cursor."
            )
    order_cursor = cursor if incremental and incremental.row_order else None
    query = TableQuery(
        conditions,
        encoded_query,
        order_cursor,
        # `row_order` follows `last_value_func`: "asc" with `min` reads the newest first
        descending=bool(order_cursor)
        and (incremental.row_order == "asc") == (incremental.last_value_func is min),
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
        if cursor and cursor_range:
            check_cursor_range(page, table, cursor, cursor_range)
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
            return records_to_rows(page, columns_hints, durations)
        if api == "csv":
            return strings_to_arrow(
                unescape_formulas(page, columns_hints), columns_hints, durations
            )
        return strings_to_arrow(page, columns_hints, durations)

    def convert_pages(page_query: TableQuery) -> Generator[TDataItem, None, None]:
        for page in read_pages(page_query):
            yield convert_page(page)

    pages = convert_pages(query)
    first_page = next(pages, None)
    if first_page is not None:
        yield first_page
        # ranges break the order of the cursor and of the pages that `add_limit` reads
        ranges = (
            plan_partitions(client, table, query, first_page, page_size, max_partitions)
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
            yield from pages
    if delete_conditions is not None:
        convert = records_to_arrow if backend == "pyarrow" else records_to_rows
        for page in deleted_records(
            client, table, delete_conditions, page_size, cursor_range
        ):
            if page:
                yield convert(page, columns_hints, durations)


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
) -> DltResource:
    """Creates a resource reading `table` with columns taken from `sys_dictionary`.

    `dictionary` holds the fields of `table` and the tables it extends, read if not passed.
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
    if dictionary is None:
        dictionary = read_dictionary(client, [table])
    chain = table_chain(client.tables(), table)
    keys = {
        "primary_key": [primary_key]
        if isinstance(primary_key, str)
        else list(primary_key or [PRIMARY_KEY]),
        "merge_key": [merge_key]
        if isinstance(merge_key, str)
        else list(merge_key or []),
    }

    if incremental is None:
        if write_disposition == "replace" and start_date is None:
            # the "replace" shorthand resets incremental state in dlt, the full table is reloaded
            cursor_field = None
        else:
            cursor_field = get_cursor_field(
                dictionary_to_columns(dictionary, chain).columns
            )
            if cursor_field is None and start_date is not None:
                raise ValueError(
                    f"Table `{table}` has no `{UPDATED_AT_FIELD}` or `{CREATED_AT_FIELD}` field"
                    " to load records changed since `start_date`. Pass an explicit incremental."
                )
    else:
        cursor_field = incremental.get_cursor_column_name()
    if included_columns:
        # records are read in the order of `sys_id`, the cursor and the keys are always loaded
        required = [PRIMARY_KEY, *keys["primary_key"], *keys["merge_key"]]
        included_columns = list(
            dict.fromkeys([*included_columns, *required, *filter(None, [cursor_field])])
        )
    columns, durations = dictionary_to_columns(
        dictionary, chain, included_columns, excluded_columns
    )
    if PRIMARY_KEY not in columns:
        raise ValueError(
            f"dlt cannot read table `{table}` because it has no `{PRIMARY_KEY}` field."
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
            )
            if cursor_field
            else None
        )
    else:
        table_incremental = incremental
    # timestamps have second precision, so many records can share the last cursor value. merge
    # deduplicates them, and the state keeps no hashes of the records at the last timestamp
    if (
        incremental is None
        and table_incremental is not None
        and table_incremental.primary_key is None
        and disposition == "merge"
        and _is_time_cursor(columns, cursor_field)
    ):
        table_incremental = table_incremental.copy()
        table_incremental.primary_key = ()

    # records deleted in ServiceNow are deleted from the destination, the latest change wins.
    # `table_rows` reads them for the incremental that dlt passes
    if (
        include_deleted
        and disposition == "merge"
        and table_incremental is not None
        and cursor_field == UPDATED_AT_FIELD
        and cursor_field in columns
    ):
        columns[DELETED_COLUMN] = {
            "name": DELETED_COLUMN,
            "data_type": "bool",
            "hard_delete": True,
        }
        columns[UPDATED_AT_FIELD] = {**columns[UPDATED_AT_FIELD], "dedup_sort": "desc"}
    columns[PRIMARY_KEY] = {**columns[PRIMARY_KEY], "nullable": False}

    resource: DltResource = dlt.resource(
        table_rows,
        name=name or table,
        write_disposition=write_disposition,
        merge_key=merge_key,
        primary_key=keys["primary_key"],
        columns=columns,
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
    )
    if parallelized:
        # parallelize the bound resource, the decorator flag does not survive binding the arguments
        resource.parallelize()
    return resource

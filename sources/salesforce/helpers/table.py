"""Builds dlt resources reading sObjects with SOQL queries"""

import importlib.util
from copy import copy
from typing import (
    Any,
    Dict,
    Iterator,
    List,
    Literal,
    NamedTuple,
    Optional,
    Tuple,
    Union,
)

import dlt
from dlt.common import logger
from dlt.common.configuration.specs import BaseConfiguration, configspec
from dlt.common.metrics import TDataLocation
from dlt.common.schema.typing import (
    TTableSchemaColumns,
    TWriteDispositionConfig,
    TWriteDispositionDict,
)
from dlt.common.schema.utils import merge_columns
from dlt.common.typing import TColumnNames, TDataItem
from dlt.extract.incremental import Incremental
from dlt.extract.resource import DltResource

from .connection import SalesforceConnection
from .credentials import SalesforceAuth
from .csv_utils import arrow_column_max, csv_page_to_arrow, csv_page_to_rows
from .query import QueryApi, TQueryApi, make_query_api
from .schema import (
    DEFAULT_LAG,
    UPDATED_AT_FIELDS,
    fields_to_columns,
    get_cursor_field,
    get_primary_key,
    get_write_disposition,
)
from .soql import make_soql, to_soql_literal

TTableBackend = Literal["auto", "python", "pyarrow"]
TIncrementalArg = Union[Literal["auto"], Incremental[Any], None]


@configspec
class SalesforceTableResourceConfiguration(BaseConfiguration):
    # `incremental` is not configurable: dlt would parse "auto" as an incremental initial value
    credentials: SalesforceAuth = None
    sobject: str = None
    lag: Optional[float] = None
    soql_filter: Optional[str] = None
    include_deleted: bool = True
    double_as_decimal: bool = False
    backend: TTableBackend = "auto"
    api: TQueryApi = "auto"
    chunk_size: int = 50000
    included_columns: Optional[List[str]] = None
    excluded_columns: Optional[List[str]] = None
    write_disposition: Optional[TWriteDispositionDict] = None
    primary_key: Optional[TColumnNames] = None
    merge_key: Optional[TColumnNames] = None
    defer_table_reflect: bool = False


class DeferredDescribe(NamedTuple):
    """Arguments to describe the sObject when extracting and hints derived without describing it"""

    included_columns: Optional[List[str]]
    excluded_columns: Optional[List[str]]
    double_as_decimal: bool
    auto_cursor: bool
    has_primary_key: bool
    hard_delete: bool
    skip_dedup: bool


def describe_columns(
    connection: SalesforceConnection,
    sobject: str,
    included_columns: Optional[List[str]],
    excluded_columns: Optional[List[str]],
    double_as_decimal: bool,
) -> Tuple[TTableSchemaColumns, Optional[List[str]]]:
    """Describes `sobject` and returns columns of the selected fields and the primary key"""
    fields = connection.describe(sobject)["fields"]
    unknown = sorted(set(excluded_columns or []) - {field["name"] for field in fields})
    if unknown:
        logger.warning(
            f"`excluded_columns` {unknown} are not fields of sObject `{sobject}`"
        )
    if included_columns:
        fields = [field for field in fields if field["name"] in included_columns]
    if excluded_columns:
        fields = [field for field in fields if field["name"] not in excluded_columns]
    columns = fields_to_columns(fields, double_as_decimal)
    unknown = [name for name in included_columns or [] if name not in columns]
    if unknown:
        logger.warning(
            f"`included_columns` {unknown} are not queryable fields of sObject `{sobject}` and"
            " will not be loaded"
        )
    return columns, get_primary_key(fields)


def _is_time_cursor(columns: TTableSchemaColumns, cursor_field: Optional[str]) -> bool:
    return columns.get(cursor_field or "", {}).get("data_type") in ("timestamp", "date")


def resolve_backend(backend: TTableBackend) -> Literal["python", "pyarrow"]:
    """Picks pyarrow for "auto" when it is installed, without importing it"""
    if backend == "auto":
        return "pyarrow" if importlib.util.find_spec("pyarrow") else "python"
    return backend


def table_rows(
    connection: SalesforceConnection,
    query_api: QueryApi,
    sobject: str,
    columns: TTableSchemaColumns,
    chunk_size: int,
    include_deleted: bool,
    soql_filter: Optional[str],
    backend: Literal["python", "pyarrow"],
    incremental: Optional[Incremental[Any]] = None,
    deferred: Optional[DeferredDescribe] = None,
) -> Iterator[TDataItem]:
    """Queries `columns` of `sobject` and yields result pages as Arrow tables or lists of dicts.

    With `deferred`, `columns` are empty and the sObject is described first.
    """
    resource = dlt.current.resource()
    primary_key: List[str] = []
    if deferred:
        columns, hints = _describe_when_extracting(
            connection, sobject, incremental, deferred
        )
        primary_key = hints.get("primary_key") or []
        # merge resource hints with described hints before yielding
        if resource.columns and not callable(resource.columns):
            hints["columns"] = merge_columns(hints["columns"], resource.columns)
        # yield empty item to set hints before records reach the incremental and create schema
        yield dlt.mark.with_hints([], dlt.mark.make_hints(**hints))
        columns_hints = hints["columns"]
    elif callable(resource.columns):
        columns_hints = columns
    else:
        # resource hints include hints applied by the user
        columns_hints = resource.columns
    resource.add_input(
        TDataLocation(
            kind="salesforce",
            resource_name=resource.name,
            location=connection.instance_url,
        ),
        replace=True,
    )
    unmatched = [name for name in columns_hints if name not in columns]
    if unmatched:
        logger.warning(
            f"Resource columns {unmatched} for sObject `{sobject}` do not match any queried field"
            " and will be ignored. Use the original Salesforce field names in apply_hints."
        )
    columns_hints = {name: columns_hints.get(name, columns[name]) for name in columns}
    # the primary key hint makes the key not nullable, arrow tables must agree with it
    for name in primary_key or _primary_key(resource):
        if name in columns_hints:
            key_column = copy(columns_hints[name])
            key_column["nullable"] = False
            columns_hints[name] = key_column

    # `add_limit` is pushed down to the query, it counts pages of `chunk_size` records
    limit = resource.limit.limit(chunk_size) if resource.limit else None
    cursor_name = incremental.get_cursor_column_name() if incremental else None
    if limit is not None and incremental is not None and incremental.row_order is None:
        logger.warning(
            f"LIMIT {limit} is applied to `{sobject}` without ORDER BY. Records are returned in"
            f" no particular order and the next run starts after the max `{cursor_name}` so"
            " records may be skipped. Set `row_order='asc'` on the incremental."
        )

    soql = make_soql(sobject, columns_hints, incremental, soql_filter, limit)
    rows_count, max_cursor = 0, None
    try:
        for page in query_api.query(soql, columns_hints, include_deleted, chunk_size):
            item: Any
            if backend == "pyarrow":
                item = csv_page_to_arrow(page, columns_hints)
                if item is None:
                    continue
                rows_count += item.num_rows
                page_max = arrow_column_max(item, cursor_name) if cursor_name else None
            else:
                item = csv_page_to_rows(page, columns_hints, cursor_name)
                if not item:
                    continue
                rows_count += len(item)
                page_max = (
                    max(
                        (
                            row[cursor_name]
                            for row in item
                            if row[cursor_name] is not None
                        ),
                        default=None,
                    )
                    if cursor_name
                    else None
                )
            if page_max is not None:
                max_cursor = (
                    page_max if max_cursor is None else max(max_cursor, page_max)
                )
            yield item
    except GeneratorExit:
        # `add_limit` closes the generator after the last page
        _warn_if_cursor_not_moved(
            sobject, columns_hints, incremental, max_cursor, rows_count, limit, True
        )
        raise
    _warn_if_cursor_not_moved(
        sobject, columns_hints, incremental, max_cursor, rows_count, limit, False
    )


def _primary_key(resource: DltResource) -> List[str]:
    """Returns the primary key set in static resource hints"""
    if resource.has_dynamic_table_name or resource.has_other_dynamic_hints:
        return []
    columns = resource.compute_table_schema()["columns"]
    return [name for name, column in columns.items() if column.get("primary_key")]


def _warn_if_cursor_not_moved(
    sobject: str,
    columns: TTableSchemaColumns,
    incremental: Optional[Incremental[Any]],
    max_cursor: Any,
    rows_count: int,
    limit: Optional[int],
    truncated: bool,
) -> None:
    """Warns when all records read under LIMIT have the cursor value the run started from"""
    # fewer records than LIMIT are all the records, unless `add_limit` closed the run early.
    # REST API pages hold fewer records than `chunk_size`
    if limit is None or incremental is None or (rows_count < limit and not truncated):
        return
    cursor_name = incremental.get_cursor_column_name()
    last_value, _ = incremental.get_current_range(apply_lag=False)
    if last_value is None or max_cursor is None:
        return
    cursor = columns[cursor_name]
    if to_soql_literal(max_cursor, cursor) == to_soql_literal(last_value, cursor):
        logger.warning(
            f"Could not move the cursor `{cursor_name}` of `{sobject}` past {last_value}: all"
            f" {rows_count} records read have this value or are within the incremental lag."
            " Increase `add_limit` or `chunk_size`."
        )


def _describe_when_extracting(
    connection: SalesforceConnection,
    sobject: str,
    incremental: Optional[Incremental[Any]],
    deferred: DeferredDescribe,
) -> Tuple[TTableSchemaColumns, Dict[str, Any]]:
    """Describes `sobject` and returns its columns and the hints `table_resource` sets for them.

    Disables deduplication of `incremental` like `table_resource` does for a described sObject.
    """
    columns, primary_key = describe_columns(
        connection,
        sobject,
        deferred.included_columns,
        deferred.excluded_columns,
        deferred.double_as_decimal,
    )
    if deferred.hard_delete and "IsDeleted" in columns:
        columns["IsDeleted"]["hard_delete"] = True
    if incremental is not None:
        cursor_field = incremental.get_cursor_column_name()
        if deferred.auto_cursor and cursor_field not in columns:
            raise ValueError(
                f'sObject `{sobject}` has no `{cursor_field}` field, which the "auto"'
                " incremental expects when the describe is deferred. Pass an explicit"
                " incremental or set `defer_table_reflect=False`."
            )
        if deferred.skip_dedup and _is_time_cursor(columns, cursor_field):
            incremental.primary_key = ()
    hints: Dict[str, Any] = {
        "columns": {name: copy(column) for name, column in columns.items()}
    }
    if not deferred.has_primary_key:
        # an explicit primary key was set when creating the resource
        hints["primary_key"] = primary_key
    return columns, hints


def table_resource(
    connection: SalesforceConnection,
    sobject: str,
    name: Optional[str] = None,
    incremental: TIncrementalArg = "auto",
    lag: Optional[float] = None,
    soql_filter: Optional[str] = None,
    include_deleted: bool = True,
    double_as_decimal: bool = False,
    backend: TTableBackend = "auto",
    api: TQueryApi = "auto",
    chunk_size: int = 50000,
    included_columns: Optional[List[str]] = None,
    excluded_columns: Optional[List[str]] = None,
    write_disposition: Optional[TWriteDispositionConfig] = None,
    primary_key: TColumnNames = None,
    merge_key: TColumnNames = None,
    parallelized: bool = False,
    defer_table_reflect: bool = False,
) -> DltResource:
    """Creates a resource reading `sobject` with columns and hints taken from its `describe()`.

    See `salesforce_table` for the arguments.
    """
    columns: TTableSchemaColumns = {}
    hints: Dict[str, Any] = {}
    if not defer_table_reflect:
        columns, hints["primary_key"] = describe_columns(
            connection, sobject, included_columns, excluded_columns, double_as_decimal
        )
        hints["columns"] = columns
    if primary_key:
        # explicit primary key wins over the record id
        hints["primary_key"] = (
            [primary_key] if isinstance(primary_key, str) else list(primary_key)
        )

    if incremental == "auto":
        if write_disposition == "replace":
            # the "replace" shorthand reloads the full table, it resets incremental state in dlt
            cursor_field = None
        elif defer_table_reflect:
            # all custom and nearly all standard sObjects have it, checked when described
            cursor_field = UPDATED_AT_FIELDS[0]
        else:
            cursor_field = get_cursor_field(columns)
    else:
        cursor_field = incremental.get_cursor_column_name() if incremental else None
    write_disposition = write_disposition or get_write_disposition(cursor_field)
    disposition = (
        write_disposition["disposition"]
        if isinstance(write_disposition, dict)
        else write_disposition
    )

    table_incremental: Optional[Incremental[Any]]
    if incremental == "auto":
        if lag is None and disposition == "merge":
            lag = DEFAULT_LAG
        table_incremental = (
            dlt.sources.incremental(cursor_field, lag=lag) if cursor_field else None
        )
    else:
        table_incremental = incremental
    # Salesforce timestamps have second precision, many records may share the last cursor value
    # and their hashes would be kept in the state. merge removes the duplicates
    skip_dedup = (
        table_incremental is not None
        and table_incremental.primary_key is None
        and disposition == "merge"
    )
    # records deleted in Salesforce are deleted from the destination
    hard_delete = include_deleted and disposition == "merge"

    deferred: Optional[DeferredDescribe] = None
    if defer_table_reflect:
        deferred = DeferredDescribe(
            included_columns,
            excluded_columns,
            double_as_decimal,
            auto_cursor=incremental == "auto",
            has_primary_key=bool(primary_key),
            hard_delete=hard_delete,
            skip_dedup=skip_dedup,
        )
    else:
        if (
            table_incremental is not None
            and skip_dedup
            and _is_time_cursor(columns, cursor_field)
        ):
            table_incremental = table_incremental.copy()
            table_incremental.primary_key = ()
        if hard_delete and "IsDeleted" in columns:
            columns["IsDeleted"]["hard_delete"] = True

    resource: DltResource = dlt.resource(
        table_rows,
        name=name or sobject,
        write_disposition=write_disposition,
        merge_key=merge_key,
        **hints,
    )(
        connection,
        make_query_api(connection, api),
        sobject,
        columns,
        chunk_size,
        include_deleted,
        soql_filter,
        resolve_backend(backend),
        incremental=table_incremental,
        deferred=deferred,
    )
    if parallelized:
        # parallelize the bound resource, the decorator flag does not survive binding the arguments
        resource.parallelize()
    return resource

"""Builds SOQL queries from selected columns, a filter and the incremental cursor range"""

from datetime import timezone  # noqa: I251
from typing import Any, List, Optional, Tuple

from dlt.common.schema.typing import TColumnSchema, TTableSchemaColumns
from dlt.common.time import ensure_date, ensure_datetime_in_tz
from dlt.extract.incremental import Incremental


def to_soql_literal(value: Any, column: TColumnSchema) -> str:
    """Renders `value` as a SOQL literal for a field with `column` schema"""
    data_type = column.get("data_type")
    if data_type == "timestamp":
        dt = ensure_datetime_in_tz(value, timezone.utc)
        # datetime literals are not quoted, Salesforce stores milliseconds
        return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"
    if data_type == "date":
        return ensure_date(value).isoformat()
    if data_type == "bool":
        return "true" if value else "false"
    if data_type in ("bigint", "double", "decimal"):
        return str(value)
    escaped = str(value).replace("\\", "\\\\").replace("'", "\\'")
    return f"'{escaped}'"


def make_soql(
    sobject: str,
    columns: TTableSchemaColumns,
    incremental: Optional[Incremental[Any]] = None,
    soql_filter: Optional[str] = None,
    limit: Optional[int] = None,
) -> str:
    """Builds a SOQL query selecting `columns`, filtered by `soql_filter` and `incremental`.

    ORDER BY is generated only when `row_order` is set and LIMIT only with `limit`: both disable
    PK chunking of Bulk API 2.0 query jobs, which may time out on large sObjects.
    """
    conditions = [soql_filter] if soql_filter else []
    order_by = ""
    if incremental:
        incremental_conditions, order_by = _incremental_soql(
            sobject, columns, incremental
        )
        conditions.extend(incremental_conditions)

    query = f"SELECT {', '.join(columns)} FROM {sobject}"
    if len(conditions) == 1:
        query += f" WHERE {conditions[0]}"
    elif conditions:
        # SOQL does not allow AND and OR in a single expression without parentheses
        query += " WHERE " + " AND ".join(f"({condition})" for condition in conditions)
    query += order_by
    if limit is not None:
        query += f" LIMIT {limit}"
    return query


def _incremental_soql(
    sobject: str, columns: TTableSchemaColumns, incremental: Incremental[Any]
) -> Tuple[List[str], str]:
    """Returns WHERE conditions and ORDER BY clause from the `incremental` cursor range"""
    cursor_name = incremental.get_cursor_column_name()
    if cursor_name is None:
        raise ValueError(
            f"Cursor path `{incremental.cursor_path}` must be a simple field name (e.g."
            " `SystemModstamp`)"
        )
    if cursor_name not in columns:
        raise KeyError(
            f"Cursor field `{cursor_name}` does not exist in sObject `{sobject}`"
        )
    cursor = columns[cursor_name]

    last_value_func = incremental.last_value_func
    if last_value_func is max:
        op = ">=" if incremental.range_start == "closed" else ">"
        op_end = "<" if incremental.range_end == "open" else "<="
    elif last_value_func is min:
        op = "<=" if incremental.range_start == "closed" else "<"
        op_end = ">" if incremental.range_end == "open" else ">="
    else:
        # custom last_value_func, load everything and let incremental filter
        return [], ""

    start_value, end_value = incremental.get_current_range()
    conditions: List[str] = []
    if start_value is not None:
        condition = f"{cursor_name} {op} {to_soql_literal(start_value, cursor)}"
        if end_value is not None:
            condition += (
                f" AND {cursor_name} {op_end} {to_soql_literal(end_value, cursor)}"
            )
        if incremental.on_cursor_value_missing == "include":
            condition = f"({condition}) OR {cursor_name} = null"
        conditions.append(condition)
    if incremental.on_cursor_value_missing == "exclude":
        conditions.append(f"{cursor_name} != null")

    row_order = incremental.row_order
    order_by = ""
    if (row_order == "asc" and last_value_func is max) or (
        row_order == "desc" and last_value_func is min
    ):
        order_by = f" ORDER BY {cursor_name} ASC"
    elif (row_order == "asc" and last_value_func is min) or (
        row_order == "desc" and last_value_func is max
    ):
        order_by = f" ORDER BY {cursor_name} DESC"
    return conditions, order_by

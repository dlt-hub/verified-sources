"""Generates JQL that selects issues in the range of an incremental"""

import re
from datetime import timedelta, tzinfo
from typing import Any, Callable, Dict, List, NamedTuple, Optional

from dlt.common.time import ensure_date, ensure_datetime_in_tz
from dlt.common.typing import DictStrAny
from dlt.extract.incremental import Incremental

from .schema_types import get_field_type
from ..settings import BOUNDING_JQL

ORDER_BY = re.compile(r"\border\s+by\b", re.IGNORECASE)
# clause names usable in JQL without quoting, e.g. `updated` or `cf[10050]`
JQL_CLAUSE_NAME = re.compile(r"cf\[\d+\]|\w+")


class CursorRange(NamedTuple):
    """The range of an incremental on a date or datetime field"""

    clause: str
    # `date` or `timestamp`
    cursor_type: str
    # JQL interprets datetime literals in the time zone of the user
    time_zone: tzinfo
    last_value: Any
    end_value: Any
    last_value_func: Callable[..., Any]
    on_cursor_value_missing: str
    row_order: Optional[str]


def get_cursor_clause(
    incremental: Incremental[Any], fields: Dict[str, DictStrAny]
) -> str:
    """Returns the JQL clause name of the incremental cursor field"""
    cursor_name = incremental.get_cursor_column_name()
    if cursor_name is None:
        raise ValueError(
            f"Cursor path `{incremental.cursor_path}` must be a simple field id (e.g. `updated`)"
        )
    if cursor_name not in fields:
        raise KeyError(f"Cursor field `{cursor_name}` does not exist in Jira")
    field = fields[cursor_name]
    # clause names are not ordered, custom fields may start with a name that requires quoting
    clause_names: List[str] = field.get("clauseNames") or []
    clause = next(
        (name for name in clause_names if JQL_CLAUSE_NAME.fullmatch(name)), None
    )
    if get_field_type(field) not in ("timestamp", "date") or clause is None:
        raise ValueError(
            f"Cursor field `{cursor_name}` must be a searchable date or datetime field"
        )
    return clause


def make_cursor_range(
    incremental: Incremental[Any], fields: Dict[str, DictStrAny], time_zone: tzinfo
) -> CursorRange:
    """Returns the current range of `incremental` with lag applied"""
    last_value, end_value = incremental.get_current_range()
    return CursorRange(
        clause=get_cursor_clause(incremental, fields),
        cursor_type=str(get_field_type(fields[incremental.get_cursor_column_name()])),
        time_zone=time_zone,
        last_value=last_value,
        end_value=end_value,
        last_value_func=incremental.last_value_func,
        on_cursor_value_missing=incremental.on_cursor_value_missing,
        row_order=incremental.row_order,
    )


def make_jql(jql: Optional[str], cursor: Optional[CursorRange]) -> str:
    """Generates JQL that selects a superset of the `cursor` range among issues matching `jql`.

    Datetimes are compared with minute precision in the time zone of the user, so the range
    is widened to whole minutes and the incremental filters the exact range. Without a cursor,
    `jql` is used as is.
    """
    if cursor is None:
        return jql or BOUNDING_JQL
    conditions: List[str] = []
    if jql:
        conditions.append(f"({jql})")
    order_by = ""
    if cursor.last_value_func in (max, min):
        range_conditions: List[str] = []
        if cursor.last_value is not None:
            range_conditions.append(
                cursor_condition(
                    cursor, cursor.last_value, upper=cursor.last_value_func is min
                )
            )
            if cursor.end_value is not None:
                range_conditions.append(
                    cursor_condition(
                        cursor, cursor.end_value, upper=cursor.last_value_func is max
                    )
                )
        if range_conditions:
            condition = " AND ".join(range_conditions)
            if cursor.on_cursor_value_missing == "include":
                if len(range_conditions) > 1:
                    condition = f"({condition})"
                condition = f"({condition} OR {cursor.clause} is EMPTY)"
            conditions.append(condition)
        if cursor.row_order is None:
            descending = cursor.last_value_func is min
        else:
            descending = (cursor.row_order == "desc") == (cursor.last_value_func is max)
        order_by = f" ORDER BY {cursor.clause} {'DESC' if descending else 'ASC'}"
    if cursor.on_cursor_value_missing == "exclude":
        conditions.append(f"{cursor.clause} is not EMPTY")
    if not conditions:
        conditions.append(BOUNDING_JQL)
    return " AND ".join(conditions) + order_by


def cursor_condition(cursor: CursorRange, value: Any, upper: bool) -> str:
    """Returns a JQL condition comparing the cursor field with `value`"""
    if cursor.cursor_type == "date":
        op, literal = (
            "<=" if upper else ">=",
            ensure_date(value).isoformat(),
        )
    else:
        dt = ensure_datetime_in_tz(value, cursor.time_zone).replace(
            second=0, microsecond=0
        )
        if upper:
            op, dt = "<", dt + timedelta(minutes=1)
        else:
            op = ">="
        literal = dt.strftime("%Y-%m-%d %H:%M")
    return f'{cursor.clause} {op} "{literal}"'

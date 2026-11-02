"""Builds encoded queries that read a table page by page, ordered by `sys_id` or by a cursor and
`sys_id`. Literals are rendered by the type of the field.
"""

import math
import re
from datetime import timedelta, timezone  # noqa: I251
from typing import Any, List, Literal, NamedTuple, Optional, Sequence

from dlt.common import Decimal
from dlt.common.schema.typing import TColumnSchema
from dlt.common.time import ensure_date, ensure_datetime_in_tz
from dlt.extract.incremental import Incremental

GLIDE_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
# hex digits of the `sys_id` bounds of partitions
PARTITION_BOUND_DIGITS = 8
# types that the instance compares with literals. durations are `bigint` seconds stored as
# date-times, they have no literals
LITERAL_TYPES = ("timestamp", "date", "bigint", "decimal", "double", "bool", "text")


class CursorRange(NamedTuple):
    """The lowest and the highest cursor value that the conditions of an incremental select, None
    if the range is open. Text is compared case-insensitively, like the instance does.
    """

    cursor: str
    lowest: Any
    highest: Any
    casefold: bool


class IncrementalQuery(NamedTuple):
    """Conditions of the current range of an incremental and the order of records by the cursor"""

    conditions: List[str]
    cursor_range: Optional[CursorRange]
    order: Optional[Literal["asc", "desc"]]


def to_glide_datetime(value: Any) -> str:
    """Formats a date or datetime as an encoded query literal in UTC. Naive values are in the dlt
    context timezone, UTC by default. The instance compares plain literals in UTC, whatever the
    time zone of the API user.
    """
    return (
        ensure_datetime_in_tz(value)
        .astimezone(timezone.utc)
        .strftime(GLIDE_DATETIME_FORMAT)
    )


def has_literals(column: TColumnSchema, is_duration: bool = False) -> bool:
    """Tells if the instance compares values of a field with `column` schema with literals"""
    return not is_duration and column.get("data_type") in LITERAL_TYPES


def to_glide_literal(value: Any, column: TColumnSchema) -> Optional[str]:
    """Renders `value` as an encoded query literal for a field with `column` schema. Returns None
    for types without literals and for text that cannot be a literal.
    """
    data_type = column.get("data_type")
    if data_type == "timestamp":
        return to_glide_datetime(value)
    if data_type == "date":
        return ensure_date(value).isoformat()
    if data_type == "bool":
        return "true" if value else "false"
    if data_type == "bigint":
        return str(int(value))
    if data_type in ("decimal", "double"):
        # the instance does not parse exponents, Arrow decimals carry the trailing zeros of
        # their scale
        return format(_to_decimal(value).normalize(), "f")
    if data_type == "text" and isinstance(value, str) and is_literal(value):
        return value
    return None


def to_cursor_value(value: Any, column: TColumnSchema, round_up: bool = False) -> Any:
    """Converts a bound of a cursor range into the type of the loaded values of `column`. Timestamps
    and integers are rounded up or down to the whole seconds and integers that the instance stores.
    """
    data_type = column.get("data_type")
    if data_type == "timestamp":
        value = ensure_datetime_in_tz(value).astimezone(timezone.utc)
        if value.microsecond:
            value = value.replace(microsecond=0) + timedelta(seconds=int(round_up))
        return value
    if data_type == "date":
        return ensure_date(value)
    if data_type == "bigint":
        return math.ceil(value) if round_up else math.floor(value)
    if data_type == "decimal":
        # bounds of decimal cursors are decimals or integers, `check_cursor_types` rejects floats
        return Decimal(value)
    if data_type == "double":
        return float(value)
    return value


def _to_decimal(value: Any) -> Decimal:
    # a float is converted from its shortest text, not from its binary value
    return Decimal(repr(value)) if isinstance(value, float) else Decimal(value)


def is_literal(value: str) -> bool:
    """Tells if `value` can be a literal in an encoded query. `^` cannot be escaped, and the
    instance evaluates a value that starts with `javascript:`.
    """
    return "^" not in value and not value.lower().startswith("javascript:")


class TableQuery:
    """Encoded query of a table read in pages: `page_query(after)` continues after the keys of the
    last record read.

    Records are ordered by `sys_id`, or by `cursor` and `sys_id` when `cursor` is set, descending
    when `descending` is set.
    """

    def __init__(
        self,
        conditions: Sequence[str] = (),
        encoded_query: Optional[str] = None,
        cursor: Optional[str] = None,
        descending: bool = False,
    ) -> None:
        if encoded_query and "ORDERBY" in encoded_query:
            raise ValueError(
                f"Encoded query `{encoded_query}` sets the order of records. dlt sets the order"
                " to read the records in pages. Remove ORDERBY from the query."
            )
        # queries copied from list views end with `^EQ`. the instance ignores conditions after it
        encoded_query = (encoded_query or "").strip().removesuffix("^EQ")
        # conditions are added to every OR block of the user query
        self.blocks: List[List[str]] = [
            [*conditions, *([block] if block else [])]
            for block in encoded_query.split("^NQ")
        ]
        self.keys = [cursor, "sys_id"] if cursor else ["sys_id"]
        self.descending = descending

    def page_query(self, after: Optional[Sequence[str]] = None) -> str:
        """Returns the query of the page that follows the record with `after` key values"""
        blocks = self.blocks
        if after:
            blocks = [
                block + keyset for block in blocks for keyset in self._keyset(after)
            ]
        where = "^NQ".join("^".join(block) for block in blocks)
        direction = "DESC" if self.descending else ""
        order_by = [f"ORDERBY{direction}{key}" for key in self.keys]
        return "^".join(([where] if where else []) + order_by)

    def _keyset(self, after: Sequence[str]) -> List[List[str]]:
        op = "<" if self.descending else ">"
        if len(self.keys) == 1:
            return [[f"sys_id{op}{after[0]}"]]
        cursor, value = self.keys[0], after[0]
        # empty values come first, last when descending. a comparison with an empty literal
        # selects empty values
        if not value:
            after_empty = [f"{cursor}ISEMPTY", f"sys_id{op}{after[1]}"]
            return (
                [after_empty]
                if self.descending
                else [after_empty, [f"{cursor}ISNOTEMPTY"]]
            )
        keyset = [
            [f"{cursor}{op}{value}"],
            [f"{cursor}={value}", f"sys_id{op}{after[1]}"],
        ]
        return keyset + [[f"{cursor}ISEMPTY"]] if self.descending else keyset


def is_hex_key(value: str) -> bool:
    """Tells if `value` starts with the hex digits that the bounds of partitions are made of"""
    return re.match(f"[0-9a-f]{{{PARTITION_BOUND_DIGITS}}}", value) is not None


def partition_conditions(after: str, partitions: int) -> List[List[str]]:
    """Splits the `sys_id`s after `after` into up to `partitions` ranges of the hex key space and
    returns the conditions of each range. Generated `sys_id`s are random hex, so ranges hold about
    the same number of records. Other keys sort into the first or the last range.

    Returns no ranges if `after` does not start with hex digits.
    """
    if not is_hex_key(after):
        return []
    space = 16**PARTITION_BOUND_DIGITS
    start = int(after[:PARTITION_BOUND_DIGITS], 16)
    bounds = sorted(
        {
            f"{start + (space - start) * i // partitions:0{PARTITION_BOUND_DIGITS}x}"
            for i in range(1, partitions)
        }
    )
    lower = [f"sys_id>{after}"]
    ranges = []
    # bounds at or before `after` are skipped: their ranges hold records of the previous pages
    for bound in (bound for bound in bounds if bound > after):
        ranges.append([*lower, f"sys_id<{bound}"])
        lower = [f"sys_id>={bound}"]
    ranges.append(lower)
    return ranges


def upper_side_open(incremental: Incremental[Any]) -> bool:
    """Tells if the current range of `incremental` has no upper end, so it also selects records
    written while the run reads the table: no `end_value` with `max`, no start with `min`
    """
    if incremental.last_value_func not in (max, min):
        return False
    start, end = incremental.get_current_range()
    return (end if incremental.last_value_func is max else start) is None


def incremental_query(
    incremental: Incremental[Any],
    column: TColumnSchema,
    is_duration: bool = False,
    upper_bound: Any = None,
) -> IncrementalQuery:
    """Returns the conditions that select the records in the current range of `incremental` on the
    field of `column`. Also returns the order of records that `row_order` sets.

    `upper_bound`, the time of the instance for a timestamp cursor, ends a range that has no upper
    end. With `min` the range goes back from its start. On the first run,
    `on_cursor_value_missing="include"` also selects records with an empty cursor.

    Returns no conditions for types without literals and for other `last_value_func`s. The
    incremental then filters the records.
    """
    cursor = column["name"]
    func = incremental.last_value_func
    order: Optional[Literal["asc", "desc"]] = None
    if incremental.row_order:
        order = "desc" if (incremental.row_order == "asc") == (func is min) else "asc"
    if func not in (max, min) or not has_literals(column, is_duration):
        return IncrementalQuery([], None, order)
    start, end = incremental.get_current_range()
    first_run = start is None and end is None
    if func is max:
        lower, upper = start, end
        lower_op = ">=" if incremental.range_start == "closed" else ">"
        upper_op = "<=" if incremental.range_end == "closed" else "<"
    else:
        lower, upper = end, start
        lower_op = ">=" if incremental.range_end == "closed" else ">"
        upper_op = "<=" if incremental.range_start == "closed" else "<"
    if upper is None and upper_bound is not None:
        upper, upper_op = upper_bound, "<"

    conditions: List[str] = []
    lowest = highest = None
    if lower is not None:
        lowest = to_cursor_value(lower, column, round_up=lower_op == ">=")
        literal = to_glide_literal(lowest, column)
        if literal is None:
            lowest = None
        else:
            conditions.append(f"{cursor}{lower_op}{literal}")
    if upper is not None:
        highest = to_cursor_value(upper, column, round_up=upper_op == "<")
        literal = to_glide_literal(highest, column)
        if literal is None:
            highest = None
        else:
            conditions.append(f"{cursor}{upper_op}{literal}")
    if first_run and incremental.on_cursor_value_missing == "include" and conditions:
        # range conditions do not select empty values
        conditions[-1] += f"^OR{cursor}ISEMPTY"
    if incremental.on_cursor_value_missing == "exclude" and not conditions:
        conditions.append(f"{cursor}ISNOTEMPTY")
    cursor_range = CursorRange(
        cursor, lowest, highest, casefold=column.get("data_type") == "text"
    )
    return IncrementalQuery(conditions, cursor_range, order)

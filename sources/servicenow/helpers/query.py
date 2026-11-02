"""Builds encoded queries that read a table page by page, ordered by `sys_id` or by a cursor and
`sys_id`
"""

import re
from datetime import timezone  # noqa: I251
from typing import Any, List, Optional, Sequence, Tuple

from dlt.common.time import ensure_datetime_in_tz
from dlt.extract.incremental import Incremental

GLIDE_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
# hex digits of the `sys_id` bounds of partitions
PARTITION_BOUND_DIGITS = 8


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


def incremental_conditions(
    incremental: Incremental[Any], cursor: str, window_end: Any
) -> List[str]:
    """Returns the conditions that select the records in the current range of `incremental` on
    the `cursor` field. The conditions also select only records changed before `window_end`. With
    `min` the range goes back from its start. A range without start or end also selects records
    with an empty cursor, unless the incremental excludes them.

    Returns no conditions for other `last_value_func`s. The incremental then filters the records.
    """
    if incremental.last_value_func not in (max, min):
        return []
    grows = incremental.last_value_func is max
    start, end = incremental.get_current_range()
    conditions = []
    if start is not None:
        op = ">" if grows else "<"
        if incremental.range_start == "closed":
            op += "="
        conditions.append(f"{cursor}{op}{to_glide_datetime(start)}")
    if end is not None:
        op = "<" if grows else ">"
        if incremental.range_end == "closed":
            op += "="
        conditions.append(f"{cursor}{op}{to_glide_datetime(end)}")
    window = f"{cursor}<{to_glide_datetime(window_end)}"
    if (
        start is None
        and end is None
        and incremental.on_cursor_value_missing != "exclude"
    ):
        # range conditions do not select empty values
        window += f"^OR{cursor}ISEMPTY"
    conditions.append(window)
    return conditions


def incremental_range(
    incremental: Incremental[Any], window_end: Any
) -> Optional[Tuple[Optional[str], str]]:
    """Returns the lowest and the highest cursor value selected by `incremental_conditions` as
    literals, None if all values are selected
    """
    if incremental.last_value_func not in (max, min):
        return None
    start, end = incremental.get_current_range()
    if incremental.last_value_func is min:
        start, end = end, start
    last = to_glide_datetime(window_end)
    if end is not None:
        last = min(last, to_glide_datetime(end))
    return (to_glide_datetime(start) if start is not None else None), last

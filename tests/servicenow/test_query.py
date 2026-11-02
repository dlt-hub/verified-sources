from datetime import date, datetime, timedelta, timezone  # noqa: I251
from typing import Any, List, Optional

import dlt
import pytest
from dlt.common import Decimal
from dlt.common.configuration.container import Container
from dlt.common.configuration.specs import TimezoneContext
from dlt.common.schema.typing import TColumnSchema

from sources.servicenow.helpers.query import (
    CursorRange,
    TableQuery,
    incremental_query,
    is_literal,
    partition_conditions,
    to_cursor_value,
    to_glide_datetime,
    to_glide_literal,
    upper_side_open,
)

from tests.servicenow.utils import evaluate_query, random_sys_id


def test_table_query() -> None:
    assert TableQuery().page_query() == "ORDERBYsys_id"
    assert TableQuery().page_query(["abc"]) == "sys_id>abc^ORDERBYsys_id"
    query = TableQuery(
        ["a>=1"], "active=true^NQpriority=1^ORpriority=2", "sys_updated_on"
    )
    assert query.page_query() == (
        "a>=1^active=true^NQa>=1^priority=1^ORpriority=2^ORDERBYsys_updated_on^ORDERBYsys_id"
    )
    # every block of the user query continues after the last record
    assert query.page_query(["2024-01-01 00:00:00", "abc"]) == (
        "a>=1^active=true^sys_updated_on>2024-01-01 00:00:00"
        "^NQa>=1^active=true^sys_updated_on=2024-01-01 00:00:00^sys_id>abc"
        "^NQa>=1^priority=1^ORpriority=2^sys_updated_on>2024-01-01 00:00:00"
        "^NQa>=1^priority=1^ORpriority=2^sys_updated_on=2024-01-01 00:00:00^sys_id>abc"
        "^ORDERBYsys_updated_on^ORDERBYsys_id"
    )
    # empty values come first, and a comparison with an empty literal selects them again
    assert TableQuery(cursor="resolved_at").page_query(["", "abc"]) == (
        "resolved_atISEMPTY^sys_id>abc^NQresolved_atISNOTEMPTY"
        "^ORDERBYresolved_at^ORDERBYsys_id"
    )
    assert is_literal("46d44a23a9fe19810012d100cca80666") and is_literal("Glean_Role")
    assert not is_literal("x^y") and not is_literal("JavaScript:gs.getUserID()")
    # queries copied from list views end the query with `^EQ`
    assert (
        TableQuery(["a>=1"], "active=true^EQ").page_query()
        == "a>=1^active=true^ORDERBYsys_id"
    )
    with pytest.raises(ValueError, match="ORDERBY"):
        TableQuery(encoded_query="active=true^ORDERBYDESCnumber")


def test_table_query_descending() -> None:
    query = TableQuery(["a>=1"], cursor="sys_updated_on", descending=True)
    assert query.page_query() == "a>=1^ORDERBYDESCsys_updated_on^ORDERBYDESCsys_id"
    # empty values come last, after the other values
    assert query.page_query(["2024-01-01 00:00:00", "abc"]) == (
        "a>=1^sys_updated_on<2024-01-01 00:00:00"
        "^NQa>=1^sys_updated_on=2024-01-01 00:00:00^sys_id<abc"
        "^NQa>=1^sys_updated_onISEMPTY"
        "^ORDERBYDESCsys_updated_on^ORDERBYDESCsys_id"
    )
    assert query.page_query(["", "abc"]) == (
        "a>=1^sys_updated_onISEMPTY^sys_id<abc"
        "^ORDERBYDESCsys_updated_on^ORDERBYDESCsys_id"
    )


def column(data_type: str) -> TColumnSchema:
    return {"name": "c", "data_type": data_type}  # type: ignore[typeddict-item]


@pytest.mark.parametrize(
    "value,data_type,expected",
    [
        (
            datetime(2024, 2, 1, 11, 30, tzinfo=timezone(timedelta(hours=1))),
            "timestamp",
            "2024-02-01 10:30:00",
        ),
        (datetime(2024, 2, 1, 10, 30, 0, 999999), "timestamp", "2024-02-01 10:30:00"),
        (date(2024, 2, 29), "date", "2024-02-29"),
        ("2024-02-29", "date", "2024-02-29"),
        (10001, "bigint", "10001"),
        (Decimal("-1234.56"), "decimal", "-1234.56"),
        (Decimal("1E+3"), "decimal", "1000"),
        (Decimal("23.500000000"), "decimal", "23.5"),
        (0.1, "double", "0.1"),
        (1e-07, "double", "0.0000001"),
        (True, "bool", "true"),
        ("INC0010001", "text", "INC0010001"),
        ("a^b", "text", None),
        ("javascript:gs.now()", "text", None),
        ({"a": 1}, "json", None),
    ],
)
def test_to_glide_literal(value: Any, data_type: str, expected: Optional[str]) -> None:
    # timestamps are UTC without sub-seconds. the instance does not parse exponents and
    # evaluates `javascript:` values
    assert to_glide_literal(value, column(data_type)) == expected


@pytest.mark.parametrize(
    "value,data_type,round_up,expected",
    [
        (
            datetime(2024, 2, 1, 10, 0, 0, 500000, tzinfo=timezone.utc),
            "timestamp",
            True,
            datetime(2024, 2, 1, 10, 0, 1, tzinfo=timezone.utc),
        ),
        (
            datetime(2024, 2, 1, 10, 0, 0, 500000, tzinfo=timezone.utc),
            "timestamp",
            False,
            datetime(2024, 2, 1, 10, 0, 0, tzinfo=timezone.utc),
        ),
        (
            datetime(2024, 2, 1, 11, 0, tzinfo=timezone(timedelta(hours=1))),
            "timestamp",
            True,
            datetime(2024, 2, 1, 10, 0, tzinfo=timezone.utc),
        ),
        (Decimal("10.5"), "bigint", True, 11),
        (Decimal("10.5"), "bigint", False, 10),
        (7, "bigint", True, 7),
        (Decimal("0.1"), "decimal", False, Decimal("0.1")),
        (7, "decimal", False, Decimal(7)),
        (date(2024, 2, 29), "date", True, date(2024, 2, 29)),
    ],
)
def test_to_cursor_value(
    value: Any, data_type: str, round_up: bool, expected: Any
) -> None:
    # the instance stores whole seconds and integers
    assert to_cursor_value(value, column(data_type), round_up) == expected


def test_context_timezone() -> None:
    # naive values are in the context timezone, as in `dlt.sources.incremental`
    with Container().injectable_context(TimezoneContext("Europe/Berlin")):
        assert to_glide_literal(datetime(2024, 2, 1, 11, 30), column("timestamp")) == (
            "2024-02-01 10:30:00"
        )
        assert to_cursor_value("2024-02-01 11:30:00", column("timestamp")) == (
            datetime(2024, 2, 1, 10, 30, tzinfo=timezone.utc)
        )
        # a `start_date` without time
        assert to_cursor_value(date(2024, 2, 1), column("timestamp")) == (
            datetime(2024, 1, 31, 23, tzinfo=timezone.utc)
        )


@pytest.mark.parametrize(
    "func,initial_value,end_value,range_start,range_end,expected,expected_range",
    [
        (max, None, None, "closed", "open", ["c<{W}"], (None, "W")),
        (min, None, None, "closed", "open", ["c<{W}"], (None, "W")),
        (max, "S", None, "closed", "open", ["c>={S}", "c<{W}"], ("S", "W")),
        (min, "S", None, "closed", "open", ["c<={S}"], (None, "S")),
        (max, "S", None, "open", "open", ["c>{S}", "c<{W}"], ("S", "W")),
        (min, "S", None, "open", "open", ["c<{S}"], (None, "S")),
        (max, "S", "E", "closed", "open", ["c>={S}", "c<{E}"], ("S", "E")),
        (min, "S", "E", "closed", "open", ["c>{E}", "c<={S}"], ("E", "S")),
        (min, "S", "E", "closed", "closed", ["c>={E}", "c<={S}"], ("E", "S")),
    ],
    ids=[
        "max",
        "min",
        "max-start",
        "min-start",
        "max-open-start",
        "min-open-start",
        "max-end",
        "min-end",
        "min-closed-end",
    ],
)
def test_incremental_query(
    func: Any,
    initial_value: Optional[str],
    end_value: Optional[str],
    range_start: str,
    range_end: str,
    expected: List[str],
    expected_range: Any,
) -> None:
    # with `min` the range goes back in time from its start. the time of the instance ends a
    # range without an upper end
    values = {
        "S": datetime(2024, 3, 1, tzinfo=timezone.utc),
        "E": datetime(2024, 1, 1, tzinfo=timezone.utc)
        if func is min
        else datetime(2024, 6, 1, tzinfo=timezone.utc),
        "W": datetime(2024, 12, 1, tzinfo=timezone.utc),
    }
    incremental = dlt.sources.incremental(
        "c",
        initial_value=values.get(initial_value),
        end_value=values.get(end_value),
        last_value_func=func,
        range_start=range_start,
        range_end=range_end,
    )
    assert upper_side_open(incremental) == ("W" in expected_range)
    query = incremental_query(incremental, column("timestamp"), upper_bound=values["W"])
    literals = {name: to_glide_datetime(value) for name, value in values.items()}
    assert query.conditions == [condition.format(**literals) for condition in expected]
    lowest, highest = expected_range
    assert query.cursor_range == CursorRange(
        "c", values.get(lowest), values[highest], casefold=False
    )
    assert query.order is None


@pytest.mark.parametrize(
    "data_type,start,expected,expected_range",
    [
        ("date", date(2024, 2, 29), ["c>=2024-02-29"], None),
        ("bigint", Decimal("10.5"), ["c>=11"], (11, None)),
        ("text", "b", ["c>=b"], None),
        ("text", "a^b", [], (None, None)),
        ("time", "12:00:00", [], "none"),
    ],
)
def test_incremental_query_types(
    data_type: str, start: Any, expected: List[str], expected_range: Any
) -> None:
    incremental = dlt.sources.incremental("c", initial_value=start)
    query = incremental_query(incremental, column(data_type))
    # values that cannot be literals select no records on the instance, the incremental filters
    assert query.conditions == expected
    if expected_range == "none":
        assert query.cursor_range is None
    else:
        lowest, highest = expected_range or (start, None)
        assert query.cursor_range == CursorRange(
            "c", lowest, highest, casefold=data_type == "text"
        )


def test_incremental_query_without_conditions() -> None:
    # durations are seconds stored as date-times, they have no literals
    incremental = dlt.sources.incremental("c", initial_value=10)
    query = incremental_query(incremental, column("bigint"), is_duration=True)
    assert query == ([], None, None)
    # the incremental filters the records of other `last_value_func`s
    custom = dlt.sources.incremental(
        "c", initial_value=10, last_value_func=lambda values: max(values)
    )
    assert incremental_query(custom, column("bigint")) == ([], None, None)
    assert not upper_side_open(custom)


@pytest.mark.parametrize(
    "on_cursor_value_missing,initial_value,upper_bound,expected",
    [
        ("include", None, "W", ["c<{W}^ORcISEMPTY"]),
        ("include", "S", "W", ["c>={S}", "c<{W}"]),
        ("include", None, None, []),
        ("raise", None, "W", ["c<{W}"]),
        ("exclude", None, "W", ["c<{W}"]),
        ("exclude", None, None, ["cISNOTEMPTY"]),
    ],
)
def test_incremental_query_empty_values(
    on_cursor_value_missing: Any,
    initial_value: Optional[str],
    upper_bound: Optional[str],
    expected: List[str],
) -> None:
    # empty values are read on the first run only, so `append` does not load them again
    values = {
        "S": datetime(2024, 3, 1, tzinfo=timezone.utc),
        "W": datetime(2024, 12, 1, tzinfo=timezone.utc),
    }
    incremental = dlt.sources.incremental(
        "c",
        initial_value=values.get(initial_value),
        on_cursor_value_missing=on_cursor_value_missing,
    )
    query = incremental_query(
        incremental, column("timestamp"), upper_bound=values.get(upper_bound)
    )
    literals = {name: to_glide_datetime(value) for name, value in values.items()}
    assert query.conditions == [condition.format(**literals) for condition in expected]


@pytest.mark.parametrize(
    "func,row_order,expected",
    [
        (max, None, None),
        (max, "asc", "asc"),
        (max, "desc", "desc"),
        (min, "asc", "desc"),
        (min, "desc", "asc"),
    ],
)
def test_incremental_query_order(func: Any, row_order: Any, expected: Any) -> None:
    # `row_order` follows `last_value_func`: "asc" with `min` reads the newest first
    incremental = dlt.sources.incremental(
        "c", last_value_func=func, row_order=row_order
    )
    assert incremental_query(incremental, column("bigint")).order == expected


@pytest.mark.parametrize("partitions", [2, 3, 10, 100])
@pytest.mark.parametrize("after", ["00000000ab", "7fffffff00", "fffffff0aa"])
def test_partition_conditions(after: str, partitions: int) -> None:
    ranges = partition_conditions(after, partitions)
    assert 1 <= len(ranges) <= partitions
    keys = [random_sys_id(i) for i in range(500)]
    keys += [after, "0", "f" * 32, "g1", "zz", "ABCDEF00"]
    for key in keys:
        matching = [
            conditions
            for conditions in ranges
            if evaluate_query([{"sys_id": key}], "^".join(conditions))
        ]
        # every key after the first page is read exactly once
        assert len(matching) == (1 if key > after else 0), key
    assert partition_conditions("x^y", partitions) == []
    assert partition_conditions("ABCDEF0012", partitions) == []

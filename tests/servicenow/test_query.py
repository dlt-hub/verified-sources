from datetime import date, datetime, timedelta, timezone  # noqa: I251
from typing import Any, List, Optional

import dlt
import pytest
from dlt.common.configuration.container import Container
from dlt.common.configuration.specs import TimezoneContext

from sources.servicenow.helpers.query import (
    TableQuery,
    incremental_conditions,
    incremental_range,
    is_literal,
    partition_conditions,
    to_glide_datetime,
)

from tests.servicenow.utils import evaluate_query, random_sys_id


@pytest.mark.parametrize(
    "value,expected",
    [
        ("2024-02-01 10:30:00", "2024-02-01 10:30:00"),
        ("2024-02-01T11:30:00+01:00", "2024-02-01 10:30:00"),
        (datetime(2024, 2, 1, 10, 30, 0, 999999), "2024-02-01 10:30:00"),
        (
            datetime(2024, 2, 1, 11, 30, tzinfo=timezone(timedelta(hours=1))),
            "2024-02-01 10:30:00",
        ),
        (date(2024, 2, 1), "2024-02-01 00:00:00"),
    ],
    ids=["naive-str", "offset-str", "naive-datetime", "aware-datetime", "date"],
)
def test_to_glide_datetime(value: Any, expected: str) -> None:
    # literals are UTC, sub-seconds are truncated
    assert to_glide_datetime(value) == expected


def test_to_glide_datetime_context_timezone() -> None:
    # naive values are in the context timezone, as in `dlt.sources.incremental`
    with Container().injectable_context(TimezoneContext("Europe/Berlin")):
        assert to_glide_datetime(datetime(2024, 2, 1, 11, 30)) == "2024-02-01 10:30:00"
        assert to_glide_datetime("2024-02-01 11:30:00") == "2024-02-01 10:30:00"
        assert to_glide_datetime(date(2024, 2, 1)) == "2024-01-31 23:00:00"
        assert to_glide_datetime("2024-02-01T11:30:00Z") == "2024-02-01 11:30:00"


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


@pytest.mark.parametrize(
    "func,initial_value,end_value,range_start,range_end,expected",
    [
        (max, None, None, "closed", "open", ["c<{W}^ORcISEMPTY"]),
        (min, None, None, "closed", "open", ["c<{W}^ORcISEMPTY"]),
        (max, "S", None, "closed", "open", ["c>={S}", "c<{W}"]),
        (min, "S", None, "closed", "open", ["c<={S}", "c<{W}"]),
        (max, "S", None, "open", "open", ["c>{S}", "c<{W}"]),
        (min, "S", None, "open", "open", ["c<{S}", "c<{W}"]),
        (max, "S", "E", "closed", "open", ["c>={S}", "c<{E}", "c<{W}"]),
        (min, "S", "E", "closed", "open", ["c<={S}", "c>{E}", "c<{W}"]),
        (min, "S", "E", "closed", "closed", ["c<={S}", "c>={E}", "c<{W}"]),
    ],
    ids=[
        "max",
        "min",
        "max-start",
        "min-start",
        "max-open-start",
        "min-open-start",
        "max-window",
        "min-window",
        "min-closed-window",
    ],
)
def test_incremental_conditions(
    func: Any,
    initial_value: Optional[str],
    end_value: Optional[str],
    range_start: str,
    range_end: str,
    expected: List[str],
) -> None:
    # with `min` the range goes back in time from its start
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
    literals = {name: to_glide_datetime(value) for name, value in values.items()}
    conditions = incremental_conditions(incremental, "c", values["W"])
    assert conditions == [condition.format(**literals) for condition in expected]
    first, last = incremental_range(incremental, values["W"])  # type: ignore[misc]
    if func is max:
        assert (first, last) == (
            literals["S"] if initial_value else None,
            literals["E"] if end_value else literals["W"],
        )
    else:
        assert (first, last) == (
            literals["E"] if end_value else None,
            literals["S"] if initial_value else literals["W"],
        )


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

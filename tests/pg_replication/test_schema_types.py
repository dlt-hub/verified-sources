from datetime import date, datetime, time, timedelta, timezone
from typing import Any, List

import pytest

from dlt.common import Decimal
from dlt.common.schema.typing import TColumnType

from sources.pg_replication.schema_types import (
    PgType,
    get_value_converter,
    parse_array,
    to_dlt_column_type,
)


@pytest.mark.parametrize(
    "literal,pg_type,expected",
    [
        ("{1,2,NULL}", PgType(1007, -1, 23), [1, 2, None]),
        ('{a,"b c",NULL,"NULL"}', PgType(1009, -1, 25), ["a", "b c", None, "NULL"]),
        ("{{1,2},{3,4}}", PgType(1007, -1, 23), [[1, 2], [3, 4]]),
        ('{"a\\"b","c\\\\d"}', PgType(1009, -1, 25), ['a"b', "c\\d"]),
        ("[0:1]={1,2}", PgType(1007, -1, 23), [1, 2]),
        ("{}", PgType(1007, -1, 23), []),
        ('{"{\\"a\\": 1}"}', PgType(3807, -1, 3802), [{"a": 1}]),
        ("{t,f}", PgType(1000, -1, 16), [True, False]),
    ],
    ids=["int", "text", "nested", "escapes", "bounds", "empty", "jsonb", "bool"],
)
def test_parse_array(literal: str, pg_type: PgType, expected: List[Any]) -> None:
    assert get_value_converter(pg_type)(literal) == expected
    assert parse_array("{a,b}") == ["a", "b"]


@pytest.mark.parametrize(
    "type_id,text,expected",
    [
        (
            1184,
            "2024-01-02 08:11:12.5+00",
            datetime(2024, 1, 2, 8, 11, 12, 500000, timezone.utc),
        ),
        (1184, "infinity", datetime.max.replace(tzinfo=timezone.utc)),
        (1114, "2024-01-02 10:11:12", datetime(2024, 1, 2, 10, 11, 12)),
        (1114, "-infinity", datetime.min),
        (1082, "-infinity", date.min),
        (1083, "10:11:12.123456", time(10, 11, 12, 123456)),
        (
            1266,
            "10:11:12+05:30",
            time(10, 11, 12, tzinfo=timezone(timedelta(hours=5, minutes=30))),
        ),
        (1266, "10:11:12-03", time(10, 11, 12, tzinfo=timezone(-timedelta(hours=3)))),
        (17, "\\xdead", b"\xde\xad"),
        (16, "f", False),
        (701, "Infinity", float("inf")),
        (700, "1.5", 1.5),
        (1700, "1.123456789012345678", Decimal("1.123456789012345678")),
        (20, "-9223372036854775808", -9223372036854775808),
    ],
    ids=[
        "timestamptz",
        "timestamptz-infinity",
        "timestamp",
        "timestamp-minus-infinity",
        "date-minus-infinity",
        "time",
        "timetz",
        "timetz-negative",
        "bytea",
        "bool",
        "double-infinity",
        "real",
        "numeric",
        "bigint",
    ],
)
def test_value_converters(type_id: int, text: str, expected: Any) -> None:
    assert get_value_converter(PgType(type_id, -1))(text) == expected


def test_numeric_nan() -> None:
    assert get_value_converter(PgType(1700, -1))("NaN").is_nan()


@pytest.mark.parametrize(
    "pg_type,expected",
    [
        (PgType(21, -1), {"data_type": "bigint", "precision": 32}),
        (PgType(23, -1), {"data_type": "bigint"}),
        (PgType(700, -1), {"data_type": "double"}),
        (
            PgType(1700, (10 << 16 | 2) + 4),
            {"data_type": "decimal", "precision": 10, "scale": 2},
        ),
        (PgType(1700, -1), {"data_type": "decimal"}),
        (PgType(1043, 24), {"data_type": "text", "precision": 20}),
        (PgType(1042, 9), {"data_type": "text", "precision": 5}),
        (PgType(25, -1), {"data_type": "text"}),
        (PgType(1266, -1), {"data_type": "time"}),
        (PgType(1114, -1), {"data_type": "timestamp", "timezone": False}),
        (PgType(1184, -1), {"data_type": "timestamp", "timezone": True}),
        (PgType(1007, -1, 23), {"data_type": "json"}),
        (PgType(3802, -1), {"data_type": "json"}),
        (PgType(2950, -1), {"data_type": "text"}),
        (PgType(1186, -1), {"data_type": "text"}),
        (PgType(16385, -1), {"data_type": "text"}),
    ],
    ids=[
        "smallint",
        "integer",
        "real",
        "numeric-10-2",
        "numeric",
        "varchar-20",
        "char-5",
        "text",
        "timetz",
        "timestamp",
        "timestamptz",
        "int-array",
        "jsonb",
        "uuid",
        "interval",
        "enum",
    ],
)
def test_dlt_column_type(pg_type: PgType, expected: TColumnType) -> None:
    assert to_dlt_column_type(pg_type) == expected

from datetime import date, datetime, time, timedelta, timezone
from typing import Any, List

import pytest

from dlt.common import Decimal
from dlt.common.schema.typing import TColumnType

from sources.pg_replication.schema_types import (
    PgType,
    column_type_fits,
    format_column_type,
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
    assert to_dlt_column_type(pg_type, "full_with_precision") == expected


@pytest.mark.parametrize(
    "pg_type,expected",
    [
        (PgType(21, -1), {"data_type": "bigint"}),
        (PgType(1043, 24), {"data_type": "text"}),
        (PgType(1042, 9), {"data_type": "text"}),
        (
            PgType(1700, (10 << 16 | 2) + 4),
            {"data_type": "decimal", "precision": 10, "scale": 2},
        ),
    ],
    ids=["smallint", "varchar-20", "char-5", "numeric-10-2"],
)
def test_dlt_column_type_without_precision(
    pg_type: PgType, expected: TColumnType
) -> None:
    assert to_dlt_column_type(pg_type, "full") == expected


def _numeric(precision: int, scale: int) -> PgType:
    return PgType(1700, (precision << 16 | scale) + 4)


@pytest.mark.parametrize(
    "existing,pg_type,fits",
    [
        ({"data_type": "bigint"}, PgType(20, -1), True),
        ({"data_type": "bigint", "precision": 32}, PgType(21, -1), True),
        ({"data_type": "bigint", "precision": 32}, PgType(23, -1), True),
        ({"data_type": "bigint", "precision": 32}, PgType(20, -1), False),
        ({"data_type": "bigint", "precision": 32}, PgType(26, -1), False),
        ({"data_type": "decimal", "precision": 10, "scale": 2}, _numeric(10, 2), True),
        ({"data_type": "decimal", "precision": 10, "scale": 2}, _numeric(8, 1), True),
        ({"data_type": "decimal", "precision": 10, "scale": 2}, _numeric(12, 2), False),
        ({"data_type": "decimal", "precision": 10, "scale": 2}, _numeric(10, 3), False),
        (
            {"data_type": "decimal", "precision": 10, "scale": 2},
            PgType(1700, -1),
            False,
        ),
        ({"data_type": "decimal"}, _numeric(38, 9), True),
        ({"data_type": "decimal"}, _numeric(20, 12), False),
        ({"data_type": "decimal"}, PgType(1700, -1), True),
        ({"data_type": "decimal", "precision": 38, "scale": 9}, PgType(1700, -1), True),
        ({"data_type": "decimal", "precision": 20}, _numeric(20, 9), True),
        ({"data_type": "decimal", "precision": 20}, _numeric(20, 10), False),
        ({"data_type": "text"}, PgType(1043, 54), True),
        ({"data_type": "text"}, PgType(2950, -1), True),
        ({"data_type": "text", "precision": 5}, PgType(1043, 9), True),
        ({"data_type": "text", "precision": 5}, PgType(1043, 54), False),
        ({"data_type": "text", "precision": 5}, PgType(25, -1), False),
        ({"data_type": "double"}, PgType(700, -1), True),
        ({"data_type": "double"}, _numeric(10, 2), False),
        ({"data_type": "json"}, PgType(3802, -1), True),
        ({"data_type": "json"}, PgType(1007, -1, 23), True),
        ({"data_type": "timestamp", "timezone": False}, PgType(1114, -1), True),
        ({"data_type": "timestamp", "timezone": False}, PgType(1184, -1), False),
        ({"data_type": "timestamp"}, PgType(1184, -1), True),
        ({"data_type": "timestamp", "precision": 3}, PgType(1184, 3), True),
        ({"data_type": "timestamp", "precision": 3}, PgType(1184, -1), False),
        ({"data_type": "time"}, PgType(1266, -1), True),
        ({"data_type": "date"}, PgType(1114, -1), False),
        ({"data_type": "binary", "precision": 10}, PgType(17, -1), False),
        ({"data_type": "bigint"}, PgType(25, -1), False),
    ],
    ids=[
        "int8-to-bigint",
        "int2-to-bigint-32",
        "int4-to-bigint-32",
        "int8-to-bigint-32",
        "oid-to-bigint-32",
        "numeric-same",
        "numeric-smaller",
        "numeric-more-integer-digits",
        "numeric-more-scale",
        "unbounded-numeric-to-numeric",
        "numeric-38-9-to-default",
        "numeric-scale-12-to-default",
        "unbounded-numeric-to-default",
        "unbounded-numeric-to-explicit-default",
        "numeric-to-precision-default-scale",
        "numeric-more-scale-to-precision-default-scale",
        "varchar-to-text",
        "uuid-to-text",
        "varchar-5-to-text-5",
        "varchar-50-to-text-5",
        "text-to-text-5",
        "real-to-double",
        "numeric-to-double",
        "jsonb-to-json",
        "array-to-json",
        "timestamp-to-timestamp",
        "timestamptz-to-timestamp",
        "timestamptz-to-timestamptz",
        "timestamp-3-to-timestamp-3",
        "timestamp-6-to-timestamp-3",
        "timetz-to-time",
        "timestamp-to-date",
        "bytea-to-binary-10",
        "text-to-bigint",
    ],
)
def test_column_type_fits(existing: TColumnType, pg_type: PgType, fits: bool) -> None:
    assert column_type_fits(existing, pg_type, (38, 9)) is fits


def test_format_column_type() -> None:
    assert format_column_type(
        {"data_type": "decimal", "precision": 10, "scale": 2}
    ) == ("decimal(10,2)")
    assert format_column_type({"data_type": "timestamp", "timezone": False}) == (
        "timestamp without time zone"
    )
    assert format_column_type({"data_type": "text"}) == "text"

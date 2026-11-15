from datetime import date, datetime, time, timezone
from typing import Any, Dict

import pytest
from dlt.common import Decimal
from dlt.common.normalizers.naming.snake_case import (
    NamingConvention as SnakeCaseNamingConvention,
)

from sources.kafka.helpers.columns import (
    TypedField,
    check_names,
    drop_null_records,
    fields_columns,
    record_paths,
    value_converter,
)

TEXT = TypedField({"data_type": "text"}, None)


@pytest.mark.parametrize(
    "conversion,value,expected",
    [
        pytest.param(
            ("epoch", "ns"),
            # binary floats round this value up a microsecond
            1767225600123456896,
            datetime(2026, 1, 1, 0, 0, 0, 123456, tzinfo=timezone.utc),
            id="epoch_ns",
        ),
        pytest.param(
            ("local_epoch", "ns"),
            1767225600123456789,
            datetime(2026, 1, 1, 0, 0, 0, 123456),
            id="local_epoch_ns",
        ),
        pytest.param(
            ("epoch", "us"),
            1767225600000001,
            datetime(2026, 1, 1, 0, 0, 0, 1, tzinfo=timezone.utc),
            id="epoch_us",
        ),
        pytest.param(("time", "us"), 3_723_000_004, time(1, 2, 3, 4), id="time_us"),
        pytest.param(("time", "ns"), 3_723_000_004_896, time(1, 2, 3, 4), id="time_ns"),
        pytest.param(("days", None), 20454, date(2026, 1, 1), id="days"),
        pytest.param(
            ("uuid", None),
            b"\x00" * 15 + b"\x01",
            "00000000-0000-0000-0000-000000000001",
            id="uuid_bytes",
        ),
        pytest.param(("str", None), Decimal("1.50"), "1.50", id="decimal_text"),
        pytest.param(("decimal", 2), "BNI=", Decimal("12.34"), id="decimal_base64"),
        pytest.param(
            ("variable_decimal", None),
            {"scale": 2, "value": b"\x04\xd2"},
            "12.34",
            id="variable_decimal_avro",
        ),
        pytest.param(
            ("variable_decimal", None),
            '{"scale": 2, "value": "BNI="}',
            "12.34",
            id="variable_decimal_json",
        ),
        pytest.param(
            ("iso_timestamp", None),
            "2026-01-01T02:00:00+02:00",
            datetime(2026, 1, 1, tzinfo=timezone.utc),
            id="iso_timestamp",
        ),
        pytest.param(("json", None), '{"a": 1}', '{"a": 1}', id="json"),
        # Debezium writes a placeholder for values that did not change
        pytest.param(
            ("json", None),
            "__debezium_unavailable_value",
            '"__debezium_unavailable_value"',
            id="json_placeholder",
        ),
    ],
)
def test_value_converter(conversion: Any, value: Any, expected: Any) -> None:
    assert value_converter(conversion)(value) == expected


def test_timestamp_out_of_range() -> None:
    # Debezium writes `infinity` of PostgreSQL as the largest microseconds it can
    with pytest.raises(OverflowError):
        value_converter(("epoch", "us"))(9223372036825200000)


def test_unknown_conversion() -> None:
    with pytest.raises(ValueError, match="Unknown conversion"):
        value_converter(("hex", None))


def test_fields_columns_and_names() -> None:
    naming = SnakeCaseNamingConvention()
    fields = {("billing", "City"): TEXT, ("id",): TEXT}
    assert list(fields_columns(fields, naming)) == ["billing__City", "id"]
    # columns of different fields that normalize to the same name
    for clash in [
        {("userId",): TEXT, ("user_id",): TEXT},
        {("a__b",): TEXT, ("a", "b"): TEXT},
    ]:
        with pytest.raises(ValueError, match="load into the same column"):
            check_names(clash, naming)


def test_drop_null_records() -> None:
    paths = record_paths({("a", "b"): TEXT})
    assert paths == [("a",)]
    row: Dict[str, Any] = {"a": None, "x": None}
    drop_null_records(row, paths)
    assert row == {"x": None}
    row = {"a": {"b": None}}
    drop_null_records(row, paths)
    assert row == {"a": {"b": None}}

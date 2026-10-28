import json
import os
import sys
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import pytest
import psycopg2

from dlt.common import Decimal
from dlt.common.schema.typing import TColumnType
from dlt.sources.credentials import ConnectionStringCredentials

from sources.pg_replication.exceptions import UnchangedToastException
from sources.pg_replication.helpers import (
    MessageConsumer,
    Segment,
    segment_to_rows,
    stream_changes,
)
from sources.pg_replication.schema_types import (
    PgType,
    get_value_converter,
    parse_array,
    to_dlt_column_type,
)

with open(
    os.path.join(os.path.dirname(__file__), "pgoutput_messages.json"), encoding="utf-8"
) as f:
    MESSAGES: Dict[str, List[List[Any]]] = json.load(f)
"""Messages captured from Postgres 17 by scenario with `capture_messages.py`."""

ALL_OPS = {"insert": True, "update": True, "delete": True}
INSERT_ONLY = {"insert": True, "update": False, "delete": False}


def consume(scenario: str, **kwargs: Any) -> MessageConsumer:
    consumer = MessageConsumer(
        pub_ops=kwargs.pop("pub_ops", ALL_OPS), upto_lsn=2**63, **kwargs
    )
    for data_start, payload in MESSAGES[scenario]:
        consumer.process_msg(bytes.fromhex(payload), data_start)
    return consumer


def change_lsns(scenario: str) -> List[int]:
    return [
        lsn for lsn, payload in MESSAGES[scenario] if payload[:2] in ("49", "55", "44")
    ]


BIG = "x" * 2500
ITEM = {
    "id": 1,
    "name": "one",
    "amount": Decimal("1.25"),
    "ts": datetime(2024, 1, 2, 8, 11, 12, 500000, tzinfo=timezone.utc),
    "flag": True,
    "doc": {"a": [1, 2]},
    "tags": ["a", "b c", None],
    "mood": "ok",
    "note": "n1",
    "data": b"\xde\xad",
    "day": date(2024, 1, 2),
}

EXPECTED_SEGMENTS: Dict[str, List[Tuple[str, List[Dict[str, Any]]]]] = {
    "basic": [
        (
            "items",
            [
                ITEM,
                {**dict.fromkeys(ITEM), "id": 2, "day": date.max},
                {**ITEM, "name": "uno"},
                # key-only delete gets dummy values in non-key columns
                {
                    "id": 2,
                    "name": "",
                    "amount": Decimal(0),
                    "ts": datetime(2000, 1, 1, tzinfo=timezone.utc),
                    "flag": True,
                    "doc": [0],
                    "tags": [0],
                    "mood": "",
                    "note": "",
                    "data": b" ",
                    "day": date(2000, 1, 1),
                    "deleted": True,
                },
            ],
        )
    ],
    "pk_update": [
        (
            "items",
            [
                {"id": 1, "name": "one"},
                {"id": 1, "name": "", "deleted": True},
                {"id": 10, "name": "one"},
            ],
        )
    ],
    "toast_full": [
        (
            "items",
            [{"id": 1, "small": "a", "big": BIG}, {"id": 1, "small": "b", "big": BIG}],
        )
    ],
    # the second Relation message of x starts a new segment with the new column
    "mid_txn_relation": [
        ("x", [{"id": 1}]),
        ("y", [{"id": 1}, {"id": 2}]),
        ("x", [{"id": 2, "c": 2}]),
    ],
    "dlt_table": [("items", [{"id": 1}])],
}
"""Rows of each captured scenario by segment, `deleted` marks deletes."""


def load_segment(segment: Segment, backend: str) -> List[Dict[str, Any]]:
    if backend == "python":
        return segment_to_rows(segment)
    from sources.pg_replication.arrow_helpers import segment_to_arrow

    rows: List[Dict[str, Any]] = segment_to_arrow(segment).to_pylist()
    json_columns = [
        name
        for name, c in segment.relation.column_schemas.items()
        if c["data_type"] == "json"
    ]
    for row in rows:
        for name in json_columns:
            if row[name] is not None:
                row[name] = json.loads(row[name])
    return rows


@pytest.mark.parametrize("backend", ["python", "pyarrow"])
@pytest.mark.parametrize("scenario", list(EXPECTED_SEGMENTS))
def test_captured_messages(scenario: str, backend: str) -> None:
    if backend == "pyarrow":
        pytest.importorskip("pyarrow")
    consumer = consume(scenario, resolve_primary_key=lambda relation_id: ["id"])
    assert not consumer.in_transaction
    lsns = set(change_lsns(scenario))
    loaded = []
    for segment in consumer.take_batch():
        rows = []
        for row in load_segment(segment, backend):
            assert row.pop("lsn") in lsns
            if row.pop("deleted_ts", None) is not None:
                row["deleted"] = True
            rows.append(row)
        loaded.append((segment.relation.table_name, rows))
    assert loaded == EXPECTED_SEGMENTS[scenario]


def test_column_hints() -> None:
    (segment,) = consume("basic").take_batch()
    schema = segment.relation.column_schemas
    assert schema["id"] == {"name": "id", "data_type": "bigint", "primary_key": True}
    assert schema["amount"]["precision"] == 10 and schema["amount"]["scale"] == 2
    assert schema["ts"]["timezone"] is True
    assert schema["tags"]["data_type"] == "json"
    assert schema["mood"]["data_type"] == "text"
    assert schema["note"]["precision"] == 20
    assert schema["lsn"]["dedup_sort"] == "desc"
    assert schema["deleted_ts"]["hard_delete"] is True
    assert segment.relation.write_disposition == "merge"


def test_unchanged_toast_from_full_old_tuple() -> None:
    (segment,) = consume(
        "toast_full", resolve_primary_key=lambda relation_id: ["id"]
    ).take_batch()
    # with REPLICA IDENTITY FULL all columns are flagged as key
    assert [
        name
        for name, c in segment.relation.column_schemas.items()
        if c.get("primary_key")
    ] == ["id"]


def test_unchanged_toast_without_old_row_raises() -> None:
    with pytest.raises(UnchangedToastException) as exc:
        consume("toast_default")
    assert exc.value.table_name == "items"
    assert exc.value.column_names == ["big"]
    assert "REPLICA IDENTITY FULL" in str(exc.value)


def test_batch_ready_only_after_commit() -> None:
    consumer = MessageConsumer(
        pub_ops=INSERT_ONLY, upto_lsn=2**63, target_batch_size=1
    )
    loaded: Dict[str, List[int]] = {}
    for data_start, payload in MESSAGES["mid_txn_relation"]:
        consumer.process_msg(bytes.fromhex(payload), data_start)
        if consumer.batch_ready:
            assert payload.startswith("43")  # C
            assert consumer.last_commit_lsn == data_start
            for segment in consumer.take_batch():
                loaded.setdefault(segment.relation.table_name, []).extend(
                    row["id"] for row in segment_to_rows(segment)
                )
    assert loaded == {"x": [1, 2], "y": [1, 2]}


def test_upto_lsn_stops_at_commit() -> None:
    first_commit = next(
        lsn for lsn, payload in MESSAGES["basic"] if payload.startswith("43")
    )
    consumer = MessageConsumer(pub_ops=ALL_OPS, upto_lsn=first_commit)
    for data_start, payload in MESSAGES["basic"]:
        consumer.process_msg(bytes.fromhex(payload), data_start)
        if consumer.consumed_all:
            break
    assert consumer.last_commit_lsn == first_commit
    assert consumer.n_items == 2


@pytest.mark.parametrize("op", [b"Y", b"O", b"M"], ids=["type", "origin", "message"])
def test_ignored_messages(op: bytes) -> None:
    consumer = MessageConsumer(pub_ops=ALL_OPS, upto_lsn=2**63)
    consumer.process_msg(op + b"\x00" * 16, 1)
    assert consumer.take_batch() == []


def test_unknown_message_raises() -> None:
    with pytest.raises(ValueError):
        MessageConsumer(pub_ops=ALL_OPS, upto_lsn=2**63).process_msg(b"X", 1)


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


def _insert_message(values: List[Any]) -> bytes:
    message = b"I" + (16384).to_bytes(4, "big") + b"N" + len(values).to_bytes(2, "big")
    for value in values:
        if value is None:
            message += b"n"
        else:
            encoded = value.encode("utf-8")
            message += b"t" + len(encoded).to_bytes(4, "big") + encoded
    return message


@pytest.mark.parametrize("chunk_size", [None, 70], ids=["one-chunk", "three-chunks"])
def test_insert_columns_match_python_parser(
    chunk_size: Optional[int], monkeypatch: pytest.MonkeyPatch
) -> None:
    pytest.importorskip("pyarrow")
    from sources.pg_replication import arrow_helpers
    from sources.pg_replication.arrow_helpers import insert_columns
    from sources.pg_replication.decoders import read_tuple

    if chunk_size:
        monkeypatch.setattr(arrow_helpers, "INSERT_CHUNK_SIZE", chunk_size)

    rows = [
        ["1", "short", None],
        [None, "", "ü 🦆 ünïcödé"],
        ["3", "x" * 1000, ""],
        ["4", None, None],
    ]
    payloads = [_insert_message(row) for row in rows]
    assert [read_tuple(payload, 6)[0] for payload in payloads] == rows
    if chunk_size:
        # messages of 25 and 38 bytes share a chunk, the 1024 byte message is alone
        chunks = list(arrow_helpers._split_payloads(payloads))
        assert [len(chunk) for chunk in chunks] == [2, 1, 1]
    columns = insert_columns(payloads, 3)
    assert [list(values) for values in zip(*(c.to_pylist() for c in columns))] == rows


def test_python_backend_does_not_import_pyarrow(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setitem(sys.modules, "pyarrow", None)
    monkeypatch.delitem(
        sys.modules, "sources.pg_replication.arrow_helpers", raising=False
    )
    credentials = ConnectionStringCredentials(
        "postgresql://loader:loader@127.0.0.1:1/dlt_data"
    )
    # fails on connect, after the backend was selected
    with pytest.raises(psycopg2.OperationalError):
        next(stream_changes(credentials, "slot", "pub", backend="python"))
    with pytest.raises(ImportError):
        next(stream_changes(credentials, "slot", "pub", backend="pyarrow"))

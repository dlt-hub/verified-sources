import json
import os
import sys
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Tuple

import pytest
import psycopg2

from dlt.common import Decimal
from dlt.sources.credentials import ConnectionStringCredentials

from sources.pg_replication.exceptions import (
    NoPrimaryKeyException,
    UnchangedToastException,
)
from sources.pg_replication.helpers import (
    MessageConsumer,
    Segment,
    segment_to_rows,
    stream_changes,
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

WITHOUT_KEY = {"mid_txn_relation": "x", "dlt_table": "items"}
"""Scenarios with tables without a primary key and the first such table."""


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
    consumer = consume(
        scenario,
        pub_ops=INSERT_ONLY if scenario in WITHOUT_KEY else ALL_OPS,
        resolve_primary_key=lambda relation_id: ["id"],
    )
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


@pytest.mark.parametrize("scenario", list(WITHOUT_KEY))
def test_table_without_key_raises(scenario: str) -> None:
    with pytest.raises(NoPrimaryKeyException) as exc:
        consume(scenario)
    assert exc.value.table_names == [WITHOUT_KEY[scenario]]
    assert "REPLICA IDENTITY USING INDEX" in str(exc.value)


@pytest.mark.parametrize(
    "column_filter",
    [{}, {"exclude_columns": {"items": ["small"]}}],
    ids=["all_columns", "other_column_excluded"],
)
def test_unchanged_toast_without_old_row_raises(column_filter: Dict[str, Any]) -> None:
    with pytest.raises(UnchangedToastException) as exc:
        consume("toast_default", **column_filter)
    assert exc.value.table_name == "items"
    assert exc.value.column_names == ["big"]
    assert "REPLICA IDENTITY FULL" in str(exc.value)


@pytest.mark.parametrize("backend", ["python", "pyarrow"])
@pytest.mark.parametrize(
    "column_filter",
    [
        {"exclude_columns": {"items": ["big"]}},
        {"include_columns": {"items": ["id", "small"]}},
    ],
    ids=["exclude", "include"],
)
def test_unchanged_toast_of_excluded_column(
    column_filter: Dict[str, Any], backend: str
) -> None:
    if backend == "pyarrow":
        pytest.importorskip("pyarrow")
    (segment,) = consume("toast_default", **column_filter).take_batch()
    rows = load_segment(segment, backend)
    assert [(row["id"], row["small"]) for row in rows] == [(1, "a"), (1, "b")]
    assert not any("big" in row for row in rows)


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

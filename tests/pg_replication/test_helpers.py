import json
import os
import sys
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Tuple

import pytest
import psycopg2

from dlt.common import Decimal
from dlt.common.schema import Schema
from dlt.common.schema.typing import TTableSchemaColumns
from dlt.common.schema.utils import new_table
from dlt.sources.credentials import ConnectionStringCredentials

from sources.pg_replication import helpers
from sources.pg_replication.decoders import decode_begin
from sources.pg_replication.exceptions import (
    ChangePosition,
    IncompatibleColumnTypeException,
    NoPrimaryKeyException,
    NullInNotNullColumnException,
    UnchangedToastException,
    ValueConversionException,
)
from sources.pg_replication.helpers import (
    MessageConsumer,
    Segment,
    segment_to_rows,
    stream_changes,
)
from sources.pg_replication.schema_types import TReflectionLevel

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


def final_lsns(scenario: str, ops: Tuple[str, ...] = ("49", "55", "44")) -> List[int]:
    """Returns the final LSN of the transaction of each change with an op in `ops`."""
    lsns, final_lsn = [], 0
    for _, payload in MESSAGES[scenario]:
        if payload.startswith("42"):
            final_lsn = decode_begin(bytes.fromhex(payload)).final_lsn
        elif payload[:2] in ops:
            lsns.append(final_lsn)
    return lsns


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
            assert row.pop("_dlt_lsn") in lsns
            if row.pop("_dlt_deleted_ts", None) is not None:
                row["deleted"] = True
            rows.append(row)
        loaded.append((segment.relation.table_name, rows))
    assert loaded == EXPECTED_SEGMENTS[scenario]


@pytest.mark.parametrize("reflection_level", ["full", "full_with_precision"])
def test_column_hints(reflection_level: TReflectionLevel) -> None:
    (segment,) = consume("basic", reflection_level=reflection_level).take_batch()
    schema = segment.relation.column_schemas
    assert schema["id"] == {"name": "id", "data_type": "bigint", "primary_key": True}
    assert schema["amount"]["precision"] == 10 and schema["amount"]["scale"] == 2
    assert schema["ts"]["timezone"] is True
    assert schema["tags"]["data_type"] == "json"
    assert schema["mood"]["data_type"] == "text"
    if reflection_level == "full":
        assert "precision" not in schema["note"]
    else:
        assert schema["note"]["precision"] == 20
    assert schema["_dlt_lsn"]["dedup_sort"] == "desc"
    assert schema["_dlt_deleted_ts"]["hard_delete"] is True
    assert segment.relation.write_disposition == "merge"


def destination_schema(columns: TTableSchemaColumns) -> Schema:
    """Returns a schema with the `items` table loaded before with `columns`."""
    schema = Schema("snapshot")
    columns = {
        "_dlt_load_id": {
            "name": "_dlt_load_id",
            "data_type": "text",
            "nullable": False,
        },
        "_dlt_id": {"name": "_dlt_id", "data_type": "text", "nullable": False},
        **columns,
    }
    schema.update_table(new_table("items", columns=list(columns.values())))
    return schema


def test_columns_keep_destination_types() -> None:
    schema = destination_schema(
        {
            "amount": {"name": "amount", "data_type": "decimal"},
            "note": {"name": "note", "data_type": "text", "precision": 50},
        }
    )
    (segment,) = consume(
        "basic", reflection_level="full_with_precision", destination_schema=schema
    ).take_batch()
    columns = segment.relation.column_schemas
    assert columns["amount"] == {
        "name": "amount",
        "data_type": "decimal",
        "primary_key": False,
    }
    assert columns["note"]["precision"] == 50
    # not loaded before, so inferred
    assert columns["mood"] == {
        "name": "mood",
        "data_type": "text",
        "primary_key": False,
    }


def test_column_type_that_does_not_fit_raises() -> None:
    schema = destination_schema(
        {
            "amount": {
                "name": "amount",
                "data_type": "decimal",
                "precision": 5,
                "scale": 1,
            }
        }
    )
    with pytest.raises(IncompatibleColumnTypeException) as exc:
        consume("basic", destination_schema=schema)
    assert exc.value.column_name == "amount"
    assert exc.value.destination_type == "decimal(5,1)"
    assert exc.value.new_type == "decimal(10,2)"
    suggested = {
        "items": {"amount": {"data_type": "decimal", "precision": 10, "scale": 2}}
    }
    assert exc.value.columns == suggested
    assert f"`columns={suggested!r}`" in str(exc.value)
    # the suggested `columns` load the new type
    (segment,) = consume(
        "basic", destination_schema=schema, columns=suggested
    ).take_batch()
    amount = segment.relation.column_schemas["amount"]
    assert (amount["precision"], amount["scale"]) == (10, 2)


def test_legacy_columns_lose_hints() -> None:
    schema = destination_schema(
        {
            "lsn": {"name": "lsn", "data_type": "bigint", "dedup_sort": "desc"},
            "deleted_ts": {
                "name": "deleted_ts",
                "data_type": "timestamp",
                "hard_delete": True,
            },
        }
    )
    (segment,) = consume("basic", destination_schema=schema).take_batch()
    columns = segment.relation.column_schemas
    assert columns["lsn"] == {"name": "lsn", "dedup_sort": None}
    assert columns["_dlt_lsn"]["dedup_sort"] == "desc"
    assert columns["deleted_ts"] == {"name": "deleted_ts", "hard_delete": False}
    assert columns["_dlt_deleted_ts"]["hard_delete"] is True
    # other hints of columns with these names are kept
    hints = {"lsn": {"data_type": "text"}, "deleted_ts": {"data_type": "bigint"}}
    (segment,) = consume(
        "basic", destination_schema=schema, columns={"items": hints}
    ).take_batch()
    columns = segment.relation.column_schemas
    assert columns["lsn"] == {"name": "lsn", "data_type": "text", "dedup_sort": None}
    assert columns["deleted_ts"] == {
        "name": "deleted_ts",
        "data_type": "bigint",
        "hard_delete": False,
    }


def test_not_null_column_without_values_raises() -> None:
    schema = destination_schema(
        {"old_name": {"name": "old_name", "data_type": "text", "nullable": False}}
    )
    with pytest.raises(NullInNotNullColumnException) as exc:
        consume("basic", destination_schema=schema)
    assert exc.value.column_names == ["old_name"]
    # the user made the destination column nullable
    consume(
        "basic",
        destination_schema=schema,
        columns={"items": {"old_name": {"nullable": True}}},
    )


@pytest.mark.parametrize("backend", ["python", "pyarrow"])
def test_null_in_not_null_column_raises(backend: str) -> None:
    if backend == "pyarrow":
        pytest.importorskip("pyarrow")
    schema = destination_schema(
        {"note": {"name": "note", "data_type": "text", "nullable": False}}
    )
    (segment,) = consume("basic", destination_schema=schema).take_batch()
    assert segment.relation.not_null_columns == ["note"]
    # the second insert has NULL in all columns but the key
    with pytest.raises(NullInNotNullColumnException) as exc:
        load_segment(segment, backend)
    assert exc.value.column_names == ["note"]
    final_lsn = final_lsns("basic", ("49",))[1]
    assert exc.value.position == ChangePosition("", final_lsn, final_lsn)
    assert "pg_replication_slot_advance" in str(exc.value)


@pytest.mark.parametrize(
    "backend,fails_in_python",
    [("python", True), ("pyarrow", True), ("pyarrow", False)],
    ids=["python", "pyarrow", "pyarrow-only"],
)
def test_conversion_error_names_transaction(
    backend: str, fails_in_python: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    if backend == "pyarrow":
        arrow_helpers = pytest.importorskip("sources.pg_replication.arrow_helpers")

        def fail_in_arrow(*args: Any) -> None:
            raise ValueError("arrow cannot parse it")

        monkeypatch.setattr(arrow_helpers, "to_castable_array", fail_in_arrow)
    if fails_in_python:
        get_value_converter = helpers.get_value_converter

        def failing_numeric(pg_type: Any) -> Any:
            if pg_type.type_id != 1700:
                return get_value_converter(pg_type)

            def convert(value: str) -> None:
                raise ValueError("python cannot parse it")

            return convert

        monkeypatch.setattr(helpers, "get_value_converter", failing_numeric)
    (segment,) = consume("basic").take_batch()
    with pytest.raises(ValueConversionException) as exc:
        load_segment(segment, backend)
    lsns = final_lsns("basic")
    if fails_in_python:
        # the first insert has the first numeric value
        assert exc.value.column_name == "amount"
        assert exc.value.position == ChangePosition("", lsns[0], lsns[0])
    else:
        assert exc.value.column_name is None
        assert exc.value.position == ChangePosition("", min(lsns), max(lsns))
    assert exc.value.position.skip_lsn in str(exc.value)


def test_reflect_not_null_in_new_tables() -> None:
    def not_null(relation_id: int) -> List[str]:
        return ["id", "name"]

    (segment,) = consume(
        "basic", reflect_not_null=True, resolve_not_null=not_null
    ).take_batch()
    assert segment.relation.column_schemas["name"]["nullable"] is False
    assert segment.relation.not_null_columns == ["name"]
    # a NOT NULL column can't be added to a table loaded before
    schema = destination_schema({"id": {"name": "id", "data_type": "bigint"}})
    (segment,) = consume(
        "basic",
        reflect_not_null=True,
        resolve_not_null=not_null,
        destination_schema=schema,
    ).take_batch()
    assert "nullable" not in segment.relation.column_schemas["name"]


@pytest.mark.parametrize("backend", ["python", "pyarrow"])
def test_soft_delete(backend: str) -> None:
    if backend == "pyarrow":
        pytest.importorskip("pyarrow")
    (segment,) = consume("basic", hard_delete=False).take_batch()
    assert "hard_delete" not in segment.relation.column_schemas["_dlt_deleted_ts"]
    (deleted,) = [
        row for row in load_segment(segment, backend) if row.get("_dlt_deleted_ts")
    ]
    # a key-only delete keeps NULLs, no dummy values
    assert {name: deleted[name] for name in ITEM} == {**dict.fromkeys(ITEM), "id": 2}


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
        consume("toast_default", slot_name="slot", **column_filter)
    assert exc.value.table_name == "items"
    assert exc.value.column_names == ["big"]
    assert "REPLICA IDENTITY FULL" in str(exc.value)
    final_lsn = final_lsns("toast_default", ("55",))[0]
    assert exc.value.position == ChangePosition("slot", final_lsn, final_lsn)
    assert (
        f"pg_replication_slot_advance('slot', '{exc.value.position.skip_lsn}')"
        in str(exc.value)
    )


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

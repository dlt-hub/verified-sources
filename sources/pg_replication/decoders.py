"""Decoders for pgoutput messages (protocol version 1) in text format.

https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html
"""

import struct
from datetime import datetime, timedelta, timezone
from typing import Any, List, NamedTuple, Optional, Tuple

_INT16 = struct.Struct(">h").unpack_from
_INT32 = struct.Struct(">i").unpack_from
_UINT32 = struct.Struct(">I").unpack_from
_INT64 = struct.Struct(">q").unpack_from

_PG_EPOCH = datetime(2000, 1, 1, tzinfo=timezone.utc)


class UnchangedToast:
    """Marks a TOASTed value that the update did not change, so pgoutput did not send it."""

    def __repr__(self) -> str:
        return "UNCHANGED_TOAST"


UNCHANGED_TOAST = UnchangedToast()

TupleValues = List[Any]
"""Column values in text format: `str`, `None` for NULL or `UNCHANGED_TOAST`."""


class ColumnType(NamedTuple):
    part_of_pkey: bool
    name: str
    type_id: int
    atttypmod: int


class Relation(NamedTuple):
    relation_id: int
    namespace: str
    relation_name: str
    replica_identity: str
    columns: List[ColumnType]


class Begin(NamedTuple):
    final_lsn: int
    commit_ts: datetime
    xid: int


class Change(NamedTuple):
    """Insert, update or delete. `old_kind` is `K` (key), `O` (full old row) or None."""

    op: str
    relation_id: int
    old_kind: Optional[str]
    old_values: Optional[TupleValues]
    new_values: Optional[TupleValues]


def read_string(buf: bytes, offset: int) -> Tuple[str, int]:
    end = buf.index(0, offset)
    return buf[offset:end].decode("utf-8"), end + 1


def read_tuple(buf: bytes, offset: int) -> Tuple[TupleValues, int]:
    """Reads TupleData starting at `offset`, returns values and offset after the tuple."""
    n_columns = _INT16(buf, offset)[0]
    offset += 2
    values: TupleValues = []
    append = values.append
    for _ in range(n_columns):
        kind = buf[offset]
        if kind == 116:  # t
            length = _INT32(buf, offset + 1)[0]
            offset += 5
            append(buf[offset : offset + length].decode("utf-8"))
            offset += length
        elif kind == 110:  # n
            append(None)
            offset += 1
        elif kind == 117:  # u
            append(UNCHANGED_TOAST)
            offset += 1
        else:
            raise ValueError(
                f"Unsupported tuple data kind {chr(kind)!r}, text format expected."
            )
    return values, offset


def decode_begin(buf: bytes) -> Begin:
    return Begin(
        final_lsn=_INT64(buf, 1)[0],
        commit_ts=_PG_EPOCH + timedelta(microseconds=_INT64(buf, 9)[0]),
        xid=_UINT32(buf, 17)[0],
    )


def decode_relation(buf: bytes) -> Relation:
    relation_id = _UINT32(buf, 1)[0]
    namespace, offset = read_string(buf, 5)
    relation_name, offset = read_string(buf, offset)
    replica_identity = chr(buf[offset])
    n_columns = _INT16(buf, offset + 1)[0]
    offset += 3
    columns = []
    for _ in range(n_columns):
        flags = buf[offset]
        name, offset = read_string(buf, offset + 1)
        columns.append(
            ColumnType(
                part_of_pkey=bool(flags & 1),
                name=name,
                type_id=_UINT32(buf, offset)[0],
                atttypmod=_INT32(buf, offset + 4)[0],
            )
        )
        offset += 8
    return Relation(relation_id, namespace, relation_name, replica_identity, columns)


def decode_change(buf: bytes) -> Change:
    """Decodes Insert, Update and Delete messages."""
    op = chr(buf[0])
    relation_id = _UINT32(buf, 1)[0]
    kind = chr(buf[5])
    old_kind = old_values = new_values = None
    offset = 6
    if kind in ("K", "O"):
        old_kind = kind
        old_values, offset = read_tuple(buf, offset)
        if op == "U":
            # skip the "N" byte that precedes the new tuple
            offset += 1
            new_values, _ = read_tuple(buf, offset)
    else:
        new_values, _ = read_tuple(buf, offset)
    return Change(op, relation_id, old_kind, old_values, new_values)

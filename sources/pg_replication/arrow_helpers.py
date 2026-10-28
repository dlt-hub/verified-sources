"""Converts replicated changes to arrow tables for the `pyarrow` backend. Requires `pyarrow` and `numpy`.

Values arrive as text: inserts are decoded with numpy (`insert_columns`), values arrow cannot
parse are converted first (`to_castable_array`) and dlt casts the strings to the column types.
"""

from functools import partial
from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Optional, Sequence

import numpy as np
import pyarrow.compute as pc

from dlt.common import json, logger
from dlt.common.configuration.container import Container
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.libs.pyarrow import cast_arrow_as_columns_schema, pyarrow as pa

from .exceptions import NullInNotNullColumnException
from .schema_types import (
    _DUMMY_VALS,
    DELETED_TS,
    PgType,
    TValueConverter,
    get_value_converter,
)

if TYPE_CHECKING:
    from .helpers import Segment

_SPECIAL_DATETIMES = ["infinity", "-infinity"]

INSERT_CHUNK_SIZE = 8 * 1024 * 1024
"""Max bytes of Insert messages decoded at once. The index array takes 8 bytes per byte of a column."""

_TEXT_KIND = ord("t")
_NULL_KIND = ord("n")


def _gather_index(starts: Any, lengths: Any, offsets: Any) -> Any:
    """Returns the index of every value byte in the source buffer, values back to back.

    Value i is at `starts[i]:starts[i] + lengths[i]` in the source and must land at
    `offsets[i]:offsets[i + 1]` in the result. Within a value the index grows by one per byte,
    so the index is a running sum of steps: 1 for every byte, except the first byte of a value,
    where the step jumps from the last byte of the previous value to the start of this one.
    The steps are summed in place, so the index is the only array of the size of the values.
    """
    index = np.ones(offsets[-1], dtype=np.int64)
    has_bytes = lengths > 0
    if has_bytes.any():
        first_bytes = offsets[:-1][has_bytes]
        starts = starts[has_bytes]
        ends = starts + lengths[has_bytes]
        # the first non empty value is at offset 0 of the result
        index[0] = starts[0]
        index[first_bytes[1:]] = starts[1:] - ends[:-1] + 1
        np.cumsum(index, out=index)
    return index


def _read_int32(data: Any, at: Any) -> Any:
    """Reads big endian int32 values starting at each offset in `at`."""
    return (
        (data[at].astype(np.int64) << 24)
        | (data[at + 1].astype(np.int64) << 16)
        | (data[at + 2].astype(np.int64) << 8)
        | data[at + 3]
    )


def insert_columns(
    payloads: List[bytes], n_columns: int, indices: Sequence[int]
) -> Dict[int, Any]:
    """Decodes columns `indices` of Insert messages of one table into chunked arrow string arrays.

    Messages are decoded in chunks of at most `INSERT_CHUNK_SIZE` bytes, so temporary arrays stay
    bounded also for large transactions.
    """
    chunks = [
        _decode_insert_chunk(chunk, n_columns, indices)
        for chunk in _split_payloads(payloads)
    ]
    return {
        idx: pa.chunked_array([chunk[idx] for chunk in chunks], type=pa.large_string())
        for idx in indices
    }


def _split_payloads(payloads: List[bytes]) -> Iterator[List[bytes]]:
    chunk: List[bytes] = []
    size = 0
    for payload in payloads:
        if chunk and size + len(payload) > INSERT_CHUNK_SIZE:
            yield chunk
            chunk, size = [], 0
        chunk.append(payload)
        size += len(payload)
    if chunk:
        yield chunk


def _decode_insert_chunk(
    payloads: List[bytes], n_columns: int, indices: Sequence[int]
) -> Dict[int, Any]:
    """Decodes columns `indices` of Insert messages into arrow string arrays.

    A message is `"I" | relation id (int32) | "N" | n columns (int16) | columns` (protocol 1,
    text), a column is `"n"` (NULL) or `"t" | length (int32) | utf-8 value`. All messages are
    walked together, one column at a time, so python runs once per column, not per value.
    """
    wanted = set(indices)
    n = len(payloads)
    data = np.frombuffer(b"".join(payloads), dtype=np.uint8)
    message_lengths = np.fromiter(map(len, payloads), dtype=np.int64, count=n)
    message_starts = np.zeros(n, dtype=np.int64)
    np.cumsum(message_lengths[:-1], out=message_starts[1:])

    # the number of columns is an int16 after "I", relation id and "N"
    counts = (data[message_starts + 6].astype(np.int64) << 8) | data[message_starts + 7]
    if not (counts == n_columns).all():
        raise ValueError(
            f"Insert messages have {set(counts.tolist())} columns, {n_columns} expected."
        )
    # offset of the current column in every message
    pos = message_starts + 8

    columns: Dict[int, Any] = {}
    for idx in range(n_columns):
        kind = data[pos]
        is_text = kind == _TEXT_KIND
        if not (is_text | (kind == _NULL_KIND)).all():
            raise ValueError("Insert messages must contain only text and NULL values.")
        value_lengths = np.zeros(n, dtype=np.int64)
        value_lengths[is_text] = _read_int32(data, pos[is_text] + 1)
        value_starts = pos + 5

        if idx in wanted:
            # value i is stored at `offsets[i]:offsets[i + 1]` in the arrow buffer
            offsets = np.zeros(n + 1, dtype=np.int64)
            np.cumsum(value_lengths, out=offsets[1:])
            # copies the value bytes of all messages with one indexing operation
            values = data[_gather_index(value_starts, value_lengths, offsets)]
            array = pa.LargeBinaryArray.from_buffers(
                pa.large_binary(),
                n,
                [
                    # validity bitmap: one bit per value, set for text values, unset for NULLs
                    pa.py_buffer(np.packbits(is_text, bitorder="little")),
                    pa.py_buffer(offsets),
                    pa.py_buffer(values),
                ],
                null_count=int(n - is_text.sum()),
            )
            # binary to string cast validates utf-8
            columns[idx] = array.cast(pa.large_string())
        # move past the column in every message
        pos = np.where(is_text, value_starts + value_lengths, pos + 1)
    return columns


def _array_to_json(value: str, to_list: TValueConverter) -> str:
    return json.dumps(to_list(value))


def _normalize_json(value: str) -> str:
    return json.dumps(json.loads(value))


def _convert_in_python(
    array: Any, convert: Any, arrow_type: Optional[Any] = None
) -> Any:
    return pa.array(
        [None if value is None else convert(value) for value in array.to_pylist()],
        type=arrow_type,
    )


def to_castable_array(
    array: Any, pg_type: PgType, column_name: str, table_name: str
) -> Any:
    """Converts Postgres text values that arrow cannot cast to the dlt column type.

    Other values (numbers, dates, timestamps with offsets, text) stay strings and are cast by
    `cast_arrow_as_columns_schema`. Values that need python conversion use the converters of the
    `python` backend, so both backends load the same values.
    """
    type_id = pg_type.type_id
    if pg_type.element_type_id is not None:
        # array literals ie. `{1,2}` become json
        return _convert_in_python(
            array,
            partial(_array_to_json, to_list=get_value_converter(pg_type)),
            pa.large_string(),
        )
    if type_id in (114, 3802):
        # json and jsonb are serialized again, as in rows and snapshots
        return _convert_in_python(array, _normalize_json, pa.large_string())
    if type_id == 16:
        # booleans are sent as `t` and `f`
        return pc.equal(array, "t")
    if type_id in (17, 1083, 1266):
        # bytea is sent in hex format, arrow does not parse time from strings
        return _convert_in_python(array, get_value_converter(pg_type))
    if type_id in (1082, 1114, 1184):
        if pc.any(pc.is_in(array, value_set=pa.array(_SPECIAL_DATETIMES))).as_py():
            return _convert_in_python(array, get_value_converter(pg_type))
    if type_id == 1700:
        # arrow decimals have no NaN
        is_nan = pc.equal(array, "NaN")
        if pc.any(is_nan).as_py():
            logger.warning(
                f"Numeric NaN in column {column_name} of {table_name} loaded as NULL."
            )
            return pc.if_else(is_nan, pa.scalar(None, pa.large_string()), array)
    return array


def segment_to_arrow(segment: "Segment") -> Any:
    """Converts changes in a segment to an arrow table with types of the dlt columns.

    Rows of inserts come first, then rows of updates and deletes. Their order does not matter:
    the `lsn` column orders changes of the same row when merging.
    """
    relation = segment.relation
    n_inserts = len(segment.insert_payloads)
    inserted = (
        insert_columns(
            segment.insert_payloads, len(relation.columns), relation.included_indices
        )
        if n_inserts
        else None
    )
    arrays = {}
    for idx in relation.included_indices:
        name = relation.columns[idx].name
        array = pa.array([row[idx] for row in segment.rows], type=pa.large_string())
        if inserted is not None:
            array = pa.chunked_array(inserted[idx].chunks + [array])
        arrays[name] = to_castable_array(
            array, relation.pg_types[idx], name, relation.table_name
        )
    arrays["lsn"] = pa.array(segment.insert_lsns + segment.row_lsns, type=pa.int64())
    if DELETED_TS in relation.column_schemas:
        arrays[DELETED_TS] = pa.array(
            [None] * n_inserts + segment.row_deleted_ts,
            type=pa.timestamp("us", tz="UTC"),
        )
    caps = (
        Container().get(DestinationCapabilitiesContext)
        or DestinationCapabilitiesContext.generic_capabilities()
    )
    table = cast_arrow_as_columns_schema(
        pa.table(arrays), relation.column_schemas, caps, "UTC"
    )

    is_deleted = pa.array(
        [False] * n_inserts + [ts is not None for ts in segment.row_deleted_ts]
    )
    hard_delete = relation.column_schemas.get(DELETED_TS, {}).get("hard_delete")
    if hard_delete and pc.any(is_deleted).as_py():
        table = _fill_deleted_rows(table, segment, is_deleted)
    if nulls := [
        name for name in relation.not_null_columns if table.column(name).null_count
    ]:
        raise NullInNotNullColumnException(
            relation.schema_name, relation.table_name, nulls
        )
    return table


def _fill_deleted_rows(table: Any, segment: "Segment", is_deleted: Any) -> Any:
    """Replaces NULLs in deleted rows with dummy values, same as the `python` backend.

    Deletes send only key columns, NULLs in other columns would violate NOT NULL constraints
    of the staging table.
    """
    relation = segment.relation
    for idx in relation.included_indices:
        name = relation.columns[idx].name
        column = table.column(name)
        fill = pc.and_(is_deleted, pc.is_null(column))
        if not pc.any(fill).as_py():
            continue
        dummy = _DUMMY_VALS[relation.column_schemas[name].get("data_type", "text")]
        is_string = pa.types.is_string(column.type) or pa.types.is_large_string(
            column.type
        )
        if is_string and not isinstance(dummy, str):
            # json columns are strings in arrow tables
            dummy = json.dumps(dummy)
        column = pc.if_else(fill, pa.scalar(dummy, column.type), column)
        table = table.set_column(table.schema.get_field_index(name), name, column)
    return table

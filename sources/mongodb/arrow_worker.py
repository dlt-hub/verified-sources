"""Parses raw BSON into Arrow tables. Imports neither dlt nor other modules of this source, so the
worker processes of `parallel` Arrow reads run this file by path and start fast."""

import json
import pickle
import struct
import sys
import traceback
import warnings
from functools import cache
from inspect import signature
from typing import (
    IO,
    Any,
    Dict,
    Iterable,
    Iterator,
    List,
    NamedTuple,
    Optional,
    Set,
    Tuple,
)

import pyarrow
import pyarrow.compute
import pyarrow.ipc
from bson import decode_all
from pymongo import MongoClient
from pymongoarrow.context import PyMongoArrowContext  # type: ignore
from pymongoarrow.schema import Schema  # type: ignore
from pymongoarrow.types import (  # type: ignore
    _is_binary,
    _is_code,
    _is_decimal128,
    _is_objectid,
)

_HEADER = struct.Struct(">I")
_BODY = struct.Struct(">Q")


class Mismatch(NamedTuple):
    """Fields with values that do not match their type in the schema, `a.b` for nested ones"""

    fields: Dict[str, List[str]]
    details: str
    with_schema: bool


class WorkerError(Exception):
    pass


def parse_bson_batches(
    batches: Iterable[bytes],
    pymongoarrow_schema: Any,
    codec_options: Any,
    strict: bool = False,
) -> Any:
    """Parse raw BSON batches into a single Arrow table with a new pymongoarrow context.

    Args:
        batches (Iterable[bytes]): Raw BSON batches, i.e. from `Collection.find_raw_batches`.
        pymongoarrow_schema (Any): The mapping of field types to convert BSON to Arrow.
        codec_options (CodecOptions): Codec options of the collection.
        strict (bool): Raise `TypeError` on values that do not match `pymongoarrow_schema` instead
            of loading them as null. Needs `pymongoarrow>=1.9`.

    Returns:
        pyarrow.lib.Table: The table with the BSON extension types of pymongoarrow.
    """
    # pymongoarrow 1.6 replaced `from_schema` and `lib.process_bson_stream` and stopped resetting
    # the context on `finish()`
    if hasattr(PyMongoArrowContext, "from_schema"):
        from pymongoarrow.lib import process_bson_stream  # type: ignore

        context = PyMongoArrowContext.from_schema(
            schema=pymongoarrow_schema, codec_options=codec_options
        )
        for batch in batches:
            process_bson_stream(batch, context)
    else:
        kwargs = {"allow_invalid": not strict} if _supports_strict_schema() else {}
        context = PyMongoArrowContext(
            pymongoarrow_schema, codec_options=codec_options, **kwargs
        )
        for batch in batches:
            context.process_bson_stream(batch)
    return context.finish()


def parse_batch(
    batches: List[bytes],
    pymongoarrow_schema: Any,
    codec_options: Any,
    detect_mismatch: bool,
) -> Tuple[Any, Optional[Mismatch]]:
    """Parse raw BSON batches into a table with plain Arrow types.

    Without `pymongoarrow_schema` the types are inferred from the first value of each field, with
    `detect_mismatch` a second strict parse finds values of other types, which load as null. Values
    that do not match `pymongoarrow_schema` give no table.

    Returns:
        Tuple[Any, Optional[Mismatch]]: The table or None, and the fields with mismatched values.
    """
    if pymongoarrow_schema is not None:
        try:
            table = parse_bson_batches(
                batches,
                pymongoarrow_schema,
                codec_options,
                strict=_supports_strict_schema(),
            )
        except TypeError as ex:
            return None, Mismatch(
                _mismatched_fields(batches, codec_options), str(ex), True
            )
        return convert_arrow_columns(table), None

    table = parse_bson_batches(batches, None, codec_options)
    mismatch = None
    if detect_mismatch and _supports_strict_schema():
        # the inferred schema loads values of another type as null, a strict one raises
        inferred = Schema({field.name: field.type for field in table.schema})
        try:
            parse_bson_batches(batches, inferred, codec_options, strict=True)
        except TypeError as ex:
            mismatch = Mismatch(
                _mismatched_fields(batches, codec_options), str(ex), False
            )
    return convert_arrow_columns(table), mismatch


def read_partition(
    collection: Any,
    filter_: Dict[str, Any],
    projection: Optional[Dict[str, Any]],
    batch_size: int,
    pymongoarrow_schema: Any,
    detect_mismatch: bool,
) -> Iterator[Tuple[Any, Optional[Mismatch]]]:
    """Parse the documents matching `filter_`, one table per raw batch of `batch_size`"""
    cursor = collection.find_raw_batches(
        filter=filter_, projection=projection, batch_size=batch_size
    )
    for batch in cursor:
        yield parse_batch(
            [batch], pymongoarrow_schema, collection.codec_options, detect_mismatch
        )


def convert_arrow_columns(table: Any) -> Any:
    """Convert the BSON extension types of pymongoarrow into Arrow types, also in nested columns.

    ObjectId is converted into its hex string, the same as `str(ObjectId)` in `convert_mongo_objs`,
    Decimal128 into its string, BSON binary and code into their binary and string storage.
    """
    for i, field in enumerate(table.schema):
        column = table.column(i)
        chunks = [_convert_arrow_array(chunk) for chunk in column.chunks]
        if all(chunk is original for chunk, original in zip(chunks, column.chunks)):
            continue
        table = table.set_column(
            i,
            pyarrow.field(field.name, chunks[0].type, field.nullable),
            pyarrow.chunked_array(chunks),
        )
    return table


def write_message(stream: IO[bytes], header: Dict[str, Any], table: Any = None) -> None:
    """Write a JSON header and an optional table as an Arrow IPC stream"""
    encoded = json.dumps(header).encode()
    stream.write(_HEADER.pack(len(encoded)))
    stream.write(encoded)
    if table is None:
        stream.write(_BODY.pack(0))
    else:
        sink = pyarrow.BufferOutputStream()
        with pyarrow.ipc.new_stream(sink, table.schema) as writer:
            writer.write_table(table)
        body = sink.getvalue()
        stream.write(_BODY.pack(body.size))
        stream.write(memoryview(body))
    stream.flush()


def read_message(stream: IO[bytes]) -> Tuple[Dict[str, Any], Any]:
    """Read a message of `write_message`, raises `EOFError` when the stream ends"""
    header = json.loads(_read_exact(stream, _read_size(stream, _HEADER)))
    size = _read_size(stream, _BODY)
    table = None
    if size:
        table = pyarrow.ipc.open_stream(
            pyarrow.py_buffer(_read_exact(stream, size))
        ).read_all()
    return header, table


def write_task(stream: IO[bytes], task: Dict[str, Any]) -> None:
    encoded = pickle.dumps(task)
    stream.write(_BODY.pack(len(encoded)))
    stream.write(encoded)
    stream.flush()


def read_task(stream: IO[bytes]) -> Optional[Dict[str, Any]]:
    """Read a task of `write_task`, None when the parent closed the stream"""
    try:
        size = _read_size(stream, _BODY)
    except EOFError:
        return None
    # tasks come only from the parent process that started this one
    return pickle.loads(_read_exact(stream, size))  # type: ignore[no-any-return]


def main() -> None:
    output = sys.stdout.buffer
    # stray prints would corrupt the messages
    sys.stdout = sys.stderr
    clients: Dict[str, Any] = {}
    while (task := read_task(sys.stdin.buffer)) is not None:
        try:
            url = task["connection_url"]
            if url not in clients:
                with warnings.catch_warnings():
                    # see `ignore_compressor_warnings` in helpers.py
                    warnings.filterwarnings("ignore", "Wire protocol compression with")
                    warnings.filterwarnings("ignore", "Unsupported compressor: $")
                    clients[url] = MongoClient(url, **task["client_options"])
            collection = clients[url][task["database"]][task["collection"]]
            for table, mismatch in read_partition(
                collection,
                task["filter"],
                task["projection"],
                task["batch_size"],
                task["pymongoarrow_schema"],
                task["detect_mismatch"],
            ):
                write_message(
                    output,
                    {"kind": "table", "mismatch": mismatch and mismatch._asdict()},
                    table,
                )
            write_message(output, {"kind": "done"})
        except Exception:
            write_message(output, {"kind": "error", "error": traceback.format_exc()})
            return


@cache
def _supports_strict_schema() -> bool:
    return "allow_invalid" in signature(PyMongoArrowContext.__init__).parameters


def _mismatched_fields(
    batches: List[bytes], codec_options: Any
) -> Dict[str, List[str]]:
    """Names the fields with values of more than one type, `a.b` for nested ones"""
    types: Dict[str, Set[str]] = {}

    def visit(value: Any, path: str) -> None:
        if value is None:
            return
        if isinstance(value, dict):
            type_name = "document"
            for key, item in value.items():
                visit(item, f"{path}.{key}")
        elif isinstance(value, list):
            type_name = "array"
            for item in value:
                visit(item, f"{path}[]")
        elif isinstance(value, int) and not isinstance(value, bool):
            type_name = "int"
        else:
            type_name = type(value).__name__
        types.setdefault(path, set()).add(type_name)

    for batch in batches:
        for document in decode_all(batch, codec_options):
            for key, value in document.items():
                visit(value, key)
    return {path: sorted(names) for path, names in types.items() if len(names) > 1}


def _convert_arrow_array(array: Any) -> Any:
    """Converts BSON extension types in `array`, returns `array` itself if there are none"""
    arrow_type = array.type
    if _is_objectid(arrow_type):
        return _objectid_to_hex(array.storage)
    if _is_decimal128(arrow_type):
        # there's no vectorized decoding of BSON decimals
        return pyarrow.array(
            [None if value is None else str(value) for value in array.to_pylist()],
            type=pyarrow.string(),
        )
    if _is_binary(arrow_type) or _is_code(arrow_type):
        return array.storage

    if not pyarrow.types.is_struct(arrow_type) and not pyarrow.types.is_list(
        arrow_type
    ):
        return array
    if array.offset:
        # nested arrays are not rebuilt from sliced offsets together with a null mask
        array = pyarrow.concat_arrays([array])
    if pyarrow.types.is_struct(arrow_type):
        children = array.flatten()
        converted = [_convert_arrow_array(child) for child in children]
        if all(new is old for new, old in zip(converted, children)):
            return array
        return pyarrow.StructArray.from_arrays(
            converted,
            fields=[
                pyarrow.field(field.name, child.type, field.nullable)
                for field, child in zip(arrow_type, converted)
            ],
            mask=array.is_null(),
        )
    values = _convert_arrow_array(array.values)
    if values is array.values:
        return array
    return pyarrow.ListArray.from_arrays(array.offsets, values, mask=array.is_null())


@cache
def _hex_octets() -> Any:
    return pyarrow.array([f"{octet:02x}" for octet in range(256)])


def _objectid_to_hex(storage: Any) -> Any:
    """Vectorized `str(ObjectId)` of a `fixed_size_binary[12]` array"""
    if len(storage) == 0:
        return pyarrow.array([], type=pyarrow.string())
    width = storage.type.byte_width
    octets = pyarrow.Array.from_buffers(
        pyarrow.uint8(),
        len(storage) * width,
        [None, storage.buffers()[1]],
        offset=storage.offset * width,
    )
    pairs = pyarrow.FixedSizeListArray.from_arrays(
        pyarrow.compute.take(_hex_octets(), octets), width, mask=storage.is_null()
    )
    return pyarrow.compute.binary_join(pairs.cast(pyarrow.list_(pyarrow.string())), "")


def _read_size(stream: IO[bytes], size: struct.Struct) -> int:
    return size.unpack(_read_exact(stream, size.size))[0]  # type: ignore[no-any-return]


def _read_exact(stream: IO[bytes], size: int) -> bytes:
    data = stream.read(size)
    if len(data) != size:
        raise EOFError(f"stream ended after {len(data)} of {size} bytes")
    return data


if __name__ == "__main__":
    main()

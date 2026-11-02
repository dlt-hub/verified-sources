"""Parses raw BSON into Arrow tables. Worker processes import this module, so it does not
import dlt."""

import warnings
from functools import cache
from inspect import signature
from typing import (
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

from .partition import TPartition, find_partition
from .worker_pool import ArrowWorker, TTableWithMetadata


class Mismatch(NamedTuple):
    """Fields with values that do not match their type, `a.b` for nested fields"""

    fields: Dict[str, List[str]]
    details: str
    with_schema: bool


def parse_bson_batches(
    batches: Iterable[bytes],
    pymongoarrow_schema: Any,
    codec_options: Any,
    strict: bool = False,
) -> Any:
    """Parses raw BSON batches into one table with the BSON extension types of pymongoarrow.
    With `strict`, values that do not match `pymongoarrow_schema` raise `TypeError`."""
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
    """Parses raw BSON batches into a table with Arrow types, and finds the fields with values of
    more than one type. Returns no table when values do not match `pymongoarrow_schema`.
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
    partition: TPartition,
    filter_: Dict[str, Any],
    projection: Optional[Dict[str, Any]],
    pymongoarrow_schema: Any,
    detect_mismatch: bool,
) -> Tuple[Any, Optional[Mismatch]]:
    """Parses the documents of `partition` that match `filter_` into one table"""
    # one parse of all batches infers one type for each field
    batches = list(
        find_partition(collection.find_raw_batches, partition, filter_, projection)
    )
    return parse_batch(
        batches, pymongoarrow_schema, collection.codec_options, detect_mismatch
    )


def convert_arrow_columns(table: Any) -> Any:
    """Converts the BSON extension types of pymongoarrow to Arrow types, also in nested columns"""
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


class MongoArrowWorker(ArrowWorker):
    """Reads a partition of a collection into one table"""

    def __init__(self) -> None:
        self._clients: Dict[str, Any] = {}

    def do_get(self, ticket: Dict[str, Any]) -> Iterator[TTableWithMetadata]:
        url = ticket["connection_url"]
        if url not in self._clients:
            with warnings.catch_warnings():
                # see `ignore_compressor_warnings` in helpers.py
                warnings.filterwarnings("ignore", "Wire protocol compression with")
                warnings.filterwarnings("ignore", "Unsupported compressor: $")
                self._clients[url] = MongoClient(url, **ticket["client_options"])
        collection = self._clients[url][ticket["database"]][ticket["collection"]]
        table, mismatch = read_partition(
            collection,
            ticket["partition"],
            ticket["filter"],
            ticket["projection"],
            ticket["pymongoarrow_schema"],
            ticket["detect_mismatch"],
        )
        yield table, {"mismatch": mismatch and mismatch._asdict()}


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
    """Converts the BSON extension types in `array`, returns `array` itself if there are none"""
    arrow_type = array.type
    if _is_objectid(arrow_type):
        # the hex string of the "python" backend
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

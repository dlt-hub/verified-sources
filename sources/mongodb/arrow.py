"""Arrow format of the MongoDB source. Needs `pymongoarrow`, import it only when loading Arrow."""

import threading
from functools import cache
from inspect import signature
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Mapping,
    NamedTuple,
    Optional,
    Set,
    Tuple,
    Union,
)

import dlt
from bson import decode_all
from dlt.common import logger
from dlt.common.libs.pyarrow import NameNormalizationCollision, pyarrow
from dlt.common.normalizers.json.relational import DataItemNormalizer
from dlt.common.normalizers.naming import NamingConvention
from dlt.common.normalizers.naming.snake_case import (
    NamingConvention as SnakeCaseNamingConvention,
)
from dlt.common.typing import TDataItem
from dlt.extract.exceptions import (
    CurrentSourceNotAvailable,
    CurrentSourceSchemaNotAvailable,
)
from pymongoarrow.api import Schema  # type: ignore
from pymongoarrow.context import PyMongoArrowContext  # type: ignore
from pymongoarrow.types import (  # type: ignore
    _is_binary,
    _is_code,
    _is_decimal128,
    _is_objectid,
)

from .helpers import (
    CollectionLoader,
    CollectionLoaderParallel,
    TCodecOptions,
    TCursor,
    TTypeMismatch,
)


class FieldTypeMismatchError(ValueError):
    def __init__(
        self,
        collection_name: str,
        fields: Dict[str, List[str]],
        details: str,
        with_schema: bool,
        raised: bool = True,
    ) -> None:
        self.collection_name = collection_name
        self.fields = fields
        described = ", ".join(
            f"`{field}` ({', '.join(types)})" for field, types in fields.items()
        )
        if with_schema:
            message = (
                f"Collection `{collection_name}` has values that do not match "
                f"`pymongoarrow_schema`: {described or details}. Correct the field types in the "
                "schema or use `data_item_format='object'`."
            )
        else:
            message = (
                f"Collection `{collection_name}` has fields with values of different types: "
                f"{described or details}. The Arrow format loads values that do not match the "
                "first type of a field as null and truncates numbers to it. Pass the field types "
                "in `pymongoarrow_schema` or use `data_item_format='object'`"
            )
            message += (
                ". Set `on_inferred_type_mismatch` to 'warn' or 'ignore' to load them anyway."
                if raised
                else " to load them as they are."
            )
        super().__init__(message)


class ArrowOptions(NamedTuple):
    pymongoarrow_schema: Any = None
    on_inferred_type_mismatch: TTypeMismatch = "ignore"
    max_nesting: int = 1000
    naming: NamingConvention = SnakeCaseNamingConvention()
    is_json_column: Callable[[str], bool] = lambda _: False


DEFAULT_ARROW_OPTIONS = ArrowOptions()


class CollectionArrowLoader(CollectionLoader):
    def load_documents(
        self,
        filter_: Dict[str, Any],
        limit: Optional[int] = None,
        projection: Optional[Union[Mapping[str, Any], Iterable[str]]] = None,
        arrow_options: ArrowOptions = DEFAULT_ARROW_OPTIONS,
    ) -> Iterator[Any]:
        """Load documents from the collection as Arrow tables, one per batch of `chunk_size`."""
        cursor = self.collection.find_raw_batches(
            filter=self._query_filter(filter_),
            batch_size=self.chunk_size,
            projection=self._projection_op(projection),
        )
        if self._sort_op:
            cursor = cursor.sort(self._sort_op)

        cursor = self._limit(cursor, limit)  # type: ignore

        converter = ArrowBatchConverter(
            self.collection.name, self.collection.codec_options, arrow_options
        )
        for batch in cursor:
            table = converter.convert([batch])
            if table is not None:
                yield table


class CollectionArrowLoaderParallel(CollectionLoaderParallel):
    def load_documents(
        self,
        filter_: Dict[str, Any],
        limit: Optional[int] = None,
        projection: Optional[Union[Mapping[str, Any], Iterable[str]]] = None,
        arrow_options: ArrowOptions = DEFAULT_ARROW_OPTIONS,
    ) -> Iterator[TDataItem]:
        """Load documents from the collection as Arrow tables, with batches read in parallel."""
        batches = self._create_batches(filter_=filter_, limit=limit)
        cursor = self._get_cursor(filter_=filter_, projection=projection)
        converter = ArrowBatchConverter(
            self.collection.name, self.collection.codec_options, arrow_options
        )
        for batch in batches:
            yield self._run_batch(cursor=cursor, batch=batch, converter=converter)

    def _get_cursor(
        self,
        filter_: Dict[str, Any],
        projection: Optional[Union[Mapping[str, Any], Iterable[str]]] = None,
    ) -> TCursor:
        cursor = self.collection.find_raw_batches(
            filter=self._query_filter(filter_),
            batch_size=self.chunk_size,
            projection=self._projection_op(projection),
        )
        return cursor.sort(self._batches_sort_op)  # type: ignore[no-any-return]

    @dlt.defer
    def _run_batch(  # type: ignore[override]
        self,
        cursor: TCursor,
        batch: Dict[str, int],
        converter: "ArrowBatchConverter",
    ) -> TDataItem:
        cursor = cursor.clone()
        return converter.convert(list(cursor.skip(batch["skip"]).limit(batch["limit"])))


class ArrowBatchConverter:
    """Converts raw BSON batches of a collection into Arrow tables. Flattens nested documents like
    the relational normalizer does and detects fields whose values change type."""

    def __init__(
        self,
        collection_name: str,
        codec_options: TCodecOptions,
        options: ArrowOptions,
    ) -> None:
        self.collection_name = collection_name
        self.codec_options = codec_options
        self.options = options
        self._field_types: Dict[str, Any] = {}
        self._warned: Set[str] = set()
        # parsing holds the GIL anyway, the lock keeps the seen field types consistent
        self._lock = threading.Lock()

    def convert(self, batches: List[bytes]) -> Optional[Any]:
        with self._lock:
            table = self._parse(batches)
            if not table.num_rows:
                return None
            table = flatten_arrow_structs(
                convert_arrow_columns(table),
                self.options.max_nesting,
                self.options.naming,
                self.options.is_json_column,
            )
            return self._check_field_types(table)

    def _parse(self, batches: List[bytes]) -> Any:
        if self.options.pymongoarrow_schema is not None:
            try:
                return parse_bson_batches(
                    batches,
                    self.options.pymongoarrow_schema,
                    self.codec_options,
                    strict=_supports_strict_schema(),
                )
            except TypeError as ex:
                raise FieldTypeMismatchError(
                    self.collection_name,
                    _mismatched_fields(batches, self.codec_options),
                    str(ex),
                    with_schema=True,
                ) from ex

        table = parse_bson_batches(batches, None, self.codec_options)
        if (
            self.options.on_inferred_type_mismatch != "ignore"
            and _supports_strict_schema()
        ):
            # the inferred schema loads values of another type as null, a strict one raises
            inferred = Schema({field.name: field.type for field in table.schema})
            try:
                parse_bson_batches(batches, inferred, self.codec_options, strict=True)
            except TypeError as ex:
                self._mismatch(_mismatched_fields(batches, self.codec_options), str(ex))
        return table

    def _check_field_types(self, table: Any) -> Any:
        if self.options.pymongoarrow_schema is not None:
            return table

        mismatched: Dict[str, List[str]] = {}
        for i, field in enumerate(table.schema):
            if pyarrow.types.is_null(field.type):
                continue
            seen = self._field_types.setdefault(field.name, field.type)
            if _same_arrow_kind(seen, field.type):
                continue
            mismatched[field.name] = [str(seen), str(field.type)]
            table = table.set_column(
                i, pyarrow.field(field.name, seen), pyarrow.nulls(table.num_rows, seen)
            )
        if mismatched:
            self._mismatch(mismatched, "")
        return table

    def _mismatch(self, fields: Dict[str, List[str]], details: str) -> None:
        raised = self.options.on_inferred_type_mismatch == "raise"
        error = FieldTypeMismatchError(
            self.collection_name, fields, details, with_schema=False, raised=raised
        )
        if raised:
            raise error
        if (
            self.options.on_inferred_type_mismatch == "warn"
            and not set(fields) <= self._warned
        ):
            self._warned.update(fields)
            logger.warning(str(error))


def parse_bson_batches(
    batches: Iterable[bytes],
    pymongoarrow_schema: Any,
    codec_options: TCodecOptions,
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


def flatten_arrow_structs(
    table: Any,
    max_nesting: int,
    naming: NamingConvention,
    is_json_column: Callable[[str], bool],
) -> Any:
    """Flatten struct columns into columns named like the relational normalizer names nested
    documents, `max_nesting` levels deep. Struct columns typed `json` and lists stay as they are.
    Raises `NameNormalizationCollision` when two columns get the same name.
    """
    names: List[str] = []
    columns: List[Any] = []

    def add(column: Any, path: Tuple[str, ...], nesting: int) -> None:
        name = naming.shorten_fragments(*path)
        if (
            pyarrow.types.is_struct(column.type)
            and nesting > 0
            and not is_json_column(name)
        ):
            for field, child in zip(column.type, column.flatten()):
                add(child, path + (naming.normalize_path(field.name),), nesting - 1)
        else:
            names.append(name)
            columns.append(column)

    for field, column in zip(table.schema, table.columns):
        add(column, (naming.normalize_path(field.name),), max_nesting)
    if len(set(names)) != len(names):
        duplicates = sorted({name for name in names if names.count(name) > 1})
        raise NameNormalizationCollision(
            f"Fields {table.schema.names} flattened from nested documents produce duplicate"
            f" columns: {duplicates}"
        )
    return pyarrow.Table.from_arrays(columns, names=names)


def arrow_options_from_context(
    pymongoarrow_schema: Any, on_inferred_type_mismatch: TTypeMismatch
) -> ArrowOptions:
    """Take the nesting level, naming and `json` columns from the extracted resource and schema"""
    try:
        resource = dlt.current.resource()
        schema = dlt.current.source_schema()
    except (CurrentSourceNotAvailable, CurrentSourceSchemaNotAvailable):
        return ArrowOptions(pymongoarrow_schema, on_inferred_type_mismatch)

    max_nesting = resource.max_table_nesting
    if max_nesting is None:
        max_nesting = DataItemNormalizer.get_normalizer_config(schema).get(
            "max_nesting", 1000
        )
    # dynamic hints need a data item, only static ones are known up front
    hinted_columns = resource.columns if isinstance(resource.columns, dict) else {}
    table_name = resource.table_name
    stored_columns = (
        schema.tables.get(table_name, {}).get("columns", {})
        if isinstance(table_name, str)
        else {}
    )

    def is_json_column(name: str) -> bool:
        column = hinted_columns.get(name) or stored_columns.get(name) or {}
        return (column.get("data_type") or schema.get_preferred_type(name)) == "json"

    return ArrowOptions(
        pymongoarrow_schema,
        on_inferred_type_mismatch,
        max_nesting,
        schema.naming,
        is_json_column,
    )


@cache
def _supports_strict_schema() -> bool:
    return "allow_invalid" in signature(PyMongoArrowContext.__init__).parameters


def _same_arrow_kind(seen: Any, arrow_type: Any) -> bool:
    if seen == arrow_type:
        return True
    # dlt loads lists and documents as json and all integers as bigint
    for is_kind in (
        pyarrow.types.is_integer,
        pyarrow.types.is_list,
        pyarrow.types.is_struct,
    ):
        if is_kind(seen) and is_kind(arrow_type):
            return True
    return False


def _mismatched_fields(
    batches: List[bytes], codec_options: TCodecOptions
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

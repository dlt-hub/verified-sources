"""The "pyarrow" backend. Imported only by this backend because it requires `pymongoarrow`."""

import threading
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
from dlt.common import logger
from dlt.common.libs.pyarrow import NameNormalizationCollision, pyarrow
from dlt.common.normalizers.json.relational import DataItemNormalizer
from dlt.common.normalizers.naming import NamingConvention
from dlt.common.normalizers.naming.snake_case import (
    NamingConvention as SnakeCaseNamingConvention,
)
from dlt.extract.exceptions import (
    CurrentSourceNotAvailable,
    CurrentSourceSchemaNotAvailable,
)

from .arrow_worker import Mismatch, MongoArrowWorker, parse_batch
from .helpers import (
    client_options,
    CollectionLoader,
    planned_partitions,
    TCodecOptions,
    TTypeMismatch,
)
from .partition import DEFAULT_PARTITION_SIZE_BYTES, TPartition
from .worker_pool import WorkerPool

# one worker process per extract thread of dlt
_WORKER_POOL = WorkerPool()


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
                'schema or use `backend="python"`.'
            )
        else:
            message = (
                f"Collection `{collection_name}` has fields with values of different types: "
                f'{described or details}. The "pyarrow" backend converts them to the type of the '
                "first value or loads them as null. Set the field types in `pymongoarrow_schema` "
                'or use `backend="python"` to load them as they are.'
            )
            if raised:
                message += ' Set `on_inferred_type_mismatch` to "warn" or "ignore" to load them anyway.'
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
        """Yields an Arrow table for each batch of `chunk_size` documents"""
        # `RawBatchCursor.sort` is typed as returning `Cursor` in some pymongo versions
        cursor: Any = self.collection.find_raw_batches(
            filter=self._query_filter(filter_),
            batch_size=self.chunk_size,
            projection=self._projection_op(projection),
        )
        if self._sort_op:
            cursor = cursor.sort(self._sort_op)

        cursor = self._limit(cursor, limit)

        converter = ArrowBatchConverter(
            self.collection.name, self.collection.codec_options, arrow_options
        )
        for batch in cursor:
            table = converter.convert([batch])
            if table is not None:
                yield table


class CollectionArrowLoaderParallel(CollectionArrowLoader):
    def __init__(
        self,
        client: Any,
        collection: Any,
        chunk_size: int,
        incremental: Optional[dlt.sources.incremental[Any]] = None,
        connection_url: Optional[str] = None,
        partition_size_bytes: int = DEFAULT_PARTITION_SIZE_BYTES,
    ) -> None:
        super().__init__(client, collection, chunk_size, incremental)
        self.connection_url = connection_url
        self.partition_size_bytes = partition_size_bytes

    def load_documents(
        self,
        filter_: Dict[str, Any],
        limit: Optional[int] = None,
        projection: Optional[Union[Mapping[str, Any], Iterable[str]]] = None,
        arrow_options: ArrowOptions = DEFAULT_ARROW_OPTIONS,
    ) -> Iterator[Any]:
        """Yields a deferred read of each `_id` partition, parsed into a table in a worker process"""
        query_filter = self._query_filter(filter_)
        partitions: List[TPartition] = []
        # partitions do not tell which documents come first
        if not limit:
            partitions = planned_partitions(
                self.collection,
                query_filter,
                self.partition_size_bytes,
                self.chunk_size,
                # pymongoarrow infers one type per column, mixed `_id` types fail or load as null
                split_id_types=True,
            )
        if len(partitions) < 2:
            yield from super().load_documents(filter_, limit, projection, arrow_options)
            return
        if not self.connection_url:
            raise ValueError(
                'Parallel reads of the "pyarrow" backend require `connection_url`.'
            )

        task = dict(
            connection_url=self.connection_url,
            client_options=client_options(self.connection_url),
            database=self.collection.database.name,
            collection=self.collection.name,
            filter=query_filter,
            projection=self._projection_op(projection),
            pymongoarrow_schema=arrow_options.pymongoarrow_schema,
            detect_mismatch=arrow_options.on_inferred_type_mismatch != "ignore",
        )
        converter = ArrowBatchConverter(
            self.collection.name, self.collection.codec_options, arrow_options
        )
        for partition in partitions:
            yield _read_partition({**task, "partition": partition}, converter)


@dlt.defer
def _read_partition(
    task: Dict[str, Any], converter: "ArrowBatchConverter"
) -> Optional[Any]:
    ((table, metadata),) = _WORKER_POOL.do_get(MongoArrowWorker, task)
    mismatch = metadata["mismatch"]
    return converter.finish(table, mismatch and Mismatch(**mismatch))


class ArrowBatchConverter:
    """Converts raw BSON batches into Arrow tables with the columns of the relational normalizer.
    Checks the field types across batches."""

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
        # parallel reads finish their tables in the extract threads
        self._lock = threading.Lock()

    def convert(self, batches: List[bytes]) -> Optional[Any]:
        return self.finish(
            *parse_batch(
                batches,
                self.options.pymongoarrow_schema,
                self.codec_options,
                self.options.on_inferred_type_mismatch != "ignore",
            )
        )

    def finish(self, table: Any, mismatch: Optional[Mismatch]) -> Optional[Any]:
        """Flattens a table of `parse_batch` and checks its field types against earlier tables"""
        if mismatch is not None:
            if mismatch.with_schema:
                raise FieldTypeMismatchError(
                    self.collection_name,
                    mismatch.fields,
                    mismatch.details,
                    with_schema=True,
                )
            with self._lock:
                self._mismatch(mismatch.fields, mismatch.details)
        if table is None or not table.num_rows:
            return None
        table = flatten_arrow_structs(
            table,
            self.options.max_nesting,
            self.options.naming,
            self.options.is_json_column,
        )
        table = _id_not_nullable(table)
        with self._lock:
            return self._check_field_types(table)

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
            if _is_number(seen) and _is_number(field.type):
                # like pymongoarrow does within a batch, `2.5` loads as 2
                column = pyarrow.compute.cast(table.column(i), seen, safe=False)
            else:
                column = pyarrow.nulls(table.num_rows, seen)
            table = table.set_column(i, pyarrow.field(field.name, seen), column)
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


def flatten_arrow_structs(
    table: Any,
    max_nesting: int,
    naming: NamingConvention,
    is_json_column: Callable[[str], bool],
) -> Any:
    """Flattens struct columns `max_nesting` levels deep into the columns of the relational
    normalizer. Lists and `json` columns stay as they are."""
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
    """Options with the nesting level, naming and `json` columns of the current resource"""
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


def _id_not_nullable(table: Any) -> Any:
    """Marks `_id` not nullable like its primary key hint, so dlt does not warn about each table"""
    index = table.schema.get_field_index("_id")
    # a null `_id` is valid in MongoDB
    if index < 0 or table.column(index).null_count:
        return table
    field = table.schema.field(index)
    return table.set_column(index, field.with_nullable(False), table.column(index))


def _is_number(arrow_type: Any) -> bool:
    return bool(
        pyarrow.types.is_integer(arrow_type) or pyarrow.types.is_floating(arrow_type)
    )

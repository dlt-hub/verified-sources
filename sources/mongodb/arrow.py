"""Arrow format of the MongoDB source. Needs `pymongoarrow`, import it only when loading Arrow."""

import json
import os
import queue
import subprocess
import sys
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
from dlt.common.configuration import resolve_configuration
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
from dlt.extract.pipe_iterator import PipeIterator

from .arrow_worker import (
    Mismatch,
    WorkerError,
    parse_batch,
    read_message,
    write_task,
)
from .helpers import (
    client_options,
    CollectionLoader,
    TCodecOptions,
    TTypeMismatch,
)
from .partition import and_filters, plan_partitions

# runs the worker file by path, so neither dlt nor this source package is imported
_WORKER_BOOTSTRAP = (
    "import json, runpy, sys; sys.path[:] = json.loads(sys.argv[1]); "
    "runpy.run_path(sys.argv[2], run_name='__main__')"
)
_WORKER_PATH = os.path.join(os.path.dirname(__file__), "arrow_worker.py")


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


class CollectionArrowLoaderParallel(CollectionArrowLoader):
    def __init__(
        self,
        client: Any,
        collection: Any,
        chunk_size: int,
        incremental: Optional[dlt.sources.incremental[Any]] = None,
        connection_url: Optional[str] = None,
        workers: Optional[int] = None,
    ) -> None:
        super().__init__(client, collection, chunk_size, incremental)
        self.connection_url = connection_url
        # the processes replace the threads that dlt runs deferred reads in
        self.workers = (
            workers
            or resolve_configuration(PipeIterator.PipeIteratorConfiguration()).workers
        )

    def load_documents(
        self,
        filter_: Dict[str, Any],
        limit: Optional[int] = None,
        projection: Optional[Union[Mapping[str, Any], Iterable[str]]] = None,
        arrow_options: ArrowOptions = DEFAULT_ARROW_OPTIONS,
    ) -> Iterator[Any]:
        """Load documents from the collection as Arrow tables, with `_id` partitions parsed in
        worker processes. Reads in one process when the collection is not split.
        """
        # partitions do not tell which documents come first
        partitions = (
            []
            if limit
            else plan_partitions(self.collection, self.chunk_size, 4 * self.workers)
        )
        if len(partitions) < 2:
            yield from super().load_documents(filter_, limit, projection, arrow_options)
            return
        if not self.connection_url:
            raise ValueError(
                "Parallel Arrow reads need the `connection_url` of the client"
            )

        query_filter = self._query_filter(filter_)
        task = dict(
            connection_url=self.connection_url,
            client_options=client_options(self.connection_url),
            database=self.collection.database.name,
            collection=self.collection.name,
            projection=self._projection_op(projection),
            batch_size=self.chunk_size,
            pymongoarrow_schema=arrow_options.pymongoarrow_schema,
            detect_mismatch=arrow_options.on_inferred_type_mismatch != "ignore",
        )
        converter = ArrowBatchConverter(
            self.collection.name, self.collection.codec_options, arrow_options
        )
        with WorkerPool(min(self.workers, len(partitions))) as pool:
            for table, mismatch in pool.read(
                [
                    {**task, "filter": and_filters(query_filter, partition)}
                    for partition in partitions
                ]
            ):
                table = converter.finish(table, mismatch)
                if table is not None:
                    yield table


class WorkerPool:
    """Worker processes started with the current Python interpreter, each reading partitions
    from MongoDB and sending them back as Arrow tables, see `arrow_worker.py`.
    """

    def __init__(self, workers: int) -> None:
        self.processes = [
            subprocess.Popen(
                [
                    sys.executable,
                    "-c",
                    _WORKER_BOOTSTRAP,
                    json.dumps(sys.path),
                    _WORKER_PATH,
                ],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
            )
            for _ in range(workers)
        ]
        self._tasks: "queue.Queue[Dict[str, Any]]" = queue.Queue()
        # a full queue stops the workers on their pipes until the tables are taken
        self._results: "queue.Queue[Any]" = queue.Queue(maxsize=2 * workers)
        self._stopped = threading.Event()
        self._threads: List[threading.Thread] = []

    def read(
        self, tasks: List[Dict[str, Any]]
    ) -> Iterator[Tuple[Any, Optional[Mismatch]]]:
        """Yield the tables of `tasks` in the order the workers read them"""
        for task in tasks:
            self._tasks.put(task)
        self._threads = [
            threading.Thread(target=self._drive, args=(process,), daemon=True)
            for process in self.processes
        ]
        for thread in self._threads:
            thread.start()
        running = len(self._threads)
        while running:
            result = self._results.get()
            if result is None:
                running -= 1
            elif isinstance(result, BaseException):
                raise result
            else:
                yield result

    def close(self) -> None:
        self._stopped.set()
        for process in self.processes:
            if process.poll() is None:
                process.kill()
            process.wait()
            process.stdin.close()
            process.stdout.close()
        for thread in self._threads:
            thread.join()

    def __enter__(self) -> "WorkerPool":
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()

    def _drive(self, process: "subprocess.Popen[bytes]") -> None:
        try:
            while not self._stopped.is_set():
                try:
                    task = self._tasks.get_nowait()
                except queue.Empty:
                    break
                write_task(process.stdin, task)
                while True:
                    try:
                        header, table = read_message(process.stdout)
                    except EOFError:
                        raise WorkerError(
                            f"Worker process exited with code {process.wait()}"
                        )
                    if header["kind"] == "done":
                        break
                    if header["kind"] == "error":
                        raise WorkerError(header["error"])
                    mismatch = header["mismatch"]
                    self._put((table, mismatch and Mismatch(**mismatch)))
        except BaseException as ex:
            if not self._stopped.is_set():
                self._put(ex)
        finally:
            self._put(None)

    def _put(self, result: Any) -> None:
        while not self._stopped.is_set():
            try:
                self._results.put(result, timeout=0.1)
                return
            except queue.Full:
                continue


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
        """Flatten a table of `parse_batch` and check its field types against earlier tables"""
        if mismatch is not None:
            if mismatch.with_schema:
                raise FieldTypeMismatchError(
                    self.collection_name,
                    mismatch.fields,
                    mismatch.details,
                    with_schema=True,
                )
            self._mismatch(mismatch.fields, mismatch.details)
        if not table.num_rows:
            return None
        table = flatten_arrow_structs(
            table,
            self.options.max_nesting,
            self.options.naming,
            self.options.is_json_column,
        )
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

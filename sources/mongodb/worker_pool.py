"""Worker processes that run tasks offloaded by threads and stream Arrow tables back, like the
`do_get` of an Arrow Flight server. Imports neither dlt nor the package it is in: a worker process
runs this file by path and loads the worker classes from their files, so it starts fast."""

import errno
import importlib
import json
import operator
import os
import pickle
import signal
import struct
import subprocess
import sys
import threading
import traceback
import types
import weakref
from enum import IntEnum
from typing import IO, Any, Dict, Iterator, Optional, Tuple, Type

import pyarrow
import pyarrow.ipc

TWorkerSpec = Tuple[str, str, str]
TTableWithMetadata = Tuple[Optional[Any], Optional[Dict[str, Any]]]

# a message is its kind, the sizes of its JSON metadata and body, the metadata and the body
_PREFIX = struct.Struct(">BIQ")
_BOOTSTRAP = (
    "import json, runpy, sys; sys.path[:] = json.loads(sys.argv[1]); "
    "runpy.run_path(sys.argv[2], run_name='__main__')"
)
# directories of worker classes imported as packages, see `load_worker`
_PACKAGES: Dict[str, str] = {}


class MessageKind(IntEnum):
    """The parent sends tasks, a worker answers each with tables and done or error. ASCII letters
    are readable in a dump of the pipes."""

    TASK = ord("T")
    TABLE = ord("A")
    DONE = ord("D")
    ERROR = ord("E")


class ArrowWorker:
    """Runs in a worker process and streams the Arrow tables of a ticket to the parent. Created
    once per process, so it may keep e.g. connections between tickets. Its module is imported without the `__init__` of its package and should
    import only what the worker needs, e.g. not dlt.
    """

    def do_get(self, ticket: Any) -> Iterator[TTableWithMetadata]:
        """Yield the tables of `ticket`, each with JSON serializable metadata. A table may be None
        to send only metadata."""
        raise NotImplementedError()


class WorkerError(Exception):
    pass


class WorkerProcess:
    """A worker process started with the current Python interpreter, runs one ticket at a time"""

    def __init__(self) -> None:
        self.process = subprocess.Popen(
            [
                sys.executable,
                "-c",
                _BOOTSTRAP,
                json.dumps(sys.path),
                os.path.abspath(__file__),
            ],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
        )
        self._finalizer = weakref.finalize(self, _close_process, self.process)

    @property
    def alive(self) -> bool:
        return self._finalizer.alive and self.process.poll() is None

    def do_get(
        self, worker: Type[ArrowWorker], ticket: Any
    ) -> Iterator[TTableWithMetadata]:
        """Stream the tables of `ticket` from `worker` in this process"""
        done = False
        try:
            try:
                write_message(
                    self.process.stdin,
                    MessageKind.TASK,
                    worker_spec(worker),
                    pickle.dumps(ticket),
                )
            except OSError as ex:
                if not _is_closed_pipe(ex):
                    raise
                raise self._exited()
            while True:
                try:
                    kind, metadata, body = read_message(self.process.stdout)
                except EOFError:
                    raise self._exited()
                if kind == MessageKind.TABLE:
                    yield _table_from_ipc(body), metadata
                    continue
                done = True
                if kind == MessageKind.ERROR:
                    raise WorkerError(metadata)
                return
        finally:
            # the rest of a stream that is not read to the end would come with the next ticket
            if not done:
                self.close()

    def close(self) -> None:
        self._finalizer()

    def _exited(self) -> WorkerError:
        return WorkerError(f"Worker process exited with code {self.process.wait()}")


class WorkerPool:
    """Worker processes for the threads that offload tasks to them, one per thread, started on
    first use and closed when the thread ends, so the threads bound the number of processes.
    """

    def __init__(self) -> None:
        self._local = threading.local()
        self._processes: "weakref.WeakSet[WorkerProcess]" = weakref.WeakSet()
        self._lock = threading.Lock()

    def do_get(
        self, worker: Type[ArrowWorker], ticket: Any
    ) -> Iterator[TTableWithMetadata]:
        """Stream the tables of `ticket` from `worker` in the process of the current thread"""
        return self._process().do_get(worker, ticket)

    def close(self) -> None:
        """Close the processes of all threads"""
        with self._lock:
            processes = list(self._processes)
        for process in processes:
            process.close()

    def __enter__(self) -> "WorkerPool":
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()

    def _process(self) -> WorkerProcess:
        holder: Optional[_ThreadProcess] = getattr(self._local, "holder", None)
        if holder is None or not holder.process.alive:
            holder = self._local.holder = _ThreadProcess(WorkerProcess())
            with self._lock:
                self._processes.add(holder.process)
        return holder.process


class _ThreadProcess:
    """Closes the process of a thread when the thread ends, also when e.g. the traceback of an
    error still refers to it"""

    __slots__ = ("process", "__weakref__")

    def __init__(self, process: WorkerProcess) -> None:
        self.process = process
        weakref.finalize(self, process.close)


def worker_spec(worker: Type[ArrowWorker]) -> TWorkerSpec:
    """The directory, module and class name that a worker process loads `worker` from"""
    path = getattr(sys.modules[worker.__module__], "__file__", None)
    if worker.__module__ == "__main__" or not path:
        raise ValueError(
            f"Worker `{worker.__qualname__}` must be defined in a module file"
        )
    directory, file_name = os.path.split(os.path.abspath(path))
    return directory, os.path.splitext(file_name)[0], worker.__qualname__


def load_worker(directory: str, module: str, name: str) -> Type[ArrowWorker]:
    """Import a worker class from its module in `directory`. The directory becomes a package
    without its `__init__`, so the relative imports of the module work."""
    package = _PACKAGES.get(directory)
    if package is None:
        package = _PACKAGES[directory] = f"_worker_package_{len(_PACKAGES)}"
        package_module = types.ModuleType(package)
        package_module.__path__ = [directory]
        sys.modules[package] = package_module
    worker: Type[ArrowWorker] = operator.attrgetter(name)(
        importlib.import_module(f"{package}.{module}")
    )
    return worker


def serve() -> None:
    """Loop of a worker process: runs the tickets that the parent writes on stdin and writes their
    tables to stdout, until the parent closes stdin."""
    # the parent stops its workers, e.g. on ctrl-c
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    output = sys.stdout.buffer
    # stray prints would corrupt the messages
    sys.stdout = sys.stderr
    workers: Dict[TWorkerSpec, ArrowWorker] = {}
    while True:
        try:
            _, worker, ticket = read_message(sys.stdin.buffer)
        except EOFError:
            return
        try:
            spec: TWorkerSpec = tuple(worker)
            if spec not in workers:
                workers[spec] = load_worker(*spec)()
            # tickets come only from the parent process that started this one
            for table, metadata in workers[spec].do_get(pickle.loads(ticket)):
                write_message(output, MessageKind.TABLE, metadata, _table_to_ipc(table))
        except Exception:
            write_message(output, MessageKind.ERROR, traceback.format_exc())
        else:
            write_message(output, MessageKind.DONE)


def write_message(
    stream: IO[bytes], kind: MessageKind, metadata: Any = None, body: Any = b""
) -> None:
    """Write a message of `kind` with JSON serializable `metadata` and a `body` of bytes"""
    encoded = json.dumps(metadata).encode()
    body = memoryview(body)
    stream.write(_PREFIX.pack(kind, len(encoded), body.nbytes))
    stream.write(encoded)
    stream.write(body)
    stream.flush()


def read_message(stream: IO[bytes]) -> Tuple[MessageKind, Any, bytes]:
    """Read the kind, metadata and body of a message of `write_message`. Raises `EOFError` when the
    stream ends."""
    kind, metadata_size, body_size = _PREFIX.unpack(_read_exact(stream, _PREFIX.size))
    metadata = json.loads(_read_exact(stream, metadata_size))
    return MessageKind(kind), metadata, _read_exact(stream, body_size)


def _close_process(process: "subprocess.Popen[bytes]") -> None:
    if process.poll() is None:
        process.kill()
    process.wait()
    try:
        process.stdin.close()
    except OSError as ex:
        # a task left in the buffer of a dead worker
        if not _is_closed_pipe(ex):
            raise
    process.stdout.close()


def _is_closed_pipe(ex: OSError) -> bool:
    """Writing to the pipe of an exited process, Windows raises EINVAL like in `subprocess`"""
    return isinstance(ex, BrokenPipeError) or ex.errno == errno.EINVAL


def _table_to_ipc(table: Any) -> Any:
    if table is None:
        return b""
    sink = pyarrow.BufferOutputStream()
    with pyarrow.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue()


def _table_from_ipc(body: bytes) -> Any:
    if not body:
        return None
    return pyarrow.ipc.open_stream(pyarrow.py_buffer(body)).read_all()


def _read_exact(stream: IO[bytes], size: int) -> bytes:
    data = stream.read(size)
    if len(data) != size:
        raise EOFError(f"stream ended after {len(data)} of {size} bytes")
    return data


if __name__ == "__main__":
    serve()

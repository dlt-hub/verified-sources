import io
import pickle
import threading
from typing import Any, Dict, List

import pyarrow as pa
import pytest
from bson import ObjectId

from sources.mongodb.helpers import client_options
from sources.mongodb.worker_pool import (
    MessageKind,
    WorkerError,
    WorkerPool,
    WorkerProcess,
    _table_from_ipc,
    _table_to_ipc,
    read_message,
    write_message,
)

from tests.mongodb.utils import ProcessRecorder
from tests.mongodb.workers import EchoWorker


def _run_tickets(pool: WorkerPool, count: int = 1) -> List[int]:
    """Run `count` tickets in the current thread, returns the pids of the processes that ran them"""
    return [
        metadata["pid"]
        for _ in range(count)
        for _, metadata in pool.do_get(EchoWorker, [([1], {})])
    ]


def test_do_get_streams_tables_with_metadata() -> None:
    with WorkerPool() as pool:
        stream = list(
            pool.do_get(
                EchoWorker,
                [([1, 2], {"part": 0}), (None, {"part": 1}), ([3], {"part": 2})],
            )
        )

    assert [table and table["n"].to_pylist() for table, _ in stream] == [
        [1, 2],
        None,
        [3],
    ]
    assert [metadata["part"] for _, metadata in stream] == [0, 1, 2]
    # the worker module is imported without the `__init__` of its package
    assert stream[0][1]["modules"] == []


@pytest.mark.parametrize(
    "ticket,error,same_process",
    [("boom", "ValueError: boom", True), ("exit", "exited with code 3", False)],
    ids=["raises", "exits"],
)
def test_worker_failure(ticket: str, error: str, same_process: bool) -> None:
    with WorkerPool() as pool:
        (before,) = _run_tickets(pool)
        with pytest.raises(WorkerError, match=error):
            list(pool.do_get(EchoWorker, ticket))
        (after,) = _run_tickets(pool)

    assert (before == after) == same_process


def test_abandoned_stream_closes_process(monkeypatch: pytest.MonkeyPatch) -> None:
    recorder = ProcessRecorder(monkeypatch)
    with WorkerPool() as pool:
        stream = pool.do_get(EchoWorker, [([1], {}), ([2], {})])
        next(stream)
        stream.close()
        # the rest of the stream would come with the next ticket
        assert recorder.running == 0
        _run_tickets(pool)

    assert len(recorder.processes) == 2


def test_process_per_thread(monkeypatch: pytest.MonkeyPatch) -> None:
    recorder = ProcessRecorder(monkeypatch)
    pool = WorkerPool()
    pids: Dict[str, List[int]] = {}
    # the threads and the test meet after the threads used their processes and before they end
    barrier = threading.Barrier(3, timeout=60)

    def run(name: str) -> None:
        pids[name] = _run_tickets(pool, 2)
        barrier.wait()
        barrier.wait()

    threads = [threading.Thread(target=run, args=(name,)) for name in ("a", "b")]
    for thread in threads:
        thread.start()
    barrier.wait()
    assert recorder.running == 2
    barrier.wait()
    for thread in threads:
        thread.join()

    # a thread reuses its process, threads do not share one
    assert len(set(pids["a"])) == len(set(pids["b"])) == 1
    assert pids["a"] != pids["b"]
    # the process of a thread closes when the thread ends
    assert len(recorder.processes) == 2
    assert recorder.running == 0


def test_close_stops_processes(monkeypatch: pytest.MonkeyPatch) -> None:
    recorder = ProcessRecorder(monkeypatch)
    pool = WorkerPool()
    _run_tickets(pool)
    assert recorder.running == 1

    pool.close()
    assert recorder.running == 0

    # the next ticket starts a new process
    _run_tickets(pool)
    assert len(recorder.processes) == 2
    pool.close()


def test_close_dead_process_with_unflushed_task() -> None:
    worker = WorkerProcess()
    worker.process.kill()
    worker.process.wait()
    worker.process.stdin.write(b"task")

    worker.close()

    assert not worker.alive


def test_messages() -> None:
    table = pa.table({"_id": ["a", "b"], "n": [1, None]})
    ticket = pickle.dumps(
        {
            "filter": {"_id": {"$gte": ObjectId()}},
            "client_options": client_options("mongodb://localhost"),
        }
    )
    stream = io.BytesIO()
    write_message(stream, MessageKind.TASK, ["dir", "module", "Worker"], ticket)
    write_message(stream, MessageKind.TABLE, {"part": 1}, _table_to_ipc(table))
    write_message(stream, MessageKind.TABLE, {"part": 2})
    write_message(stream, MessageKind.ERROR, "boom")
    write_message(stream, MessageKind.DONE)
    stream.seek(0)

    assert read_message(stream) == (
        MessageKind.TASK,
        ["dir", "module", "Worker"],
        ticket,
    )
    kind, metadata, body = read_message(stream)
    assert (kind, metadata, _table_from_ipc(body)) == (
        MessageKind.TABLE,
        {"part": 1},
        table,
    )
    # a table may be None to send only metadata
    assert read_message(stream) == (MessageKind.TABLE, {"part": 2}, b"")
    assert read_message(stream) == (MessageKind.ERROR, "boom", b"")
    assert read_message(stream) == (MessageKind.DONE, None, b"")
    # an unknown kind
    with pytest.raises(ValueError):
        read_message(io.BytesIO(b"\x09" + stream.getvalue()[1:]))
    # the other process closed the stream
    with pytest.raises(EOFError):
        read_message(stream)
    # e.g. a worker killed while it writes
    with pytest.raises(EOFError):
        read_message(io.BytesIO(stream.getvalue()[:20]))

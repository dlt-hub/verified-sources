import base64
import csv
import io
import itertools
import random
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, Iterator, List, Optional

import pytest
from requests.exceptions import ChunkedEncodingError
from simple_salesforce.exceptions import SalesforceOperationError
from tenacity import stop_after_attempt, wait_none

from sources.salesforce.helpers import query as query_module
from sources.salesforce.helpers.query import BulkQueryApi, ResultPage

from tests.salesforce.utils import (
    BULK_HEADER,
    BULK_JOB_ID,
    FakeBulkConnection,
    bulk_chunks,
    bulk_locator,
)


@pytest.fixture(autouse=True)
def no_resume_wait(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(query_module, "CHUNK_DOWNLOAD_WAIT", wait_none())


@pytest.fixture(scope="module")
def pool() -> Iterator[ThreadPoolExecutor]:
    with ThreadPoolExecutor(4) as executor:
        yield executor


def chunk_of(params: Dict[str, Any]) -> int:
    locator = params.get("locator") or bulk_locator(0)
    decoded = base64.b64decode(locator + "=" * (-len(locator) % 4)).decode()
    return int(decoded.split(":")[1][len("chunk") :])


def page_ids(page: ResultPage) -> List[str]:
    rows = list(csv.reader(io.StringIO(page.csv.decode(), newline="")))[1:]
    assert len(rows) == page.records
    return [row[0] for row in rows]


def all_ids(chunks: List[List[str]]) -> List[str]:
    return [f"{n:03d}" for n in range(sum(len(chunk) for chunk in chunks))]


def read_ids(
    pool: ThreadPoolExecutor,
    connection: FakeBulkConnection,
    partitioned: bool,
    page_size: int = 50,
    max_page_bytes: Optional[int] = None,
) -> List[str]:
    """Reads the ids of all records of the job, the pages of chunks one at a time on any thread
    of `pool` like dlt extract workers
    """
    ids: List[str] = []
    chunk_pages: List[Iterator[ResultPage]] = []
    for result in BulkQueryApi(connection).query(  # type: ignore[arg-type]
        "SELECT", {}, False, page_size, partitioned, max_page_bytes
    ):
        if isinstance(result, ResultPage):
            ids += page_ids(result)
        else:
            chunk_pages.append(result)
    order = random.Random(1)
    while chunk_pages:
        pages = order.choice(chunk_pages)
        page = pool.submit(next, pages, None).result()
        if page is None:
            chunk_pages.remove(pages)
        else:
            ids += page_ids(page)
    return ids


def assert_read_once(
    ids: List[str], chunks: List[List[str]], partitioned: bool
) -> None:
    # chunks read in parallel come in any order, the records of a chunk in their order
    assert (sorted(ids) if partitioned else ids) == all_ids(chunks)


@pytest.mark.parametrize("partitioned", [True, False], ids=["parallel", "sequential"])
@pytest.mark.parametrize(
    "page_size,max_page_bytes",
    [(1, None), (3, None), (50, 30)],
    ids=["1", "3", "bytes"],
)
@pytest.mark.parametrize("broken", [True, False], ids=["broken", "ended"])
def test_bulk_chunk_breaks_at_every_byte(
    pool: ThreadPoolExecutor,
    partitioned: bool,
    page_size: int,
    max_page_bytes: Optional[int],
    broken: bool,
) -> None:
    # the first download of a chunk breaks, or its body ends, at every byte, the header included
    chunks = bulk_chunks(3, 4, 2)
    for chunk, records in enumerate(chunks):
        for cut in range(len(BULK_HEADER) + len("".join(records)) + 1):

            def cut_first(
                n: int, params: Dict[str, Any], chunk: int = chunk, cut: int = cut
            ) -> Any:
                return (
                    cut
                    if "maxRecords" not in params and chunk_of(params) == chunk
                    else None
                )

            connection = FakeBulkConnection(chunks, cut_first, broken=broken)
            ids = read_ids(pool, connection, partitioned, page_size, max_page_bytes)
            assert_read_once(ids, chunks, partitioned)


@pytest.mark.parametrize("partitioned", [True, False], ids=["parallel", "sequential"])
def test_bulk_chunk_breaks_repeatedly(
    pool: ThreadPoolExecutor, monkeypatch: pytest.MonkeyPatch, partitioned: bool
) -> None:
    # the first download of a chunk breaks at one byte and each resumed download at another
    monkeypatch.setattr(query_module, "CHUNK_DOWNLOAD_STOP", stop_after_attempt(50))
    chunks = bulk_chunks(4, 3)
    body_size = len(BULK_HEADER) + len("".join(chunks[0]))
    completed = 0
    for first_cut, resumed_cut in itertools.product(
        range(0, body_size + 1, 4), repeat=2
    ):

        def cut(
            n: int,
            params: Dict[str, Any],
            first: int = first_cut,
            resumed: int = resumed_cut,
        ) -> Any:
            if chunk_of(params) != 0:
                return None
            return resumed if "maxRecords" in params else first

        connection = FakeBulkConnection(chunks, cut)
        try:
            ids = read_ids(pool, connection, partitioned, page_size=1)
        except ChunkedEncodingError:
            # resumed downloads that break before their first record make no progress
            continue
        assert_read_once(ids, chunks, partitioned)
        completed += 1
    assert completed > 0


def test_bulk_result_pages_paged(pool: ThreadPoolExecutor) -> None:
    chunks = bulk_chunks(2, 2, 2)

    def link(n: int) -> Dict[str, str]:
        return {
            "resultLink": f"/jobs/query/{BULK_JOB_ID}/results?locator={bulk_locator(n)}"
        }

    next_page = f"/services/data/v59.0/jobs/query/{BULK_JOB_ID}/resultPages?locator=1"
    result_pages = [
        {
            "done": False,
            "nextRecordsUrl": next_page,
            "resultChunks": [link(0), link(1)],
        },
        {"done": True, "nextRecordsUrl": None, "resultChunks": [link(2)]},
    ]
    connection = FakeBulkConnection(chunks, result_pages=result_pages)
    assert_read_once(read_ids(pool, connection, True), chunks, True)
    assert sorted(chunk_of(params) for params in connection.results) == [0, 1, 2]


def test_bulk_result_pages_documented_names(pool: ThreadPoolExecutor) -> None:
    chunks = bulk_chunks(2, 2)
    result_pages = [
        {
            "done": True,
            "nextRecordsUrl": None,
            "resultPages": [
                {
                    "resultUrl": f"/jobs/query/{BULK_JOB_ID}/results?locator={bulk_locator(n)}"
                }
                for n in range(2)
            ],
        }
    ]
    connection = FakeBulkConnection(chunks, result_pages=result_pages)
    assert_read_once(read_ids(pool, connection, True), chunks, True)
    assert all("locator" in params for params in connection.results)


@pytest.mark.parametrize(
    "result_pages,message",
    [
        (
            [{"done": False, "nextRecordUrl": None, "resultChunks": []}],
            "is not done and has no next page",
        ),
        (
            [
                {
                    "done": True,
                    "resultChunks": [
                        {
                            "resultLink": f"/jobs/query/{BULK_JOB_ID}/results?locator={locator}"
                        }
                        for locator in [
                            bulk_locator(0),
                            bulk_locator(1),
                            bulk_locator(1),
                        ]
                    ],
                }
            ],
            "holds a chunk twice",
        ),
    ],
    ids=["not_done", "chunk_twice"],
)
def test_bulk_result_pages_invalid(
    pool: ThreadPoolExecutor, result_pages: List[Dict[str, Any]], message: str
) -> None:
    connection = FakeBulkConnection(bulk_chunks(2, 2), result_pages=result_pages)
    with pytest.raises(SalesforceOperationError, match=message):
        read_ids(pool, connection, True)


@pytest.mark.parametrize(
    "partitioned,listed,read",
    [
        (True, [0, 1], False),
        (False, [0, 1], False),
        (True, [1, 0, 2], True),
        (False, [1, 0, 2], False),
    ],
    ids=[
        "chunk_missing-parallel",
        "chunk_missing-sequential",
        "out_of_order-parallel",
        "out_of_order-sequential",
    ],
)
def test_bulk_result_pages_inconsistent(
    pool: ThreadPoolExecutor, partitioned: bool, listed: List[int], read: bool
) -> None:
    # a listing without a chunk raises, out of order it raises when chunks are read in sequence
    result_pages = [
        {
            "done": True,
            "resultChunks": [
                {
                    "resultLink": f"/jobs/query/{BULK_JOB_ID}/results?locator={bulk_locator(n)}"
                }
                for n in listed
            ],
        }
    ]
    chunks = bulk_chunks(2, 2, 2)
    connection = FakeBulkConnection(chunks, result_pages=result_pages)
    if read:
        assert_read_once(read_ids(pool, connection, partitioned), chunks, partitioned)
    else:
        with pytest.raises(SalesforceOperationError):
            read_ids(pool, connection, partitioned)


@pytest.mark.parametrize("partitioned", [True, False], ids=["parallel", "sequential"])
@pytest.mark.parametrize("offset_shift", [-1, 1])
def test_bulk_chunk_resumed_at_another_offset(
    pool: ThreadPoolExecutor, partitioned: bool, offset_shift: int
) -> None:
    # a server that reads resumed offsets differently returns the right number of records, but the
    # chunk then ends where no listed chunk starts
    chunks = bulk_chunks(4, 4)
    cut = len(BULK_HEADER) + len(chunks[0][0]) + len(chunks[0][1]) + 2

    def cut_first(n: int, params: Dict[str, Any]) -> Any:
        return cut if "maxRecords" not in params and chunk_of(params) == 0 else None

    connection = FakeBulkConnection(chunks, cut_first, offset_shift=offset_shift)
    with pytest.raises(
        SalesforceOperationError, match="not the start of a listed chunk"
    ):
        read_ids(pool, connection, partitioned, page_size=1)


@pytest.mark.parametrize("partitioned", [True, False], ids=["parallel", "sequential"])
@pytest.mark.parametrize("delta", [-1, 1])
def test_bulk_job_records(
    pool: ThreadPoolExecutor, partitioned: bool, delta: int
) -> None:
    chunks = bulk_chunks(2, 2, 2)
    connection = FakeBulkConnection(chunks, job_records=6 + delta)
    with pytest.raises(
        SalesforceOperationError, match=f"returned 6 of its {6 + delta} records"
    ):
        read_ids(pool, connection, partitioned)


@pytest.mark.parametrize(
    "partitioned,message",
    [
        (True, "2 result chunks .* end the results"),
        (False, "returned 2 of its 6 records"),
    ],
    ids=["parallel", "sequential"],
)
def test_bulk_chunk_ends_results_early(
    pool: ThreadPoolExecutor, partitioned: bool, message: str
) -> None:
    # the first chunk ends the results, its records still add up with the others in parallel
    connection = FakeBulkConnection(bulk_chunks(2, 2, 2), null_locators=(0,))
    with pytest.raises(SalesforceOperationError, match=message):
        read_ids(pool, connection, partitioned)


def test_bulk_chunk_closed_early() -> None:
    # pages that are not read are not downloaded and the response is closed
    connection = FakeBulkConnection(bulk_chunks(5))
    pages = BulkQueryApi(connection).query(  # type: ignore[arg-type]
        "SELECT", {}, False, 1, False, None
    )
    next(pages)
    pages.close()  # type: ignore[attr-defined]
    assert len(connection.results) == 1
    assert connection.responses[0].closed

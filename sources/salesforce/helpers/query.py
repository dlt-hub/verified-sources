"""Query APIs that run SOQL and yield CSV result pages in the Bulk API 2.0 format"""

import base64
import threading
from time import sleep
from typing import (
    Any,
    Dict,
    Generator,
    Iterator,
    List,
    Literal,
    NamedTuple,
    Optional,
    Protocol,
    Tuple,
    Union,
)
from urllib.parse import parse_qs, quote, urlparse

from dlt.common import Decimal, logger
from dlt.common.schema.typing import TTableSchemaColumns
from dlt.sources.helpers.requests import Response
from requests.exceptions import ChunkedEncodingError, ContentDecodingError
from requests.exceptions import ConnectionError as RequestsConnectionError
from simple_salesforce.exceptions import (
    SalesforceMalformedRequest,
    SalesforceOperationError,
    SalesforceResourceNotFound,
)
from tenacity import RetryCallState, Retrying, retry_if_exception

from ..settings import (
    CHUNK_DOWNLOAD_STOP,
    CHUNK_DOWNLOAD_WAIT,
    MAX_JOB_WAIT,
    MAX_POLL_INTERVAL,
    MAX_REST_URL_LENGTH,
    RESPONSE_READ_BYTES,
    REST_BATCH_SIZE,
)
from .connection import SalesforceConnection
from .csv_utils import TruncatedCsvError, rest_records_to_csv, split_csv_records

TQueryApi = Literal["auto", "bulk", "rest"]

# errors of a response body that broke while it streamed or ended early
BROKEN_DOWNLOAD_ERRORS = (
    ChunkedEncodingError,
    ContentDecodingError,
    RequestsConnectionError,
    TruncatedCsvError,
)


class ResultPage(NamedTuple):
    """A CSV result page and the number of its records if Salesforce sends it"""

    csv: bytes
    records: Optional[int] = None


# a result page, or the pages of a result chunk to be read in parallel with the other chunks
TQueryResult = Union[ResultPage, Iterator[ResultPage]]


class QueryApi(Protocol):
    def query(
        self,
        soql: str,
        columns: TTableSchemaColumns,
        query_all: bool,
        page_size: int,
        partitioned: bool = False,
        max_page_bytes: Optional[int] = None,
    ) -> Iterator[TQueryResult]:
        """Yields CSV result pages of `soql` selecting `columns`.

        `query_all` also returns deleted and archived records. `page_size` is the max number of
        records in a page, `max_page_bytes` its max size if the API streams its results. With
        `partitioned`, results that Salesforce splits into chunks are yielded as one iterator of
        pages per chunk, in no particular order.
        """
        ...


class ChunkLocator(NamedTuple):
    """A decoded Bulk API 2.0 result locator: a record offset in a result chunk"""

    offset: int
    chunk_id: str
    suffix: str

    @classmethod
    def decode(cls, locator: Optional[str]) -> Optional["ChunkLocator"]:
        """Returns None for a locator of another format: the format is not documented"""
        if not locator:
            return None
        try:
            offset, chunk_id, suffix = (
                base64.b64decode(locator + "=" * (-len(locator) % 4))
                .decode("ascii")
                .split(":")
            )
            return cls(int(offset), chunk_id, suffix)
        # also a `binascii.Error` or a `UnicodeDecodeError`
        except ValueError:
            return None

    def encode(self) -> str:
        return (
            base64.b64encode(f"{self.offset}:{self.chunk_id}:{self.suffix}".encode())
            .decode()
            .rstrip("=")
        )


class ChunkDownload:
    """Reads a result chunk page by page from a streamed response. After a break it opens a new
    response at the records already read, with `maxRecords` set to the rest of the chunk: a larger
    `maxRecords` goes on into the next chunk
    """

    def __init__(
        self,
        connection: SalesforceConnection,
        job_id: str,
        locator: Optional[str],
        page_size: int,
        max_page_bytes: Optional[int],
    ) -> None:
        self.connection = connection
        self.job_id = job_id
        self.locator = locator
        self.page_size = page_size
        self.max_page_bytes = max_page_bytes
        self.start = ChunkLocator.decode(locator)
        self.total: Optional[int] = None
        self.read = 0
        self.next_locator: Optional[str] = None
        self._response: Optional[Response] = None
        self._pages: Optional[Iterator[Tuple[bytes, int]]] = None

    def next_page(self) -> Optional[ResultPage]:
        """Returns the next page, None after the last one"""
        pages = self._pages or self._open()
        try:
            page, records = next(pages)
        except StopIteration:
            self._finish()
            return None
        except BROKEN_DOWNLOAD_ERRORS as ex:
            # a connection that broke after the last record leaves a complete body
            if self.read == self.total and not isinstance(ex, TruncatedCsvError):
                self._finish()
                return None
            self.close()
            raise
        self.read += records
        return ResultPage(page, records)

    def resumable(self, ex: BaseException) -> bool:
        return (
            isinstance(ex, BROKEN_DOWNLOAD_ERRORS)
            and self.start is not None
            and self.total is not None
            and self.read < self.total
        )

    def log_resume(self, retry_state: RetryCallState) -> None:
        ex = retry_state.outcome.exception() if retry_state.outcome else None
        logger.warning(
            f"Download of a result chunk of job {self.job_id} broke after {self.read} records,"
            f" resuming in {retry_state.upcoming_sleep:.0f} s: {ex}"
        )

    def close(self) -> None:
        if self._response is not None:
            self._response.close()
        self._response, self._pages = None, None

    def _open(self) -> Iterator[Tuple[bytes, int]]:
        params: Dict[str, Any] = {"locator": self.locator} if self.locator else {}
        if self.total is not None:
            if self.start is None:
                raise SalesforceOperationError(
                    f"A result chunk of job {self.job_id} cannot resume without a locator"
                )
            resume = self.start._replace(offset=self.start.offset + self.read)
            params = {"locator": resume.encode(), "maxRecords": self.total - self.read}
        response = self.connection.request(
            "GET",
            "bulk2",
            f"query/{self.job_id}/results",
            accept="text/csv",
            params=params,
            stream=True,
        )
        records = _records_header(response.headers)
        if self.total is None:
            self.total = records
        elif records != self.total - self.read:
            response.close()
            raise SalesforceOperationError(
                f"A resumed download of a result chunk of job {self.job_id} holds {records}"
                f" records, {self.total - self.read} were expected"
            )
        self._response = response
        self._pages = split_csv_records(
            response.iter_content(RESPONSE_READ_BYTES),
            self.page_size,
            self.max_page_bytes,
        )
        return self._pages

    def _finish(self) -> None:
        response = self._response
        self.close()
        if self.total is not None and self.read < self.total:
            raise TruncatedCsvError(
                f"A result chunk of job {self.job_id} ended after {self.read} of the"
                f" {self.total} records Salesforce counted"
            )
        if self.total is not None and self.read > self.total:
            raise SalesforceOperationError(
                f"A result chunk of job {self.job_id} holds {self.read} of the {self.total}"
                " records Salesforce counted"
            )
        next_locator = response.headers.get("Sforce-Locator") if response else None
        self.next_locator = (
            None if not next_locator or next_locator == "null" else next_locator
        )


class JobResults:
    """Checks the result chunks of a query job as they are read: a chunk must end at the start of
    a listed chunk or at the end of the results, and their records must add up to the records of
    the job when the last chunk ends, or when `check` is called
    """

    def __init__(
        self, job_id: str, expected: int, locators: List[Optional[str]]
    ) -> None:
        self.job_id = job_id
        self.expected = expected
        self.starts = {_chunk_key(locator) for locator in locators}
        self.chunks_left = len(locators)
        self.read = 0
        self._lock = threading.Lock()

    def chunk_read(self, records: int, next_locator: Optional[str]) -> None:
        """Adds a chunk read to its end, `next_locator` is where its last response ended"""
        if (
            self.starts
            and next_locator is not None
            and _chunk_key(next_locator) not in self.starts
        ):
            raise SalesforceOperationError(
                f"A result chunk of Bulk API job {self.job_id} ends at locator {next_locator},"
                " which is not the start of a listed chunk"
            )
        with self._lock:
            self.read += records
            self.chunks_left -= 1
            last = self.chunks_left == 0
        if last:
            self.check()

    def check(self) -> None:
        if self.read != self.expected:
            raise SalesforceOperationError(
                f"Bulk API query job {self.job_id} returned {self.read} of its"
                f" {self.expected} records"
            )


class BulkQueryApi:
    """Runs queries as Bulk API 2.0 query jobs"""

    def __init__(self, connection: SalesforceConnection) -> None:
        self.connection = connection

    def query(
        self,
        soql: str,
        columns: TTableSchemaColumns,
        query_all: bool,
        page_size: int,
        partitioned: bool = False,
        max_page_bytes: Optional[int] = None,
    ) -> Iterator[TQueryResult]:
        job_id = self._create_job(soql, query_all)
        job_records = self._wait_for_job(job_id)
        locators = self._result_chunks(job_id)
        if partitioned and len(locators) > 1:
            logger.info(
                f"Bulk API query job {job_id} has {len(locators)} result chunks"
            )
            results = JobResults(job_id, job_records, locators)
            for chunk_locator in locators:
                yield self._chunk_pages(
                    job_id, chunk_locator, page_size, max_page_bytes, results
                )
            return
        # the locators of the responses lead from chunk to chunk in the order of the query
        results = JobResults(job_id, job_records, locators)
        locator = locators[0] if locators else None
        while True:
            locator = yield from self._chunk_pages(
                job_id, locator, page_size, max_page_bytes, results
            )
            if locator is None:
                results.check()
                return

    def _result_chunks(self, job_id: str) -> List[Optional[str]]:
        """Returns the locators of the result chunks of a completed job, None for a chunk read
        without a locator
        """
        locators: List[Optional[str]] = []
        path = f"query/{job_id}/resultPages"
        while True:
            try:
                body = self.connection.request("GET", "bulk2", path).json()
            except (SalesforceResourceNotFound, SalesforceMalformedRequest) as ex:
                logger.warning(
                    f"Cannot list the result chunks of Bulk API job {job_id}, so they download one"
                    " after another and a broken download of the first one fails the load. Listing"
                    f" them needs API version 58.0 or later, set `version` in the credentials. {ex}"
                )
                return []
            # API v59.0 sends `resultChunks` and `resultLink`, the docs name `resultPages` and
            # `resultUrl`
            chunks = body.get("resultChunks", body.get("resultPages"))
            if chunks is None:
                logger.warning(
                    f"The result chunks of Bulk API job {job_id} come in an unknown format, so they"
                    " download one after another and a broken download of the first one fails the"
                    f" load: {list(body)}"
                )
                return []
            for chunk in chunks:
                link = chunk.get("resultLink") or chunk.get("resultUrl")
                locators.append(
                    parse_qs(urlparse(link).query).get("locator", [None])[0]
                )
            if body.get("done", True):
                break
            next_url = body.get("nextRecordsUrl") or body.get("nextRecordUrl")
            if not next_url:
                raise SalesforceOperationError(
                    f"The list of result chunks of Bulk API job {job_id} is not done and has no"
                    " next page"
                )
            # links start at the API version, e.g. `/jobs/query/<id>/resultPages?locator=...`
            path = next_url.split("/jobs/", 1)[-1]
        if len(set(locators)) != len(locators):
            raise SalesforceOperationError(
                f"The list of result chunks of Bulk API job {job_id} holds a chunk twice"
            )
        return locators

    def _chunk_pages(
        self,
        job_id: str,
        locator: Optional[str],
        page_size: int,
        max_page_bytes: Optional[int],
        job_results: JobResults,
    ) -> Generator[ResultPage, None, Optional[str]]:
        """Downloads the result chunk at `locator` in one streamed response and yields it in pages
        of at most `page_size` records and `max_page_bytes`. A broken download continues after the
        records already yielded. A chunk read to its end is checked by `job_results`. Returns the
        locator of the next chunk, None after the last one.
        """
        download = ChunkDownload(
            self.connection, job_id, locator, page_size, max_page_bytes
        )
        # tenacity keeps its state per thread and any extract worker may resume this generator,
        # so each page is read in a call of its own
        retrying = Retrying(
            retry=retry_if_exception(download.resumable),
            stop=CHUNK_DOWNLOAD_STOP,
            wait=CHUNK_DOWNLOAD_WAIT,
            before_sleep=download.log_resume,
            reraise=True,
        )
        try:
            while (page := retrying(download.next_page)) is not None:
                yield page
        finally:
            download.close()
        job_results.chunk_read(download.read, download.next_locator)
        return download.next_locator

    def _create_job(self, soql: str, query_all: bool) -> str:
        job = self.connection.request(
            "POST",
            "bulk2",
            "query",
            json={
                "operation": "queryAll" if query_all else "query",
                "query": soql,
                "contentType": "CSV",
                "columnDelimiter": "COMMA",
                "lineEnding": "LF",
            },
        ).json()
        return job["id"]  # type: ignore[no-any-return]

    def _wait_for_job(self, job_id: str) -> int:
        """Waits for a query job to complete, returns its number of records"""
        delay, waited = 0.5, 0.0
        while True:
            job = self.connection.request("GET", "bulk2", f"query/{job_id}").json()
            if job["state"] == "JobComplete":
                if job.get("numberRecordsProcessed") is None:
                    raise SalesforceOperationError(
                        f"Bulk API query job {job_id} completed without its number of records"
                    )
                return int(job["numberRecordsProcessed"])
            if job["state"] in ("Failed", "Aborted"):
                raise SalesforceOperationError(
                    f"Bulk API query job {job_id} {job['state']}: {job.get('errorMessage')}"
                )
            if waited >= MAX_JOB_WAIT:
                self.connection.request(
                    "PATCH", "bulk2", f"query/{job_id}", json={"state": "Aborted"}
                )
                raise SalesforceOperationError(
                    f"Bulk API query job {job_id} is still in state `{job['state']}` after"
                    f" {MAX_JOB_WAIT} seconds and was aborted"
                )
            sleep(delay)
            waited += delay
            delay = min(delay * 1.5, MAX_POLL_INTERVAL)


class RestQueryApi:
    """Pages through REST API query results"""

    def __init__(self, connection: SalesforceConnection) -> None:
        self.connection = connection

    def query(
        self,
        soql: str,
        columns: TTableSchemaColumns,
        query_all: bool,
        page_size: int,
        partitioned: bool = False,
        max_page_bytes: Optional[int] = None,
    ) -> Iterator[TQueryResult]:
        path = "queryAll" if query_all else "query"
        if (
            len(self.connection.client.base_url + path) + len(quote(soql)) + 3
            > MAX_REST_URL_LENGTH
        ):
            raise ValueError(
                f"SOQL query is too long for the REST API ({len(soql)} chars). Select fewer"
                " fields with `included_columns`."
            )
        batch_size = max(200, min(page_size, REST_BATCH_SIZE))
        headers = {"Sforce-Query-Options": f"batchSize={batch_size}"}
        result = self.connection.request(
            "GET", "rest", path, params={"q": soql}, headers=headers
        ).json(parse_float=Decimal)
        while True:
            yield ResultPage(rest_records_to_csv(result["records"], columns))
            if result["done"]:
                return
            result = self.connection.request(
                "GET", "instance", result["nextRecordsUrl"], headers=headers
            ).json(parse_float=Decimal)


class AutoQueryApi:
    """Runs queries with `bulk`, and with `rest` for sObjects or orgs without the Bulk API"""

    def __init__(self, bulk: QueryApi, rest: QueryApi) -> None:
        self.bulk = bulk
        self.rest = rest

    def query(
        self,
        soql: str,
        columns: TTableSchemaColumns,
        query_all: bool,
        page_size: int,
        partitioned: bool = False,
        max_page_bytes: Optional[int] = None,
    ) -> Iterator[TQueryResult]:
        pages = self.bulk.query(
            soql, columns, query_all, page_size, partitioned, max_page_bytes
        )
        try:
            # the job is created when the first page is requested
            first_page = next(pages)
        except StopIteration:
            return
        except SalesforceMalformedRequest as ex:
            if not is_bulk_not_supported(ex):
                raise
            logger.info(
                f"Bulk API cannot run `{soql[:100]}`, using REST API: {ex.content!r}"
            )
            yield from self.rest.query(soql, columns, query_all, page_size)
            return
        yield first_page
        yield from pages


def make_query_api(connection: SalesforceConnection, api: TQueryApi) -> QueryApi:
    """Returns the query API for "bulk", "rest" or "auto" """
    if api == "bulk":
        return BulkQueryApi(connection)
    if api == "rest":
        return RestQueryApi(connection)
    return AutoQueryApi(BulkQueryApi(connection), RestQueryApi(connection))


def _chunk_key(locator: Optional[str]) -> Any:
    """Compares locators by their decoded offset and chunk, so base64 padding does not matter"""
    return ChunkLocator.decode(locator) or locator


def _records_header(headers: Any) -> Optional[int]:
    records = headers.get("Sforce-NumberOfRecords")
    return None if records is None else int(records)


def is_bulk_not_supported(ex: SalesforceMalformedRequest) -> bool:
    """Tells if the Bulk API rejected a query job for the sObject or the org"""
    errors = ex.content if isinstance(ex.content, list) else [ex.content]
    for error in errors:
        if not isinstance(error, dict):
            continue
        # e.g. `Entity 'TaskStatus' is not supported by the Bulk API.`
        if error.get("errorCode") == "INVALIDENTITY" and "Bulk API" in error.get(
            "message", ""
        ):
            return True
        if error.get("errorCode") in ("FeatureNotEnabled", "FEATURE_NOT_ENABLED"):
            return True
        if "Selecting compound data not supported in Bulk Query" in error.get(
            "message", ""
        ):
            return True
    return False

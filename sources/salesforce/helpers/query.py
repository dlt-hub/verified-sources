"""Query APIs that run SOQL and yield CSV result pages in the Bulk API 2.0 format"""

from time import sleep
from typing import (
    Any,
    Dict,
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
from simple_salesforce.exceptions import (
    SalesforceMalformedRequest,
    SalesforceOperationError,
    SalesforceResourceNotFound,
)

from .connection import SalesforceConnection
from .csv_utils import rest_records_to_csv

TQueryApi = Literal["auto", "bulk", "rest"]

# max seconds between query job status checks
MAX_POLL_INTERVAL = 10.0
# max seconds to wait for a query job, then it is aborted
MAX_JOB_WAIT = 24 * 3600.0
# REST API rejects longer request URIs
MAX_REST_URL_LENGTH = 16384
# records in a REST API result page, 2000 is the max
REST_BATCH_SIZE = 2000


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
    ) -> Iterator[TQueryResult]:
        """Yields CSV result pages of `soql` selecting `columns`.

        `query_all` also returns deleted and archived records. `page_size` is the max number of
        records in a page. With `partitioned`, results that Salesforce splits into chunks are
        yielded as one iterator of pages per chunk, in no particular order.
        """
        ...


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
    ) -> Iterator[TQueryResult]:
        job_id = self._create_job(soql, query_all)
        self._wait_for_job(job_id)
        locators = self._result_chunks(job_id) if partitioned else []
        if len(locators) > 1:
            logger.info(
                f"Bulk API query job {job_id} has {len(locators)} result chunks"
            )
            for locator in locators:
                yield self._chunk_pages(job_id, locator, page_size)
            return
        yield from self._pages(job_id, page_size)

    def _pages(
        self,
        job_id: str,
        page_size: int,
        locator: Optional[str] = None,
        records: Optional[int] = None,
    ) -> Iterator[ResultPage]:
        """Yields the result pages from `locator` on, and stops after `records` records if given"""
        read = 0
        while records is None or read < records:
            max_records = (
                page_size if records is None else min(page_size, records - read)
            )
            page, locator = self._page(job_id, max_records, locator)
            yield page
            if records is not None and page.records is None:
                raise SalesforceOperationError(
                    f"A result page of job {job_id} came without `Sforce-NumberOfRecords`"
                )
            read += page.records or 0
            if (
                records is not None
                and (not page.records or locator is None)
                and read < records
            ):
                raise SalesforceOperationError(
                    f"A result chunk of job {job_id} ended after {read} of the {records} records"
                    " Salesforce counted"
                )
            if locator is None:
                return

    def _page(
        self, job_id: str, max_records: Optional[int], locator: Optional[str]
    ) -> Tuple[ResultPage, Optional[str]]:
        """Returns a result page and the locator of the next one, None after the last page"""
        params: Dict[str, Any] = {}
        if max_records is not None:
            params["maxRecords"] = max_records
        if locator:
            params["locator"] = locator
        response = self.connection.request(
            "GET", "bulk2", f"query/{job_id}/results", accept="text/csv", params=params
        )
        records = response.headers.get("Sforce-NumberOfRecords")
        next_locator = response.headers.get("Sforce-Locator")
        # null bytes are not valid in CSV
        page = ResultPage(
            response.content.replace(b"\x00", b""),
            None if records is None else int(records),
        )
        return (
            page,
            None if not next_locator or next_locator == "null" else next_locator,
        )

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
                    " after another. Listing them needs API version 58.0 or later, set `version`"
                    f" in the credentials. {ex}"
                )
                return []
            for chunk in body.get("resultChunks") or []:
                query = parse_qs(urlparse(chunk["resultLink"]).query)
                locators.append(query.get("locator", [None])[0])
            next_url = body.get("nextRecordsUrl")
            if body.get("done", True) or not next_url:
                return locators
            # links start at the API version, e.g. `/jobs/query/<id>/resultPages?locator=...`
            path = next_url.split("/jobs/", 1)[-1]

    def _chunk_pages(
        self, job_id: str, locator: Optional[str], page_size: int
    ) -> Iterator[ResultPage]:
        """Yields the pages of the result chunk at `locator`. The last page asks only for the
        records left in the chunk: a larger `maxRecords` goes on into the next chunk
        """
        records = self._chunk_records(job_id, locator)
        if records is None:
            # without `maxRecords` a response ends with its chunk
            yield self._page(job_id, None, locator)[0]
            return
        yield from self._pages(job_id, page_size, locator, records)

    def _chunk_records(self, job_id: str, locator: Optional[str]) -> Optional[int]:
        """Returns the number of records in the result chunk at `locator`, from the headers of a
        response closed before its body is read
        """
        response = self.connection.request(
            "GET",
            "bulk2",
            f"query/{job_id}/results",
            accept="text/csv",
            params={"locator": locator} if locator else {},
            stream=True,
        )
        response.close()
        records = response.headers.get("Sforce-NumberOfRecords")
        return None if records is None else int(records)

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

    def _wait_for_job(self, job_id: str) -> None:
        delay, waited = 0.5, 0.0
        while True:
            job = self.connection.request("GET", "bulk2", f"query/{job_id}").json()
            if job["state"] == "JobComplete":
                return
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
    ) -> Iterator[TQueryResult]:
        pages = self.bulk.query(soql, columns, query_all, page_size, partitioned)
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

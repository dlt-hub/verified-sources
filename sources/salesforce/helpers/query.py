"""Query APIs that run SOQL and yield CSV result pages in the Bulk API 2.0 format"""

from time import sleep
from typing import Any, Dict, Iterator, Literal, Protocol
from urllib.parse import quote

from dlt.common import Decimal, logger
from dlt.common.schema.typing import TTableSchemaColumns
from simple_salesforce.exceptions import (
    SalesforceMalformedRequest,
    SalesforceOperationError,
)

from .connection import SalesforceConnection
from .csv_utils import rest_records_to_csv

TQueryApi = Literal["auto", "bulk", "rest"]

# max seconds between query job status checks
MAX_POLL_INTERVAL = 10.0
# REST API rejects longer request URIs
MAX_REST_URL_LENGTH = 16384
# records in a REST API result page, 2000 is the max
REST_BATCH_SIZE = 2000


class QueryApi(Protocol):
    def query(
        self, soql: str, columns: TTableSchemaColumns, query_all: bool, page_size: int
    ) -> Iterator[bytes]:
        """Yields CSV result pages of `soql` selecting `columns`.

        `query_all` also returns deleted and archived records. `page_size` is the max number of
        records in a page.
        """
        ...


class BulkQueryApi:
    """Runs queries as Bulk API 2.0 query jobs"""

    def __init__(self, connection: SalesforceConnection) -> None:
        self.connection = connection

    def query(
        self, soql: str, columns: TTableSchemaColumns, query_all: bool, page_size: int
    ) -> Iterator[bytes]:
        job_id = self._create_job(soql, query_all)
        self._wait_for_job(job_id)
        params: Dict[str, Any] = {"maxRecords": page_size}
        while True:
            response = self.connection.request(
                "GET",
                "bulk2",
                f"query/{job_id}/results",
                accept="text/csv",
                params=params,
            )
            # null bytes are not valid in CSV
            yield response.content.replace(b"\x00", b"")
            locator = response.headers.get("Sforce-Locator")
            if not locator or locator == "null":
                return
            params["locator"] = locator

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
        delay = 0.5
        while True:
            job = self.connection.request("GET", "bulk2", f"query/{job_id}").json()
            if job["state"] == "JobComplete":
                return
            if job["state"] in ("Failed", "Aborted"):
                raise SalesforceOperationError(
                    f"Bulk API query job {job_id} {job['state']}: {job.get('errorMessage')}"
                )
            sleep(delay)
            delay = min(delay * 1.5, MAX_POLL_INTERVAL)


class RestQueryApi:
    """Pages through REST API query results"""

    def __init__(self, connection: SalesforceConnection) -> None:
        self.connection = connection

    def query(
        self, soql: str, columns: TTableSchemaColumns, query_all: bool, page_size: int
    ) -> Iterator[bytes]:
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
            yield rest_records_to_csv(result["records"], columns)
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
        self, soql: str, columns: TTableSchemaColumns, query_all: bool, page_size: int
    ) -> Iterator[bytes]:
        pages = self.bulk.query(soql, columns, query_all, page_size)
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
    return False

"""Runs SOQL queries with the Bulk API 2.0 or the REST API and yields CSV result pages"""

import threading
from time import sleep
from typing import Any, Callable, Dict, Iterator, Literal, Optional
from urllib.parse import quote

from dlt.common import Decimal, logger
from dlt.common.schema.typing import TTableSchemaColumns
from dlt.sources.helpers.requests import Response
from simple_salesforce import Salesforce
from simple_salesforce.exceptions import (
    SalesforceExpiredSession,
    SalesforceMalformedRequest,
    SalesforceOperationError,
)
from simple_salesforce.util import call_salesforce

from .pages import rest_records_to_csv

TQueryApi = Literal["auto", "bulk", "rest"]

# max seconds between query job status checks
MAX_POLL_INTERVAL = 10.0
# REST API rejects longer request URIs
MAX_REST_URL_LENGTH = 16384
# records in a REST API result page, 2000 is the max
REST_BATCH_SIZE = 2000


class QueryClient:
    """Queries sObjects with a Bulk API 2.0 job, or with the REST API when Bulk is not supported.

    Both APIs yield CSV pages in the Bulk API format. Logs in with `login` on first use and when
    the session expires. Instances may be shared by threads.
    """

    def __init__(
        self, client: Optional[Salesforce], login: Callable[[], Salesforce]
    ) -> None:
        self._client = client
        self.login = login
        self._login_lock = threading.Lock()

    @property
    def client(self) -> Salesforce:
        """Logged in client"""
        if self._client is None:
            with self._login_lock:
                if self._client is None:
                    self._client = self.login()
        return self._client

    @client.setter
    def client(self, client: Salesforce) -> None:
        self._client = client

    def describe(self, sobject: str) -> Dict[str, Any]:
        """Returns the `describe()` result of `sobject`"""
        return self._request("GET", "rest", f"sobjects/{sobject}/describe").json()  # type: ignore[no-any-return]

    def query(
        self,
        soql: str,
        columns: TTableSchemaColumns,
        max_records: int,
        query_all: bool,
        api: TQueryApi = "auto",
    ) -> Iterator[bytes]:
        """Yields CSV result pages of `soql` selecting `columns`.

        `query_all` also returns deleted and archived records. With `api="auto"` the REST API is
        used for sObjects not supported by the Bulk API and for orgs without the Bulk API.
        """
        if api != "rest":
            try:
                job_id = self._create_bulk2_job(soql, query_all)
            except SalesforceMalformedRequest as ex:
                if api == "bulk" or not _is_bulk_not_supported(ex):
                    raise
                logger.info(
                    f"Bulk API cannot run `{soql[:100]}`, using REST API: {ex.content!r}"
                )
            else:
                yield from self._bulk2_pages(job_id, max_records)
                return
        yield from self._rest_pages(soql, columns, query_all, max_records)

    def _create_bulk2_job(self, soql: str, query_all: bool) -> str:
        job = self._request(
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

    def _bulk2_pages(self, job_id: str, max_records: int) -> Iterator[bytes]:
        self._wait_for_job(job_id)
        params: Dict[str, Any] = {"maxRecords": max_records}
        while True:
            response = self._request(
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

    def _wait_for_job(self, job_id: str) -> None:
        delay = 0.5
        while True:
            job = self._request("GET", "bulk2", f"query/{job_id}").json()
            if job["state"] == "JobComplete":
                return
            if job["state"] in ("Failed", "Aborted"):
                raise SalesforceOperationError(
                    f"Bulk API query job {job_id} {job['state']}: {job.get('errorMessage')}"
                )
            sleep(delay)
            delay = min(delay * 1.5, MAX_POLL_INTERVAL)

    def _rest_pages(
        self, soql: str, columns: TTableSchemaColumns, query_all: bool, max_records: int
    ) -> Iterator[bytes]:
        path = "queryAll" if query_all else "query"
        if (
            len(self.client.base_url + path) + len(quote(soql)) + 3
            > MAX_REST_URL_LENGTH
        ):
            raise ValueError(
                f"SOQL query is too long for the REST API ({len(soql)} chars). Select fewer"
                " fields with `included_columns`."
            )
        batch_size = max(200, min(max_records, REST_BATCH_SIZE))
        headers = {"Sforce-Query-Options": f"batchSize={batch_size}"}
        result = self._request(
            "GET", "rest", path, params={"q": soql}, headers=headers
        ).json(parse_float=Decimal)
        while True:
            yield rest_records_to_csv(result["records"], columns)
            if result["done"]:
                return
            result = self._request(
                "GET", "instance", result["nextRecordsUrl"], headers=headers
            ).json(parse_float=Decimal)

    def _request(
        self,
        method: str,
        api: Literal["bulk2", "rest", "instance"],
        path: str,
        accept: str = "application/json",
        **kwargs: Any,
    ) -> Response:
        client = self.client
        try:
            return self._call(client, method, api, path, accept, **kwargs)
        except SalesforceExpiredSession:
            # a long job may outlive the session. job ids, result locators and REST cursors stay
            # valid in the new session so the call is repeated
            with self._login_lock:
                # threads sharing this instance log in once, others use the new session
                if self._client is client:
                    logger.info("Salesforce session expired, logging in again")
                    self._client = self.login()
            return self._call(self.client, method, api, path, accept, **kwargs)

    @staticmethod
    def _call(
        client: Salesforce,
        method: str,
        api: Literal["bulk2", "rest", "instance"],
        path: str,
        accept: str,
        headers: Dict[str, str] = None,
        **kwargs: Any,
    ) -> Response:
        base_url = {
            "bulk2": client.bulk2_url,
            "rest": client.base_url,
            # REST API next page urls start with the path
            "instance": f"https://{client.sf_instance}",
        }[api]
        return call_salesforce(
            url=base_url + path,
            method=method,
            session=client.session,
            headers={
                "Authorization": "Bearer " + client.session_id,
                "Content-Type": "application/json",
                "Accept": accept,
                **(headers or {}),
            },
            **kwargs,
        )


def _is_bulk_not_supported(ex: SalesforceMalformedRequest) -> bool:
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

import base64
import csv
import gzip
import io
import json
import os
import random
import re
import threading
from datetime import date, datetime, timedelta, timezone  # noqa: I251
from time import sleep
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple

import dlt
from dlt.common import Decimal
from dlt.common.arithmetics import ROUND_HALF_UP
from dlt.common.configuration import with_config
from dlt.common.time import ensure_datetime_in_tz
from requests.exceptions import ChunkedEncodingError
from simple_salesforce import Salesforce

from sources.salesforce.helpers.credentials import (
    SalesforceAuth,
    make_salesforce_client,
)
from sources.salesforce.helpers.csv_utils import split_csv_records

from tests.salesforce.seed import data
from tests.utils import load_tables_to_dicts

CASES_DIR = os.path.join(os.path.dirname(__file__), "cases")


def load_captured_describe() -> List[Dict[str, Any]]:
    """Returns `DltTypes__c` fields captured from the test org by the seed script"""
    with open(
        os.path.join(CASES_DIR, "dlt_types__c.describe.json"), encoding="utf-8"
    ) as f:
        return json.load(f)["fields"]  # type: ignore[no-any-return]


def load_captured_csv() -> str:
    """Returns a `DltTypes__c` Bulk API 2.0 result page captured from the test org"""
    with gzip.open(
        os.path.join(CASES_DIR, "dlt_types__c.csv.gz"),
        "rt",
        encoding="utf-8",
        newline="",
    ) as f:
        return f.read()


def expected_dlt_types(double_as_decimal: bool) -> Dict[str, Dict[str, Any]]:
    """Returns values of seeded `DltTypes__c` records as loaded, by seed id and field name"""
    decimal_fields = {"Currency__c", "FormulaCurrency__c"}
    if double_as_decimal:
        decimal_fields |= {
            "Number18__c",
            "Number16_2__c",
            "Percent__c",
            "Location__Latitude__s",
            "Location__Longitude__s",
            "FormulaNumber__c",
            "FormulaRatio__c",
        }
    # fields not set in a record are expected to be null
    seeded_fields = {name for seeded in data.DLT_TYPES_RECORDS for name in seeded}
    expected = {}
    for seeded in data.DLT_TYPES_RECORDS:
        record = {
            name: value
            for name, value in seeded.items()
            # relationship ids are org specific
            if not name.endswith("__r")
        }
        record.update(data.STORED_AS.get(record["Seed_Id__c"], {}))
        for name in seeded_fields:
            if not name.endswith("__r"):
                record.setdefault(name, None)
        if record["Checkbox__c"] is None:
            record["Checkbox__c"] = False
        currency = record.get("Currency__c")
        record["FormulaNumber__c"] = None if currency is None else currency * 2
        record["FormulaText__c"] = (record.get("Text__c") or "") + "!"
        record["FormulaCheckbox__c"] = currency is not None and currency > 0
        # Salesforce rounds formula values to the field scale
        record["FormulaCurrency__c"] = (
            None if currency is None else round_cents(currency / 3)
        )
        number = record.get("Number16_2__c")
        record["FormulaRatio__c"] = None if number is None else round_cents(number / 7)
        day = record.get("Date__c")
        if day is not None and day < date(4000, 12, 31):
            record["FormulaDate__c"] = day + timedelta(days=1)
        else:
            record["FormulaDate__c"] = day
        for name, value in record.items():
            if isinstance(value, (int, Decimal)) and not isinstance(value, bool):
                record[name] = (
                    Decimal(value) if name in decimal_fields else float(value)
                )
        expected[record["Seed_Id__c"]] = record
    return expected


def round_cents(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def assert_dlt_types_loaded(pipeline: dlt.Pipeline, double_as_decimal: bool) -> None:
    """Compares loaded `DltTypes__c` rows with the seeded records, field by field"""
    # salesforce names end with `__c`, dlt keeps `__` as a path separator
    naming = pipeline.default_schema.naming
    table_name = naming.normalize_path(data.DLT_TYPES)
    rows = {
        row[naming.normalize_path("Seed_Id__c")]: row
        for row in load_tables_to_dicts(pipeline, table_name)[table_name]
    }
    expected = expected_dlt_types(double_as_decimal)
    # deleted records are removed by the hard delete hint
    assert set(rows) == set(expected)
    for seed_id, record in expected.items():
        row = rows[seed_id]
        for name, value in record.items():
            assert row[naming.normalize_path(name)] == value, (seed_id, name)
        assert re.fullmatch(r"DT-\d{4,}", row[naming.normalize_path("AutoNumber__c")])


@with_config(sections=("sources", "salesforce"))
def salesforce_client(credentials: SalesforceAuth = dlt.secrets.value) -> Salesforce:
    """Returns a client of the test org"""
    return make_salesforce_client(credentials)


@with_config(sections=("sources", data.PERF_SECTION))
def performance_credentials(
    credentials: SalesforceAuth = dlt.secrets.value,
) -> SalesforceAuth:
    """Returns the credentials of the performance org"""
    return credentials


class PageBreaks:
    """Replaces `split_csv_records` of the query module: breaks the download of a result chunk at
    one random page in every `every` pages, counted over all chunks
    """

    def __init__(self, rng: random.Random, every: int = 10) -> None:
        self.rng, self.every = rng, every
        self.pages = 0
        self.breaks = 0
        self._break_at = rng.randrange(every)
        self._lock = threading.Lock()

    def split(self, *args: Any, **kwargs: Any) -> Iterator[Tuple[bytes, int]]:
        for page in split_csv_records(*args, **kwargs):
            with self._lock:
                n, self.pages = self.pages, self.pages + 1
                broken = n == self._break_at
                if broken:
                    self.breaks += 1
                    self._break_at = (
                        n // self.every + 1
                    ) * self.every + self.rng.randrange(self.every)
            if broken:
                raise ChunkedEncodingError(f"page {n} broken by the test")
            yield page


def perf_wide_records(credentials: SalesforceAuth) -> int:
    """Returns the records of the wide object that a load with deleted records reads: the seeded
    ones and at most `PERF_WIDE_MAX_DELETED` deleted ones
    """
    client = make_salesforce_client(credentials)
    count: int = client.query(
        f"SELECT COUNT() FROM {data.PERF_WIDE_OBJECT}", include_deleted=True
    )["totalSize"]
    most = data.PERF_WIDE_RECORDS + data.PERF_WIDE_MAX_DELETED
    if not data.PERF_WIDE_RECORDS <= count <= most:
        raise ValueError(
            f"{data.PERF_WIDE_OBJECT} holds {count} records with the deleted ones, expected"
            f" {data.PERF_WIDE_RECORDS} to {most}"
        )
    return count


def write_records(
    client: Salesforce, method: str, sobject: str, records: List[Dict[str, Any]]
) -> List[str]:
    """Creates (POST) or updates (PATCH) `records` of `sobject`, returns their ids"""
    results = client.restful(
        "composite/sobjects",
        method=method,
        json={
            "allOrNone": True,
            "records": [
                {"attributes": {"type": sobject}, **record} for record in records
            ],
        },
    )
    assert all(result["success"] for result in results), results
    return [result["id"] for result in results]


def delete_records(client: Salesforce, ids: List[str]) -> None:
    results = client.restful(
        "composite/sobjects",
        method="DELETE",
        params={"ids": ",".join(ids), "allOrNone": "true"},
    )
    assert all(result["success"] for result in results), results


def wait_for_org_clock(client: Salesforce, sobject: str, condition: str) -> None:
    """Waits until the local clock passes the last change of the `sobject` records matching
    `condition`. Incremental runs read changes until the local time"""
    (record,) = client.query_all(
        f"SELECT SystemModstamp FROM {sobject} WHERE {condition}"
        " ORDER BY SystemModstamp DESC LIMIT 1",
        include_deleted=True,
    )["records"]
    last_change = ensure_datetime_in_tz(record["SystemModstamp"])
    wait = (last_change - datetime.now(timezone.utc)).total_seconds() + 1
    if wait > 0:
        sleep(wait)


def count_rows(pipeline: dlt.Pipeline, table_name: str, condition: str) -> int:
    """Counts rows of `table_name` matching the SQL `condition`"""
    with pipeline.sql_client() as client:
        table = client.make_qualified_table_name(table_name)
        rows = client.execute_sql(f"SELECT COUNT(*) FROM {table} WHERE {condition}")
    return rows[0][0]  # type: ignore[no-any-return]


def load_rows(
    pipeline: dlt.Pipeline, sobject: str, *fields: str
) -> List[Tuple[Any, ...]]:
    """Returns the sorted values of `fields` in the loaded `sobject` rows"""
    naming = pipeline.default_schema.naming
    table_name = naming.normalize_path(sobject)
    rows = load_tables_to_dicts(pipeline, table_name)[table_name]
    return sorted(
        tuple(row[naming.normalize_path(field)] for field in fields) for row in rows
    )


BULK_JOB_ID = "750JOB"
BULK_HEADER = '"Id","Value"\n'


def bulk_locator(chunk: int, offset: int = 0) -> str:
    """A Bulk API 2.0 result locator, `<offset>:chunk<n>:0` in base64 without padding"""
    return base64.b64encode(f"{offset}:chunk{chunk}:0".encode()).decode().rstrip("=")


def bulk_chunks(*sizes: int) -> List[List[str]]:
    """CSV records in chunks of `sizes`, ids count up from `000` and values hold quotes, commas
    and newlines
    """
    values = ["", 'a"b', "multi\nline", "x,y", '""\n"']
    chunks, n = [], 0
    for size in sizes:
        chunk = []
        for _ in range(size):
            buffer = io.StringIO()
            csv.writer(buffer, quoting=csv.QUOTE_ALL, lineterminator="\n").writerow(
                [f"{n:03d}", values[n % len(values)]]
            )
            chunk.append(buffer.getvalue())
            n += 1
        chunks.append(chunk)
    return chunks


class FakeBulkResponse:
    def __init__(
        self,
        body: bytes = b"",
        headers: Optional[Dict[str, str]] = None,
        payload: Any = None,
        cut: Optional[int] = None,
        broken: bool = True,
    ) -> None:
        self.body, self.headers, self.payload = body, headers or {}, payload
        self.cut, self.broken = cut, broken
        self.closed = False

    def json(self, **kwargs: Any) -> Any:
        return self.payload

    def iter_content(self, size: int) -> Iterator[bytes]:
        end = len(self.body) if self.cut is None else self.cut
        for start in range(0, end, size):
            yield self.body[start : min(start + size, end)]
        if self.cut is not None and self.broken:
            raise ChunkedEncodingError("broken by the test")

    def close(self) -> None:
        self.closed = True


class FakeBulkConnection:
    """A `SalesforceConnection` to a Bulk API 2.0 job whose results are in `chunks`.

    Like Salesforce, a results request without `maxRecords` returns one chunk, with `maxRecords`
    it goes on into the next chunks, and `resultPages` lists the chunks. `cut` returns where the
    body of the n-th results request with `params` ends, None for a whole body: with `broken` the
    connection breaks there, else the body ends early. `offset_shift` moves the offsets the server
    reads from the locators of resumed downloads. `result_pages` replaces the listing.
    """

    def __init__(
        self,
        chunks: List[List[str]],
        cut: Callable[[int, Dict[str, Any]], Optional[int]] = lambda n, params: None,
        broken: bool = True,
        offset_shift: int = 0,
        result_pages: Optional[List[Dict[str, Any]]] = None,
        job_records: Optional[int] = None,
    ) -> None:
        self.chunks, self.cut, self.broken = chunks, cut, broken
        self.offset_shift = offset_shift
        self.result_pages = result_pages or [
            {
                "done": True,
                "nextRecordsUrl": None,
                "resultChunks": [
                    {
                        "resultLink": f"/jobs/query/{BULK_JOB_ID}/results?locator={bulk_locator(n)}"
                    }
                    for n in range(len(chunks))
                ],
            }
        ]
        self.job_records = (
            sum(len(chunk) for chunk in chunks) if job_records is None else job_records
        )
        self.results: List[Dict[str, Any]] = []
        self.responses: List[FakeBulkResponse] = []
        self._lock = threading.Lock()

    def request(
        self, method: str, base: str, path: str, params: Any = None, **kwargs: Any
    ) -> FakeBulkResponse:
        if method == "POST":
            return FakeBulkResponse(payload={"id": BULK_JOB_ID})
        if path == f"query/{BULK_JOB_ID}":
            return FakeBulkResponse(
                payload={
                    "state": "JobComplete",
                    "numberRecordsProcessed": self.job_records,
                }
            )
        if "resultPages" in path:
            page = int(path.rsplit("=", 1)[1]) if "=" in path else 0
            return FakeBulkResponse(payload=self.result_pages[page])
        params = dict(params or {})
        with self._lock:
            n = len(self.results)
            self.results.append(params)
        locator = params.get("locator") or bulk_locator(0)
        offset, name, _ = (
            base64.b64decode(locator + "=" * (-len(locator) % 4)).decode().split(":")
        )
        chunk, position = int(name[len("chunk") :]), int(offset)
        if "maxRecords" in params:
            position += self.offset_shift
            wanted, records = int(params["maxRecords"]), []
            while len(records) < wanted and chunk < len(self.chunks):
                taken = self.chunks[chunk][position : position + wanted - len(records)]
                records += taken
                position += len(taken)
                if position >= len(self.chunks[chunk]):
                    chunk, position = chunk + 1, 0
        else:
            records = self.chunks[chunk][position:]
            chunk, position = chunk + 1, 0
        headers = {
            "Sforce-Locator": (
                bulk_locator(chunk, position) if chunk < len(self.chunks) else "null"
            ),
            "Sforce-NumberOfRecords": str(len(records)),
        }
        response = FakeBulkResponse(
            (BULK_HEADER + "".join(records)).encode(),
            headers,
            cut=self.cut(n, params),
            broken=self.broken,
        )
        self.responses.append(response)
        return response

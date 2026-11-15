import gzip
import json
import os
import re
from datetime import date, datetime, timedelta, timezone  # noqa: I251
from time import sleep
from typing import Any, Dict, List, Tuple

import dlt
from dlt.common import Decimal
from dlt.common.arithmetics import ROUND_HALF_UP
from dlt.common.configuration import with_config
from dlt.common.time import ensure_datetime_in_tz
from simple_salesforce import Salesforce

from sources.salesforce.helpers.credentials import (
    SalesforceAuth,
    make_salesforce_client,
)

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

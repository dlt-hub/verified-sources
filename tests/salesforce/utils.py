import gzip
import json
import os
import re
from datetime import date, timedelta  # noqa: I251
from typing import Any, Dict, List

import dlt
from dlt.common import Decimal

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
    decimal_fields = {"Currency__c"}
    if double_as_decimal:
        decimal_fields |= {
            "Number18__c",
            "Number16_2__c",
            "Percent__c",
            "Location__Latitude__s",
            "Location__Longitude__s",
            "FormulaNumber__c",
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


def count_rows(pipeline: dlt.Pipeline, table_name: str, condition: str) -> int:
    """Counts rows of `table_name` matching the SQL `condition`"""
    with pipeline.sql_client() as client:
        table = client.make_qualified_table_name(table_name)
        rows = client.execute_sql(f"SELECT COUNT(*) FROM {table} WHERE {condition}")
    return rows[0][0]  # type: ignore[no-any-return]

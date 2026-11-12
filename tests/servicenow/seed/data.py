"""Synthetic records seeded into the ServiceNow test instance and the values loaded from them"""

import os
from datetime import date, datetime, time, timedelta, timezone  # noqa: I251
from typing import Any, Dict, List

from dlt.common import Decimal

DLT_TYPES = "u_dlt_types"
DLT_PAGES = "u_dlt_pages"
DLT_INCREMENTAL = "u_dlt_incremental"
# records written by `test_incremental_changes`, tagged with the id of the test run
DLT_CHANGES = "u_dlt_changes"
SEED_ID_FIELD = "u_seed_id"
# `DLT_PAGES` has records for two full pages and a short one
PAGE_SIZE = 100
PAGE_RECORDS = 250
# `DLT_INCREMENTAL` has records for five chunks and a short one
CHUNK_SIZE = 200
INCREMENTAL_RECORDS = 1050
INCREMENTAL_CURSOR = "u_updated_at"
# responses of the seeded instance captured by `seed.py capture`
CASES_DIR = os.path.join(os.path.dirname(__file__), "..", "cases")
CAPTURED_DICTIONARY = os.path.join(CASES_DIR, f"{DLT_TYPES}.dictionary.json")
CAPTURED_PAGE = os.path.join(CASES_DIR, f"{DLT_TYPES}.page.json")
CAPTURED_EXPORT = os.path.join(CASES_DIR, f"{DLT_TYPES}.export.csv")
CAPTURED_EXPORT_HEADERS = os.path.join(CASES_DIR, f"{DLT_TYPES}.export.json")
# responses captured by `seed.py capture-hidden` while a field ACL hides `HIDDEN_FIELD`
HIDDEN_FIELD = "sys_updated_on"
HIDDEN_CAPTURE_FIELDS = ["sys_id", SEED_ID_FIELD, "u_when", HIDDEN_FIELD]
CAPTURED_HIDDEN_PAGE = os.path.join(CASES_DIR, f"{DLT_PAGES}.hidden.page.json")
CAPTURED_HIDDEN_EXPORT = os.path.join(CASES_DIR, f"{DLT_PAGES}.hidden.export.csv")
# seed ids of records created and deleted, so `sys_audit_delete` has records of `DLT_TYPES`
DELETED_SEED_IDS = ["deleted-1", "deleted-2"]

# fields of `DLT_TYPES` by internal type, besides `SEED_ID_FIELD`
FIELDS: Dict[str, Dict[str, Any]] = {
    "u_string": {"internal_type": "string", "max_length": 1000},
    "u_integer": {"internal_type": "integer"},
    "u_longint": {"internal_type": "longint"},
    "u_decimal": {"internal_type": "decimal"},
    "u_float": {"internal_type": "float"},
    "u_currency": {"internal_type": "currency"},
    "u_boolean": {"internal_type": "boolean"},
    "u_date_time": {"internal_type": "glide_date_time"},
    "u_date": {"internal_type": "glide_date"},
    "u_time": {"internal_type": "glide_time"},
    "u_duration": {"internal_type": "glide_duration"},
    "u_due_date": {"internal_type": "due_date"},
    "u_reference": {"internal_type": "reference", "reference": "sys_user"},
    "u_choice": {"internal_type": "choice", "max_length": 40},
    "u_html": {"internal_type": "html", "max_length": 4000},
    "u_url": {"internal_type": "url", "max_length": 1024},
    "u_journal": {"internal_type": "journal_input"},
}

# fields of `DLT_PAGES` besides `SEED_ID_FIELD`
PAGE_FIELDS: Dict[str, Dict[str, Any]] = {
    "u_number": {"internal_type": "integer"},
    "u_when": {"internal_type": "glide_date_time"},
}
# `u_when` of more than a page of records is empty
EMPTY_WHEN_RECORDS = 150

UTC = timezone.utc

# values as loaded, durations in seconds. None is an empty field
RECORDS: List[Dict[str, Any]] = [
    {
        SEED_ID_FIELD: "types-1",
        "u_string": "multi\nline, with \"quotes\", 'apostrophes' and unicode ✓ zażółć",
        "u_integer": 2147483647,
        "u_longint": 9007199254740993,
        "u_decimal": Decimal("-1234.56"),
        "u_float": 0.1,
        "u_currency": Decimal("1234.56"),
        "u_boolean": True,
        "u_date_time": datetime(2024, 2, 29, 23, 59, 59, tzinfo=UTC),
        "u_date": date(2024, 2, 29),
        "u_time": time(12, 34, 56),
        "u_duration": 2 * 86400 + 4 * 3600 + 5,
        "u_due_date": datetime(2030, 12, 31, 0, 0, 0, tzinfo=UTC),
        "u_choice": "2",
        "u_html": "<p>bold <b>text</b> &amp; entities</p>",
        "u_url": "https://dlthub.com/docs?a=1&b=2",
    },
    {
        # null-like text, zero and false. "NULL" written with REST empties the field
        SEED_ID_FIELD: "types-null-like",
        "u_string": "None",
        "u_integer": 0,
        "u_longint": -9007199254740993,
        "u_decimal": Decimal("0.01"),
        "u_float": -1.5e10,
        "u_currency": Decimal("0"),
        "u_boolean": False,
        "u_date_time": datetime(1970, 1, 1, 0, 0, 0, tzinfo=UTC),
        "u_date": date(1970, 1, 1),
        "u_time": time(0, 0, 0),
        "u_duration": 0,
        "u_choice": "NA",
    },
    {
        SEED_ID_FIELD: "types-number-like",
        "u_string": "00123",
        "u_integer": -2147483648,
        "u_decimal": Decimal("99999999.99"),
        "u_float": 1e-7,
        "u_date_time": datetime(2099, 12, 31, 23, 59, 59, tzinfo=UTC),
        "u_date": date(2099, 12, 31),
        "u_time": time(23, 59, 59),
        "u_duration": 400 * 86400,
    },
    {
        SEED_ID_FIELD: "types-whitespace",
        "u_string": "  padded  ",
        "u_html": " ",
    },
    # CSV exports put an apostrophe before text that starts with a formula character
    {SEED_ID_FIELD: "types-formula-eq", "u_string": "=SUM(A1:A2)"},
    {SEED_ID_FIELD: "types-formula-plus", "u_string": "+1 555 0100"},
    {SEED_ID_FIELD: "types-formula-minus", "u_string": "-dash", "u_url": "-1"},
    {SEED_ID_FIELD: "types-formula-at", "u_string": "@user"},
    {SEED_ID_FIELD: "types-apostrophe", "u_string": "'quoted"},
    # all fields empty
    {SEED_ID_FIELD: "types-empty"},
]


# values of `FIELDS` loaded from empty fields, booleans and currencies are never empty
EMPTY_VALUES: Dict[str, Any] = {"u_boolean": False, "u_currency": Decimal("0")}
# choices of `u_choice`, values not in `sys_choice` are not stored
CHOICES = {"2": "Two", "NA": "Not applicable"}


def expected_value(record: Dict[str, Any], field: str) -> Any:
    """Returns the value of `field` loaded from `record`"""
    value = record.get(field)
    return EMPTY_VALUES.get(field) if value is None else value


def page_records() -> List[Dict[str, Any]]:
    return [
        {
            SEED_ID_FIELD: f"page-{i:04d}",
            "u_number": i,
            "u_when": (
                None
                if i < EMPTY_WHEN_RECORDS
                else datetime(2024, 1, 1, tzinfo=UTC) + timedelta(minutes=i)
            ),
        }
        for i in range(PAGE_RECORDS)
    ]


INCREMENTAL_FIELDS: Dict[str, Dict[str, Any]] = {
    "u_number": {"internal_type": "integer"},
    INCREMENTAL_CURSOR: {"internal_type": "glide_date_time"},
}


def incremental_records() -> List[Dict[str, Any]]:
    """Records with unique cursor values in 2024, at the second precision of the instance and in
    an order unrelated to their seed ids. The platform sets `sys_updated_on` on every write, so
    the seed cannot fix it
    """
    return [
        {
            SEED_ID_FIELD: f"incremental-{i:04d}",
            "u_number": i,
            # 1000003 is coprime with the seconds of 2024. `random` may change between Python versions
            INCREMENTAL_CURSOR: datetime(2024, 1, 1, tzinfo=UTC)
            + timedelta(seconds=i * 1_000_003 % (366 * 24 * 3600)),
        }
        for i in range(INCREMENTAL_RECORDS)
    ]


CHANGES_FIELDS: Dict[str, Dict[str, Any]] = {
    "u_run_id": {"internal_type": "string", "max_length": 64},
    "u_value": {"internal_type": "integer"},
}


# a cursor value after the time a run starts, read only by a range that ends after it
FUTURE_RECORD: Dict[str, Any] = {
    SEED_ID_FIELD: "incremental-future",
    "u_number": INCREMENTAL_RECORDS,
    INCREMENTAL_CURSOR: datetime(2032, 1, 1, tzinfo=UTC),
}

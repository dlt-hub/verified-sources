import csv
import io
import json
import re
import sys
from datetime import date, datetime, time, timezone  # noqa: I251
from typing import Any, Dict, Iterator, List, Optional, Tuple
from urllib.parse import parse_qs, urlparse

import dlt
import pyarrow as pa
import pytest
import requests_mock
from dlt.common import Decimal
from dlt.common.schema.typing import TWriteDisposition
from dlt.extract.exceptions import ResourceExtractionError
from pytest_mock import MockerFixture
from simple_salesforce.exceptions import (
    SalesforceMalformedRequest,
    SalesforceExpiredSession,
    SalesforceOperationError,
)

from sources.salesforce import SOURCE_SOBJECTS, salesforce_source, salesforce_table
from sources.salesforce.helpers.client import (
    ConsumerKeySecretDomainAuth,
    InstanceAuth,
    make_salesforce_client,
)
from sources.salesforce.helpers.schema_types import sf_field_to_column_schema
from sources.salesforce.helpers.query import QueryClient, TQueryApi
from sources.salesforce.helpers import table as table_module
from sources.salesforce.helpers.table import TTableBackend

from tests.salesforce.seed import data
from tests.salesforce.utils import (
    assert_dlt_types_loaded,
    load_captured_csv,
    load_captured_describe,
)
from tests.utils import assert_load_info, load_table_counts, load_tables_to_dicts

JOB_ID = "750R0000000zxr8IAA"
LOCATORS = ["", "MTAw", "MjAw", "MzAw"]

DESCRIBE_FIELDS: List[Dict[str, Any]] = [
    {"name": "Id", "type": "id", "nillable": False},
    {"name": "Name", "type": "string", "nillable": True},
    {"name": "IsDeleted", "type": "boolean", "nillable": False},
    {"name": "NumberOfEmployees", "type": "int", "nillable": True},
    {
        "name": "AnnualRevenue",
        "type": "currency",
        "precision": 18,
        "scale": 0,
        "nillable": True,
    },
    {
        "name": "Amount__c",
        "type": "currency",
        "precision": 16,
        "scale": 2,
        "nillable": True,
    },
    {
        "name": "Probability",
        "type": "percent",
        "precision": 3,
        "scale": 0,
        "nillable": True,
    },
    {"name": "SystemModstamp", "type": "datetime", "nillable": False},
    {"name": "CloseDate", "type": "date", "nillable": True},
    {"name": "StartTime__c", "type": "time", "nillable": True},
    {"name": "Description", "type": "textarea", "nillable": True},
    {"name": "Tags__c", "type": "multipicklist", "nillable": True},
    {"name": "BillingAddress", "type": "address", "nillable": True},
    {"name": "Logo__c", "type": "base64", "nillable": True},
]
QUERIED_FIELDS = (
    "Id, Name, IsDeleted, NumberOfEmployees, AnnualRevenue, Amount__c, Probability,"
    " SystemModstamp, CloseDate, StartTime__c, Description, Tags__c"
)
HEADER = (
    '"Id","Name","IsDeleted","NumberOfEmployees","AnnualRevenue","Amount__c","Probability",'
    '"SystemModstamp","CloseDate","StartTime__c","Description","Tags__c"\n'
)
# Bulk API 2.0 quotes all values, a null is a blank field, quoted or not. null bytes are dropped
ROW_1 = (
    '"001A","Acme","false","120","15000000","1234.56","50","2024-01-15T10:30:00.000Z",'
    '"2024-01-15","10:30:00.000Z","multi\nline, with ""quotes""","a;\x00b"\n'
)
ROW_2 = '"001B","NA","true",,,"","","2024-02-01T00:00:00.000Z",,,"N/A","NULL"\n'
ROW_3 = '"001C","Globex","false","5","","0.01","100","2024-01-20T08:00:00.000Z",,"",,\n'


def mock_bulk2(
    m: requests_mock.Mocker,
    pages: List[str],
    fields: Optional[List[Dict[str, Any]]] = None,
    job_states: Tuple[str, ...] = ("JobComplete",),
    failures: Optional[Dict[str, List[int]]] = None,
    sobject: str = "Account",
) -> Dict[str, List[Any]]:
    """Mocks `sobject` describe and a Bulk API 2.0 query job returning `pages`.

    `job_states` are returned by consecutive job status checks. `failures` maps a page locator
    to HTTP status codes returned before the page is served.

    Returns created job queries, result request params and their authorization headers,
    recorded as they are made.
    """
    calls: Dict[str, List[Any]] = {
        "operations": [],
        "queries": [],
        "results": [],
        "auth": [],
    }
    failures = failures or {}
    m.get(
        re.compile(rf"/sobjects/{sobject}/describe$"),
        json={"name": sobject, "fields": fields or DESCRIBE_FIELDS},
    )

    def create_job(request: Any, context: Any) -> Dict[str, Any]:
        calls["operations"].append(request.json()["operation"])
        calls["queries"].append(request.json()["query"])
        return {"id": JOB_ID, "state": "UploadComplete"}

    def get_results(request: Any, context: Any) -> str:
        # requests_mock lowercases `request.qs` but locators are case sensitive
        params = parse_qs(urlparse(request.url).query)
        calls["results"].append(params)
        calls["auth"].append(request.headers["Authorization"])
        locator = params.get("locator", [""])[0]
        if failures.get(locator):
            context.status_code = failures[locator].pop(0)
            if context.status_code == 401:
                return json.dumps(
                    [
                        {
                            "errorCode": "INVALID_SESSION_ID",
                            "message": "Session expired or invalid",
                        }
                    ]
                )
            context.headers = {"Retry-After": "0"}
            return ""
        page_idx = LOCATORS.index(locator)
        next_idx = page_idx + 1
        context.headers = {
            "Sforce-Locator": LOCATORS[next_idx] if next_idx < len(pages) else "null",
            "Sforce-NumberOfRecords": str(max(pages[page_idx].count('"\n') - 1, 0)),
        }
        return pages[page_idx]

    m.post(re.compile(r"/jobs/query$"), json=create_job)
    m.get(
        re.compile(rf"/jobs/query/{JOB_ID}$"),
        [
            {"json": {"id": JOB_ID, "state": state, "errorMessage": f"job {state}"}}
            for state in job_states
        ],
    )
    m.get(re.compile(rf"/jobs/query/{JOB_ID}/results"), text=get_results)
    return calls


def make_page(field_names: List[str], value: str) -> str:
    """Returns a CSV page with `field_names` header and a single row with all fields set to `value`"""
    header = ",".join(f'"{name}"' for name in field_names)
    row = ",".join(f'"{value}"' for _ in field_names)
    return f"{header}\n{row}\n"


@pytest.fixture
def sf_mock(mocker: MockerFixture) -> Iterator[requests_mock.Mocker]:
    # skip waits between job status checks
    mocker.patch("sources.salesforce.helpers.query.sleep")
    with requests_mock.Mocker() as m:
        yield m


@pytest.fixture
def credentials() -> InstanceAuth:
    # resolved credentials are not replaced with the ones from secrets.toml
    credentials = InstanceAuth(
        session_id="00Dsession", instance_url="https://test.my.salesforce.com"
    )
    credentials.resolve()
    return credentials


FIELD_TYPE_CASES: List[Tuple[Dict[str, Any], Optional[Dict[str, Any]]]] = [
    ({"type": "id", "nillable": False}, {"data_type": "text", "nullable": False}),
    ({"type": "reference"}, {"data_type": "text"}),
    ({"type": "picklist"}, {"data_type": "text"}),
    ({"type": "multipicklist"}, {"data_type": "text"}),
    ({"type": "textarea"}, {"data_type": "text"}),
    ({"type": "anyType"}, {"data_type": "text"}),
    ({"type": "boolean"}, {"data_type": "bool"}),
    ({"type": "int"}, {"data_type": "bigint"}),
    ({"type": "long"}, {"data_type": "bigint"}),
    ({"type": "double", "precision": 18, "scale": 2}, {"data_type": "double"}),
    ({"type": "percent", "precision": 3, "scale": 0}, {"data_type": "double"}),
    (
        {"type": "currency", "precision": 16, "scale": 2},
        {"data_type": "decimal", "precision": 16, "scale": 2},
    ),
    ({"type": "date"}, {"data_type": "date"}),
    (
        {"type": "datetime"},
        {"data_type": "timestamp", "precision": 3, "timezone": True},
    ),
    ({"type": "time"}, {"data_type": "time", "precision": 3}),
    ({"type": "json"}, {"data_type": "json"}),
    # unknown types are loaded as text
    ({"type": "new_salesforce_type"}, {"data_type": "text"}),
    # compound and base64 fields are not queried, compounds come as their individual fields
    ({"type": "address"}, None),
    ({"type": "location"}, None),
    ({"type": "base64"}, None),
]


@pytest.mark.parametrize(
    "field,expected",
    FIELD_TYPE_CASES,
    ids=[field["type"] for field, _ in FIELD_TYPE_CASES],
)
def test_sf_field_to_column_schema(
    field: Dict[str, Any], expected: Optional[Dict[str, Any]]
) -> None:
    column = sf_field_to_column_schema({"name": "Field__c", "nillable": True, **field})
    if expected is None:
        assert column is None
    else:
        assert column == {"name": "Field__c", "nullable": True, **expected}


@pytest.mark.parametrize(
    "pages,expected_rows",
    [
        ([HEADER + ROW_1, HEADER + ROW_2, HEADER + ROW_3], [1, 1, 1]),
        ([HEADER + ROW_1 + ROW_2, HEADER + ROW_3], [2, 1]),
        ([""], []),
        # Salesforce returns just the header when a job has no results
        ([HEADER], []),
        ([HEADER + ROW_1, HEADER], [1]),
    ],
    ids=[
        "three_pages",
        "two_pages",
        "empty_body",
        "header_only",
        "header_only_last_page",
    ],
)
def test_salesforce_table_pagination(
    sf_mock: requests_mock.Mocker,
    credentials: InstanceAuth,
    pages: List[str],
    expected_rows: List[int],
) -> None:
    calls = mock_bulk2(sf_mock, pages)
    items = list(
        salesforce_table(credentials=credentials, sobject="Account", chunk_size=2)
    )

    # each result page is requested with the locator of the previous one
    assert calls["results"] == [
        {"maxRecords": ["2"], **({"locator": [locator]} if locator else {})}
        for locator in LOCATORS[: len(pages)]
    ]
    assert all(isinstance(item, pa.Table) for item in items)
    assert [item.num_rows for item in items] == expected_rows
    if expected_rows:
        # ROW_1 is on every page with records, its null byte is dropped
        tags = [value for item in items for value in item.column("Tags__c").to_pylist()]
        assert "a;b" in tags


@pytest.mark.parametrize(
    "cursor_path,incremental_kwargs,expected_soql_filter",
    [
        ("SystemModstamp", {}, ""),
        (
            "SystemModstamp",
            {"initial_value": datetime(2024, 1, 1, tzinfo=timezone.utc)},
            " WHERE SystemModstamp >= 2024-01-01T00:00:00.000Z",
        ),
        (
            "SystemModstamp",
            {"initial_value": "2024-01-01T02:00:00.123+02:00", "range_start": "open"},
            " WHERE SystemModstamp > 2024-01-01T00:00:00.123Z",
        ),
        (
            "SystemModstamp",
            {
                "initial_value": datetime(2024, 1, 1, tzinfo=timezone.utc),
                "end_value": datetime(2024, 3, 1, tzinfo=timezone.utc),
            },
            " WHERE SystemModstamp >= 2024-01-01T00:00:00.000Z"
            " AND SystemModstamp < 2024-03-01T00:00:00.000Z",
        ),
        (
            "SystemModstamp",
            {
                "initial_value": datetime(2024, 3, 1, tzinfo=timezone.utc),
                "end_value": datetime(2024, 1, 1, tzinfo=timezone.utc),
                "last_value_func": min,
                "range_end": "closed",
            },
            " WHERE SystemModstamp <= 2024-03-01T00:00:00.000Z"
            " AND SystemModstamp >= 2024-01-01T00:00:00.000Z",
        ),
        (
            "SystemModstamp",
            {
                "initial_value": datetime(2024, 1, 1, tzinfo=timezone.utc),
                "row_order": "asc",
            },
            " WHERE SystemModstamp >= 2024-01-01T00:00:00.000Z ORDER BY SystemModstamp ASC",
        ),
        (
            "SystemModstamp",
            {"row_order": "desc"},
            " ORDER BY SystemModstamp DESC",
        ),
        (
            "CloseDate",
            {"initial_value": date(2024, 1, 1), "on_cursor_value_missing": "include"},
            " WHERE (CloseDate >= 2024-01-01) OR CloseDate = null",
        ),
        (
            "CloseDate",
            {"initial_value": date(2024, 1, 1), "on_cursor_value_missing": "exclude"},
            " WHERE (CloseDate >= 2024-01-01) AND (CloseDate != null)",
        ),
        (
            "NumberOfEmployees",
            {"initial_value": 100},
            " WHERE NumberOfEmployees >= 100",
        ),
        (
            "Name",
            {"initial_value": "O'Brien \\ Sons"},
            " WHERE Name >= 'O\\'Brien \\\\ Sons'",
        ),
        # a custom last value function cannot be translated to SOQL, dlt filters the rows
        (
            "SystemModstamp",
            {
                "initial_value": datetime(2024, 1, 1, tzinfo=timezone.utc),
                "last_value_func": lambda values: max(values),
            },
            "",
        ),
    ],
    ids=[
        "no_initial_value",
        "initial_value",
        "open_range_start_tz_offset",
        "end_value",
        "min_closed_range_end",
        "row_order_asc",
        "row_order_desc_no_initial_value",
        "date_include_missing",
        "date_exclude_missing",
        "bigint",
        "text_escaped",
        "custom_last_value_func",
    ],
)
def test_salesforce_table_incremental_soql(
    sf_mock: requests_mock.Mocker,
    credentials: InstanceAuth,
    cursor_path: str,
    incremental_kwargs: Dict[str, Any],
    expected_soql_filter: str,
) -> None:
    # the incremental range becomes the WHERE clause and row_order the ORDER BY of the query
    calls = mock_bulk2(sf_mock, [""])
    resource = salesforce_table(
        credentials=credentials,
        sobject="Account",
        incremental=dlt.sources.incremental(cursor_path, **incremental_kwargs),
    )
    assert list(resource) == []
    assert calls["queries"] == [
        f"SELECT {QUERIED_FIELDS} FROM Account{expected_soql_filter}"
    ]


def test_salesforce_table_incremental_missing_cursor(
    sf_mock: requests_mock.Mocker, credentials: InstanceAuth
) -> None:
    # the cursor is not a field of the sObject, so its type for the SOQL literal is unknown
    mock_bulk2(sf_mock, [""])
    resource = salesforce_table(
        credentials=credentials,
        sobject="Account",
        incremental=dlt.sources.incremental("LastViewedDate"),
    )
    with pytest.raises(ResourceExtractionError) as py_ex:
        list(resource)
    assert isinstance(py_ex.value.__cause__, KeyError)


def test_salesforce_table_incremental_runs(
    sf_mock: requests_mock.Mocker, credentials: InstanceAuth
) -> None:
    calls = mock_bulk2(sf_mock, [HEADER + ROW_1 + ROW_2 + ROW_3])
    pipeline = dlt.pipeline(
        pipeline_name="salesforce_table_incremental",
        destination="duckdb",
        dev_mode=True,
    )

    def account() -> Any:
        return salesforce_table(
            credentials=credentials,
            sobject="Account",
            incremental=dlt.sources.incremental("SystemModstamp"),
        )

    # an incremental on SystemModstamp merges, 001B is deleted in Salesforce and not loaded
    assert_load_info(pipeline.run(account()))
    assert load_table_counts(pipeline, "account") == {"account": 2}
    # the next job starts from the max cursor value. the mock ignores WHERE and returns all
    # records again, they are merged on Id
    pipeline.run(account())
    assert calls["queries"][1].endswith(
        " WHERE SystemModstamp >= 2024-02-01T00:00:00.000Z"
    )
    assert load_table_counts(pipeline, "account") == {"account": 2}


def test_salesforce_table_columns(
    sf_mock: requests_mock.Mocker, credentials: InstanceAuth
) -> None:
    # Description is both included and excluded, excluded wins
    page = '"Id","Amount__c","SystemModstamp"\n"001A","1234.56","2024-01-15T10:30:00.000Z"\n'
    calls = mock_bulk2(sf_mock, [page])
    resource = salesforce_table(
        credentials=credentials,
        sobject="Account",
        included_columns=["Id", "Amount__c", "SystemModstamp", "Description"],
        excluded_columns=["Description"],
    )
    assert list(resource.compute_table_schema()["columns"]) == [
        "Id",
        "Amount__c",
        "SystemModstamp",
    ]

    # user hints override types reflected from describe
    resource.apply_hints(columns={"Amount__c": {"data_type": "double"}})
    tables = list(resource)
    assert calls["queries"] == ["SELECT Id, Amount__c, SystemModstamp FROM Account"]
    assert tables[0].schema.field("Amount__c").type == pa.float64()
    assert tables[0].schema.field("SystemModstamp").type == pa.timestamp("ms", tz="UTC")
    assert tables[0].column("Amount__c").to_pylist() == [1234.56]


FIELD_TYPES = {
    "Id": "id",
    "Name": "string",
    "Field": "string",
    "CreatedDate": "datetime",
    "LastModifiedDate": "datetime",
    "SystemModstamp": "datetime",
    "LoginTime": "datetime",
}


@pytest.mark.parametrize(
    "field_names,kwargs,explicit_cursor,expected_disposition,expected_pk,expected_filter",
    [
        (
            ["Id", "CreatedDate", "LastModifiedDate", "SystemModstamp"],
            {},
            None,
            "merge",
            ["Id"],
            " WHERE SystemModstamp >= 2024-01-31T23:50:00.000Z",
        ),
        (
            ["Id", "CreatedDate", "LastModifiedDate"],
            {},
            None,
            "merge",
            ["Id"],
            " WHERE LastModifiedDate >= 2024-01-31T23:50:00.000Z",
        ),
        (
            ["Id", "CreatedDate", "Field"],
            {},
            None,
            "append",
            ["Id"],
            " WHERE CreatedDate >= 2024-02-01T00:00:00.000Z",
        ),
        (
            ["Id", "LoginTime"],
            {},
            None,
            "append",
            ["Id"],
            " WHERE LoginTime >= 2024-02-01T00:00:00.000Z",
        ),
        (["Id", "Name"], {}, None, "replace", ["Id"], ""),
        (
            ["Id", "SystemModstamp"],
            {"lag": 60},
            None,
            "merge",
            ["Id"],
            " WHERE SystemModstamp >= 2024-01-31T23:59:00.000Z",
        ),
        (
            ["Id", "SystemModstamp"],
            {"write_disposition": "append"},
            None,
            "append",
            ["Id"],
            " WHERE SystemModstamp >= 2024-02-01T00:00:00.000Z",
        ),
        (["Id", "SystemModstamp"], {"incremental": None}, None, "replace", ["Id"], ""),
        (
            ["Id", "SystemModstamp"],
            {"write_disposition": "replace"},
            None,
            "replace",
            ["Id"],
            "",
        ),
        (
            ["Id", "Name", "SystemModstamp"],
            {"primary_key": "Name"},
            None,
            "merge",
            ["Name"],
            " WHERE SystemModstamp >= 2024-01-31T23:50:00.000Z",
        ),
        (
            ["Id", "SystemModstamp"],
            {},
            "SystemModstamp",
            "merge",
            ["Id"],
            " WHERE SystemModstamp >= 2024-02-01T00:00:00.000Z",
        ),
        (
            ["Id", "CreatedDate"],
            {},
            "CreatedDate",
            "append",
            ["Id"],
            " WHERE CreatedDate >= 2024-02-01T00:00:00.000Z",
        ),
    ],
    ids=[
        "system_modstamp",
        "last_modified_date",
        "created_date_history",
        "login_time",
        "no_cursor",
        "explicit_lag",
        "explicit_append_no_lag",
        "no_incremental",
        "explicit_replace_no_incremental",
        "explicit_primary_key",
        "explicit_incremental_merge",
        "explicit_incremental_append",
    ],
)
def test_salesforce_table_defaults(
    sf_mock: requests_mock.Mocker,
    credentials: InstanceAuth,
    field_names: List[str],
    kwargs: Dict[str, Any],
    explicit_cursor: Optional[str],
    expected_disposition: str,
    expected_pk: List[str],
    expected_filter: str,
) -> None:
    # the sObject has `field_names` and a single record with all of them set to 2024-02-01.
    # the WHERE of the second run shows which cursor was picked and its lag: 10 minutes when
    # "auto" merges, none on append or with an explicit incremental
    fields = [
        {"name": name, "type": FIELD_TYPES[name], "nillable": name != "Id"}
        for name in field_names
    ]
    calls = mock_bulk2(
        sf_mock, [make_page(field_names, "2024-02-01T00:00:00.000Z")], fields=fields
    )
    pipeline = dlt.pipeline(pipeline_name="salesforce_table_defaults", dev_mode=True)

    for _ in range(2):
        if explicit_cursor:
            kwargs["incremental"] = dlt.sources.incremental(explicit_cursor)
        resource = salesforce_table(
            credentials=credentials, sobject="Account", **kwargs
        )
        pipeline.extract(resource)

    assert resource.write_disposition == expected_disposition
    columns = resource.compute_table_schema()["columns"]
    assert [
        name for name, col in columns.items() if col.get("primary_key")
    ] == expected_pk
    assert (
        calls["queries"][1]
        == f"SELECT {', '.join(field_names)} FROM Account{expected_filter}"
    )


def test_salesforce_table_session_expired(sf_mock: requests_mock.Mocker) -> None:
    # the second result page fails with 401, client credentials can log in again
    token = sf_mock.post(
        "https://test.my.salesforce.com/services/oauth2/token",
        [
            {
                "json": {
                    "access_token": f"token{n}",
                    "instance_url": "https://test.my.salesforce.com",
                }
            }
            for n in (1, 2)
        ],
    )
    calls = mock_bulk2(
        sf_mock,
        [HEADER + ROW_1, HEADER + ROW_2, HEADER + ROW_3],
        failures={"MTAw": [401]},
    )
    credentials = ConsumerKeySecretDomainAuth(
        consumer_key="key", consumer_secret="secret", domain="test.my"
    )
    credentials.resolve()

    items = list(salesforce_table(credentials=credentials, sobject="Account"))

    # logged in again and continued the same job from the page that failed
    assert token.call_count == 2
    assert [params.get("locator") for params in calls["results"]] == [
        None,
        ["MTAw"],
        ["MTAw"],
        ["MjAw"],
    ]
    assert calls["auth"] == [
        "Bearer token1",
        "Bearer token1",
        "Bearer token2",
        "Bearer token2",
    ]
    assert [item.num_rows for item in items] == [1, 1, 1]


def test_salesforce_table_session_expired_no_login(
    sf_mock: requests_mock.Mocker, credentials: InstanceAuth
) -> None:
    # a session id cannot be renewed, the error is raised after a single retry
    calls = mock_bulk2(sf_mock, [HEADER + ROW_1], failures={"": [401, 401]})
    with pytest.raises(ResourceExtractionError) as py_ex:
        list(salesforce_table(credentials=credentials, sobject="Account"))
    assert isinstance(py_ex.value.__cause__, SalesforceExpiredSession)
    assert len(calls["results"]) == 2


def test_salesforce_table_retries_throttling(
    sf_mock: requests_mock.Mocker, credentials: InstanceAuth
) -> None:
    # the second result page fails with 429 then 503, the dlt session retries both
    calls = mock_bulk2(
        sf_mock, [HEADER + ROW_1, HEADER + ROW_2], failures={"MTAw": [429, 503]}
    )
    items = list(salesforce_table(credentials=credentials, sobject="Account"))
    assert [params.get("locator") for params in calls["results"]] == [
        None,
        ["MTAw"],
        ["MTAw"],
        ["MTAw"],
    ]
    assert [item.num_rows for item in items] == [1, 1]


@pytest.mark.parametrize(
    "job_states,expected_sleeps,expected_error",
    [
        (("UploadComplete", "InProgress", "JobComplete"), [0.5, 0.75], None),
        (("InProgress", "Failed"), [0.5], "job Failed"),
        (("Aborted",), [], "job Aborted"),
    ],
    ids=["complete", "failed", "aborted"],
)
def test_salesforce_table_job_status(
    sf_mock: requests_mock.Mocker,
    credentials: InstanceAuth,
    mocker: MockerFixture,
    job_states: Tuple[str, ...],
    expected_sleeps: List[float],
    expected_error: Optional[str],
) -> None:
    # the job status is polled with growing waits until the job completes, fails or is aborted
    sleep = mocker.patch("sources.salesforce.helpers.query.sleep")
    mock_bulk2(sf_mock, [HEADER + ROW_1], job_states=job_states)
    resource = salesforce_table(credentials=credentials, sobject="Account")
    if expected_error:
        with pytest.raises(ResourceExtractionError) as py_ex:
            list(resource)
        assert isinstance(py_ex.value.__cause__, SalesforceOperationError)
        assert expected_error in str(py_ex.value.__cause__)
    else:
        assert [item.num_rows for item in resource] == [1]
    assert [call.args[0] for call in sleep.call_args_list] == expected_sleeps


@pytest.mark.parametrize(
    "kwargs,expected_soql_filter",
    [
        ({"incremental": None}, " WHERE Name LIKE 'A%' AND IsDeleted = false"),
        (
            {
                "incremental": dlt.sources.incremental(
                    "SystemModstamp", initial_value="2024-01-01"
                )
            },
            " WHERE (Name LIKE 'A%' AND IsDeleted = false)"
            " AND (SystemModstamp >= 2024-01-01T00:00:00.000Z)",
        ),
        (
            {
                "incremental": dlt.sources.incremental(
                    "CloseDate",
                    initial_value=date(2024, 1, 1),
                    on_cursor_value_missing="include",
                )
            },
            " WHERE (Name LIKE 'A%' AND IsDeleted = false)"
            " AND ((CloseDate >= 2024-01-01) OR CloseDate = null)",
        ),
        (
            {
                "incremental": dlt.sources.incremental(
                    "SystemModstamp",
                    initial_value="2024-01-01",
                    last_value_func=lambda values: max(values),
                )
            },
            " WHERE Name LIKE 'A%' AND IsDeleted = false",
        ),
    ],
    ids=[
        "filter_only",
        "filter_and_incremental",
        "filter_and_or_condition",
        "custom_last_value_func",
    ],
)
def test_salesforce_table_soql_filter(
    sf_mock: requests_mock.Mocker,
    credentials: InstanceAuth,
    kwargs: Dict[str, Any],
    expected_soql_filter: str,
) -> None:
    # conditions are parenthesized when the filter is combined with the incremental range
    calls = mock_bulk2(sf_mock, [""])
    resource = salesforce_table(
        credentials=credentials,
        sobject="Account",
        soql_filter="Name LIKE 'A%' AND IsDeleted = false",
        **kwargs,
    )
    assert list(resource) == []
    assert calls["queries"] == [
        f"SELECT {QUERIED_FIELDS} FROM Account{expected_soql_filter}"
    ]


@pytest.mark.parametrize(
    "include_deleted,expected_operation,expected_ids",
    [(True, "queryAll", ["001C"]), (False, "query", ["001A", "001C"])],
    ids=["include_deleted", "exclude_deleted"],
)
@pytest.mark.parametrize("defer_table_reflect", [False, True], ids=["", "deferred"])
def test_salesforce_table_deleted_records(
    sf_mock: requests_mock.Mocker,
    credentials: InstanceAuth,
    include_deleted: bool,
    expected_operation: str,
    expected_ids: List[str],
    defer_table_reflect: bool,
) -> None:
    pipeline = dlt.pipeline(
        pipeline_name="salesforce_table_deleted", destination="duckdb", dev_mode=True
    )

    def account() -> Any:
        return salesforce_table(
            credentials=credentials,
            sobject="Account",
            include_deleted=include_deleted,
            defer_table_reflect=defer_table_reflect,
        )

    # include_deleted is about the query, not the destination: queryAll sees the delete and
    # the hard delete removes the record, query never returns it and the loaded copy stays
    calls = mock_bulk2(sf_mock, [HEADER + ROW_1 + ROW_3])
    assert_load_info(pipeline.run(account()))
    assert load_table_counts(pipeline, "account") == {"account": 2}

    # 001A is deleted in Salesforce, queryAll returns it with IsDeleted set, query skips it
    deleted_row = ROW_1.replace('"false"', '"true"').replace(
        "2024-01-15T10:30:00.000Z", "2024-02-01T00:00:00.000Z"
    )
    calls = mock_bulk2(sf_mock, [HEADER + deleted_row if include_deleted else HEADER])
    pipeline.run(account())

    assert calls["operations"] == [expected_operation]
    is_deleted = pipeline.default_schema.get_table_columns("account")["is_deleted"]
    assert is_deleted.get("hard_delete", False) is include_deleted
    rows = load_tables_to_dicts(pipeline, "account")["account"]
    assert sorted(row["id"] for row in rows) == expected_ids
    assert not any(row["is_deleted"] for row in rows)


@pytest.mark.parametrize(
    "field,expected",
    [
        (
            {"type": "double", "precision": 18, "scale": 0},
            {"data_type": "decimal", "precision": 18, "scale": 0},
        ),
        (
            {"type": "percent", "precision": 5, "scale": 2},
            {"data_type": "decimal", "precision": 5, "scale": 2},
        ),
        # fields without declared precision stay floats
        ({"type": "double", "precision": 0, "scale": 0}, {"data_type": "double"}),
        ({"type": "int"}, {"data_type": "bigint"}),
    ],
    ids=["double", "percent", "double_no_precision", "int"],
)
def test_sf_field_to_column_schema_double_as_decimal(
    field: Dict[str, Any], expected: Dict[str, Any]
) -> None:
    column = sf_field_to_column_schema(
        {"name": "Field__c", "nillable": True, **field}, double_as_decimal=True
    )
    assert column == {"name": "Field__c", "nullable": True, **expected}


@pytest.mark.parametrize("backend", ["python", "pyarrow"])
@pytest.mark.parametrize("double_as_decimal", [False, True], ids=["double", "decimal"])
def test_salesforce_table_captured(
    sf_mock: requests_mock.Mocker,
    credentials: InstanceAuth,
    double_as_decimal: bool,
    backend: TTableBackend,
) -> None:
    # describe and result page captured from the test org by tests/salesforce/seed/seed.py
    mock_bulk2(
        sf_mock,
        [load_captured_csv()],
        fields=load_captured_describe(),
        sobject=data.DLT_TYPES,
    )
    pipeline = dlt.pipeline(
        pipeline_name="salesforce_table_captured", destination="duckdb", dev_mode=True
    )
    load_info = pipeline.run(
        salesforce_table(
            credentials=credentials,
            sobject=data.DLT_TYPES,
            double_as_decimal=double_as_decimal,
            backend=backend,
        )
    )
    assert_load_info(load_info)
    assert_dlt_types_loaded(pipeline, double_as_decimal)


@pytest.mark.parametrize(
    "renewed_by_other_thread", [False, True], ids=["login", "reuse"]
)
def test_bulk2_query_session_refresh(
    sf_mock: requests_mock.Mocker, renewed_by_other_thread: bool
) -> None:
    # the job status request is sent with an expired session and fails with 401
    expired = InstanceAuth(
        session_id="expired", instance_url="https://test.my.salesforce.com"
    )
    expired.resolve()
    renewed = InstanceAuth(
        session_id="renewed", instance_url="https://test.my.salesforce.com"
    )
    renewed.resolve()
    logins: List[str] = []

    def login() -> Any:
        logins.append("login")
        return make_salesforce_client(renewed)

    bulk2_query = QueryClient(make_salesforce_client(expired), login)

    def job_status(request: Any, context: Any) -> Any:
        if request.headers["Authorization"] == "Bearer expired":
            if renewed_by_other_thread:
                # another thread sharing the instance logged in after this request was sent
                bulk2_query.client = make_salesforce_client(renewed)
            context.status_code = 401
            return [{"errorCode": "INVALID_SESSION_ID", "message": "Session expired"}]
        return {"id": JOB_ID, "state": "JobComplete"}

    sf_mock.get(re.compile(rf"/jobs/query/{JOB_ID}$"), json=job_status)
    bulk2_query._wait_for_job(JOB_ID)

    # the session is renewed once, a thread that sees a newer session retries with it
    assert logins == ([] if renewed_by_other_thread else ["login"])
    assert bulk2_query.client.session_id == "renewed"


def rest_records_from_captured() -> List[Dict[str, Any]]:
    """Returns the captured `DltTypes__c` rows typed as the REST API returns them"""
    types = {field["name"]: field["type"] for field in load_captured_describe()}
    records = []
    for row in csv.DictReader(io.StringIO(load_captured_csv(), newline="")):
        record: Dict[str, Any] = {"attributes": {"type": data.DLT_TYPES}}
        for name, value in row.items():
            if value == "":
                record[name] = None
            elif types[name] == "boolean":
                record[name] = value == "true"
            elif types[name] in ("double", "percent", "currency"):
                record[name] = float(value)
            elif types[name] == "datetime":
                # e.g. `2024-02-29T23:59:59.000+0000`
                record[name] = value[:-1] + "+0000"
            else:
                record[name] = value
        records.append(record)
    return records


def mock_rest(
    m: requests_mock.Mocker, records: List[Dict[str, Any]], page_size: int
) -> Dict[str, List[Any]]:
    """Mocks REST API query results paged by `nextRecordsUrl`, records requests"""
    calls: Dict[str, List[Any]] = {"paths": [], "queries": [], "next": []}
    pages = [records[n : n + page_size] for n in range(0, len(records), page_size)] or [
        []
    ]

    def page(n: int) -> Dict[str, Any]:
        result: Dict[str, Any] = {
            "totalSize": len(records),
            "done": n == len(pages) - 1,
            "records": pages[n],
        }
        if not result["done"]:
            result["nextRecordsUrl"] = f"/services/data/v59.0/query/01gNEXT-{n + 1}"
        return result

    def first_page(request: Any, context: Any) -> Dict[str, Any]:
        calls["paths"].append(urlparse(request.url).path.rsplit("/", 1)[-1])
        calls["queries"].append(parse_qs(urlparse(request.url).query)["q"][0])
        return page(0)

    def next_page(request: Any, context: Any) -> Dict[str, Any]:
        n = int(request.url.rsplit("-", 1)[1])
        calls["next"].append(n)
        return page(n)

    m.get(re.compile(r"/services/data/v[\d.]+/(query|queryAll)\?q="), json=first_page)
    m.get(re.compile(r"/query/01gNEXT-\d+$"), json=next_page)
    return calls


def mock_bulk_not_supported(m: requests_mock.Mocker) -> None:
    m.post(
        re.compile(r"/jobs/query$"),
        status_code=400,
        json=[
            {
                "errorCode": "INVALIDENTITY",
                "message": f"Entity '{data.DLT_TYPES}' is not supported by the Bulk API.",
            }
        ],
    )


@pytest.mark.parametrize("backend", ["python", "pyarrow"])
@pytest.mark.parametrize("api", ["auto", "rest"])
def test_salesforce_table_rest_captured(
    sf_mock: requests_mock.Mocker,
    credentials: InstanceAuth,
    api: TQueryApi,
    backend: TTableBackend,
) -> None:
    # REST API records typed like the captured Bulk API page load to the same values. "auto"
    # falls back to REST when Bulk rejects the job, "rest" does not create a job at all
    calls = mock_bulk2(
        sf_mock, [], fields=load_captured_describe(), sobject=data.DLT_TYPES
    )
    if api == "auto":
        mock_bulk_not_supported(sf_mock)
    rest_calls = mock_rest(sf_mock, rest_records_from_captured(), page_size=4)
    pipeline = dlt.pipeline(
        pipeline_name="salesforce_table_rest", destination="duckdb", dev_mode=True
    )
    load_info = pipeline.run(
        salesforce_table(
            credentials=credentials, sobject=data.DLT_TYPES, api=api, backend=backend
        )
    )
    assert_load_info(load_info)
    assert_dlt_types_loaded(pipeline, double_as_decimal=False)
    # queryAll returns deleted records, 10 records in pages of 4
    assert rest_calls["paths"] == ["queryAll"]
    assert rest_calls["next"] == [1, 2]
    assert calls["queries"] == []


def test_salesforce_table_bulk_api_not_supported(
    sf_mock: requests_mock.Mocker, credentials: InstanceAuth
) -> None:
    # "bulk" does not fall back to REST
    mock_bulk2(sf_mock, [], fields=load_captured_describe(), sobject=data.DLT_TYPES)
    mock_bulk_not_supported(sf_mock)
    resource = salesforce_table(
        credentials=credentials, sobject=data.DLT_TYPES, api="bulk"
    )
    with pytest.raises(ResourceExtractionError) as py_ex:
        list(resource)
    assert isinstance(py_ex.value.__cause__, SalesforceMalformedRequest)


def test_salesforce_table_rest_query_too_long(
    sf_mock: requests_mock.Mocker, credentials: InstanceAuth, mocker: MockerFixture
) -> None:
    # REST sends the query in the URL, a query over the limit asks for fewer columns
    mocker.patch("sources.salesforce.helpers.query.MAX_REST_URL_LENGTH", 200)
    mock_bulk2(sf_mock, [], fields=load_captured_describe(), sobject=data.DLT_TYPES)
    resource = salesforce_table(
        credentials=credentials, sobject=data.DLT_TYPES, api="rest"
    )
    with pytest.raises(ResourceExtractionError) as py_ex:
        list(resource)
    assert "included_columns" in str(py_ex.value.__cause__)


def test_salesforce_table_without_pyarrow(
    sf_mock: requests_mock.Mocker, credentials: InstanceAuth, mocker: MockerFixture
) -> None:
    # "auto" picks the python backend, which must not import pyarrow
    mocker.patch.dict(sys.modules, {"pyarrow": None, "dlt.common.libs.pyarrow": None})
    mock_bulk2(
        sf_mock,
        [load_captured_csv()],
        fields=load_captured_describe(),
        sobject=data.DLT_TYPES,
    )
    items = list(salesforce_table(credentials=credentials, sobject=data.DLT_TYPES))
    assert all(isinstance(item, dict) for item in items)
    assert {item["Seed_Id__c"] for item in items} >= {
        record["Seed_Id__c"] for record in data.DLT_TYPES_RECORDS
    }


SOURCE_FIELDS: List[Dict[str, Any]] = [
    {"name": "Id", "type": "id", "nillable": False},
    {"name": "Name", "type": "string", "nillable": True},
    {"name": "IsDeleted", "type": "boolean", "nillable": False},
    {"name": "SystemModstamp", "type": "datetime", "nillable": False},
]


@pytest.mark.parametrize("backend", ["python", "pyarrow"])
def test_salesforce_source(
    sf_mock: requests_mock.Mocker, backend: TTableBackend
) -> None:
    token = sf_mock.post(
        "https://test.my.salesforce.com/services/oauth2/token",
        json={
            "access_token": "token",
            "instance_url": "https://test.my.salesforce.com",
        },
    )
    page = (
        '"Id","Name","IsDeleted","SystemModstamp"\n'
        '"001A","Acme","false","2024-01-15T10:30:00.000Z"\n'
    )
    # every sObject has the same fields and records
    calls = mock_bulk2(sf_mock, [page], fields=SOURCE_FIELDS, sobject=r"\w+")
    credentials = ConsumerKeySecretDomainAuth(
        consumer_key="key", consumer_secret="secret", domain="test.my"
    )
    credentials.resolve()

    source = salesforce_source(
        credentials=credentials,
        soql_filters={"opportunity": "IsClosed = false"},
        backend=backend,
    )
    assert list(source.resources) == list(SOURCE_SOBJECTS)
    assert token.call_count == 1
    for resource in source.resources.values():
        assert resource.write_disposition == "merge"
        assert resource.compute_table_schema()["columns"]["Id"]["primary_key"] is True

    pipeline = dlt.pipeline(
        pipeline_name="salesforce_source", destination="duckdb", dev_mode=True
    )
    assert_load_info(pipeline.run(source))
    assert load_table_counts(pipeline, *SOURCE_SOBJECTS) == {
        name: 1 for name in SOURCE_SOBJECTS
    }
    # the filter is added to the opportunity query only
    filtered = [query for query in calls["queries"] if "IsClosed = false" in query]
    assert len(filtered) == 1 and filtered[0].startswith(
        "SELECT Id, Name, IsDeleted, SystemModstamp FROM Opportunity"
    )
    assert token.call_count == 1


def test_salesforce_source_replace_without_pyarrow(
    sf_mock: requests_mock.Mocker, credentials: InstanceAuth, mocker: MockerFixture
) -> None:
    mocker.patch.dict(sys.modules, {"pyarrow": None, "dlt.common.libs.pyarrow": None})
    page = (
        '"Id","Name","IsDeleted","SystemModstamp"\n'
        '"001A","Acme","false","2024-01-15T10:30:00.000Z"\n'
    )
    calls = mock_bulk2(sf_mock, [page], fields=SOURCE_FIELDS, sobject=r"\w+")
    source = salesforce_source(
        credentials=credentials, write_disposition="replace"
    ).with_resources("account", "task")
    pipeline = dlt.pipeline(pipeline_name="salesforce_source_replace", dev_mode=True)
    for _ in range(2):
        pipeline.extract(source)
    # replace loads all records on every run, resources run in parallel
    assert sorted(calls["queries"]) == sorted(
        f"SELECT Id, Name, IsDeleted, SystemModstamp FROM {sobject}"
        for sobject in ("Account", "Task") * 2
    )


def test_salesforce_source_unknown_filter(credentials: InstanceAuth) -> None:
    # filters are keyed by resource name, a typo would silently load unfiltered data
    with pytest.raises(ValueError):
        salesforce_source(
            credentials=credentials, soql_filters={"accounts": "Name = 'x'"}
        )


class FakeBulk2Org:
    """Answers Bulk API 2.0 query jobs on `Account` by filtering, ordering and limiting `records`"""

    def __init__(
        self,
        m: requests_mock.Mocker,
        fields: List[Dict[str, Any]],
        records: List[Dict[str, str]],
    ) -> None:
        self.records = records
        self.queries: List[str] = []
        self._page = ""
        m.get(
            re.compile(r"/sobjects/Account/describe$"),
            json={"name": "Account", "fields": fields},
        )
        m.post(re.compile(r"/jobs/query$"), json=self.create_job)
        m.get(
            re.compile(rf"/jobs/query/{JOB_ID}$"),
            json={"id": JOB_ID, "state": "JobComplete"},
        )
        m.get(re.compile(rf"/jobs/query/{JOB_ID}/results"), text=self.results)

    def create_job(self, request: Any, context: Any) -> Dict[str, Any]:
        soql = request.json()["query"]
        self.queries.append(soql)
        names = soql[len("SELECT ") : soql.index(" FROM ")].split(", ")
        rows = self.records
        # timestamps in records have the format of SOQL literals so they compare as strings
        where = re.search(r" WHERE (\w+) (>=|>) (\S+)", soql)
        if where:
            field, op, literal = where.groups()
            rows = [
                r
                for r in rows
                if r[field] >= literal
                if op == ">=" or r[field] > literal
            ]
        order_by = re.search(r" ORDER BY (\w+) ASC", soql)
        if order_by:
            rows = sorted(rows, key=lambda r: r[order_by.group(1)])
        limit = re.search(r" LIMIT (\d+)$", soql)
        if limit:
            rows = rows[: int(limit.group(1))]
        lines = [",".join(f'"{name}"' for name in names)]
        lines += [",".join(f'"{row[name]}"' for name in names) for row in rows]
        self._page = "\n".join(lines) + "\n"
        return {"id": JOB_ID}

    def results(self, request: Any, context: Any) -> str:
        context.headers = {"Sforce-Locator": "null", "Sforce-NumberOfRecords": "0"}
        return self._page


def account_records(stamps: List[str]) -> List[Dict[str, str]]:
    return [
        {
            "Id": f"001{n:03d}",
            "Name": f"account {n}",
            "IsDeleted": "false",
            "SystemModstamp": stamp,
        }
        for n, stamp in enumerate(stamps)
    ]


def incremental_state(pipeline: dlt.Pipeline, cursor: str) -> Dict[str, Any]:
    source_state = next(iter(pipeline.state["sources"].values()))
    return source_state["resources"]["Account"]["incremental"][cursor]  # type: ignore[no-any-return]


@pytest.mark.parametrize("backend", ["python", "pyarrow"])
@pytest.mark.parametrize(
    "defer_table_reflect",
    [
        False,
        pytest.param(
            True,
            id="deferred",
            # TODO: remove when dlt does not count empty items emitted with hints
            marks=pytest.mark.xfail(
                reason="add_limit counts the empty item with hints", strict=True
            ),
        ),
    ],
    ids=["", "deferred"],
)
def test_salesforce_table_chunked_loading(
    sf_mock: requests_mock.Mocker,
    credentials: InstanceAuth,
    backend: TTableBackend,
    defer_table_reflect: bool,
) -> None:
    # 7 records with distinct timestamps and 3 sharing one, fewer than the limit
    stamps = [f"2024-01-01T00:00:0{n}.000Z" for n in range(1, 8)] + [
        "2024-01-01T00:00:09.000Z"
    ] * 3
    org = FakeBulk2Org(sf_mock, SOURCE_FIELDS, account_records(stamps))
    pipeline = dlt.pipeline(
        pipeline_name="salesforce_chunked", destination="duckdb", dev_mode=True
    )
    account = salesforce_table(
        credentials=credentials,
        sobject="Account",
        chunk_size=4,
        backend=backend,
        incremental=dlt.sources.incremental("SystemModstamp", row_order="asc"),
        defer_table_reflect=defer_table_reflect,
    )

    # every run loads a single page of max 4 records, the last page is not full
    runs = 0
    while True:
        assert_load_info(pipeline.run(account.add_limit(1)))
        runs += 1
        if pipeline.last_trace.last_normalize_info.row_counts["account"] < 4:
            break
        assert runs < 10

    # the next run starts at the last timestamp, records on it are loaded again and merged:
    # runs load stamps 1-4, 4-7, 7 and 9 x 3, 9 x 3
    assert runs == 4
    assert all(
        query.endswith(" ORDER BY SystemModstamp ASC LIMIT 4") for query in org.queries
    )
    assert load_table_counts(pipeline, "account") == {"account": 10}
    # merge on a timestamp cursor does not deduplicate, the state keeps no record hashes
    assert incremental_state(pipeline, "SystemModstamp")["unique_hashes"] == []


def test_salesforce_table_chunked_loading_warnings(
    sf_mock: requests_mock.Mocker, credentials: InstanceAuth, mocker: MockerFixture
) -> None:
    warning = mocker.spy(table_module.logger, "warning")
    # 5 records share the first timestamp, more than the limit
    stamps = ["2024-01-01T00:00:01.000Z"] * 5 + ["2024-01-01T00:00:02.000Z"]
    FakeBulk2Org(sf_mock, SOURCE_FIELDS, account_records(stamps))
    pipeline = dlt.pipeline(
        pipeline_name="salesforce_stuck", destination="duckdb", dev_mode=True
    )
    account = salesforce_table(
        credentials=credentials,
        sobject="Account",
        chunk_size=3,
        incremental=dlt.sources.incremental("SystemModstamp", row_order="asc"),
    )
    pipeline.run(account.add_limit(1))
    assert warning.call_count == 0
    # all records on the next page have the start timestamp, the cursor cannot move
    pipeline.run(account.add_limit(1))
    assert "Could not move the cursor `SystemModstamp`" in warning.call_args.args[0]

    # LIMIT without ORDER BY returns any records
    warning.reset_mock()
    pipeline.run(
        salesforce_table(credentials=credentials, sobject="Account").add_limit(1)
    )
    assert "without ORDER BY" in warning.call_args_list[0].args[0]


@pytest.mark.parametrize(
    "cursor,write_disposition,deduplicated",
    [("SystemModstamp", "merge", False), ("SystemModstamp", "append", True)],
    ids=["merge", "append"],
)
def test_salesforce_table_deduplication(
    sf_mock: requests_mock.Mocker,
    credentials: InstanceAuth,
    cursor: str,
    write_disposition: TWriteDisposition,
    deduplicated: bool,
) -> None:
    # records sharing the last cursor value are deduplicated with hashes kept in the state.
    # a deferred describe sets the same deduplication key, the record id
    FakeBulk2Org(
        sf_mock, SOURCE_FIELDS, account_records(["2024-01-01T00:00:01.000Z"] * 3)
    )
    states = []
    for defer_table_reflect in (False, True):
        pipeline = dlt.pipeline(
            pipeline_name=f"salesforce_dedup_{defer_table_reflect}",
            destination="duckdb",
            dev_mode=True,
        )
        pipeline.run(
            salesforce_table(
                credentials=credentials,
                sobject="Account",
                write_disposition=write_disposition,
                defer_table_reflect=defer_table_reflect,
            )
        )
        states.append(incremental_state(pipeline, cursor))
    assert bool(states[0]["unique_hashes"]) is deduplicated
    assert states[1]["unique_hashes"] == states[0]["unique_hashes"]


def mock_login(m: requests_mock.Mocker) -> Any:
    """Mocks the client credentials login"""
    return m.post(
        "https://test.my.salesforce.com/services/oauth2/token",
        json={
            "access_token": "token",
            "instance_url": "https://test.my.salesforce.com",
        },
    )


def login_credentials() -> ConsumerKeySecretDomainAuth:
    credentials = ConsumerKeySecretDomainAuth(
        consumer_key="key", consumer_secret="secret", domain="test.my"
    )
    credentials.resolve()
    return credentials


@pytest.mark.parametrize("backend", ["python", "pyarrow"])
def test_salesforce_table_defer_table_reflect(
    sf_mock: requests_mock.Mocker, backend: TTableBackend
) -> None:
    # a deferred resource logs in and describes when extracting and gets the same hints
    token = mock_login(sf_mock)
    page = (
        '"Id","Name","IsDeleted","AnnualRevenue","SystemModstamp"\n'
        '"001A","Acme","false","15000000","2024-01-15T10:30:00.000Z"\n'
        '"001B","NA","true",,"2024-02-01T00:00:00.000Z"\n'
        '"001C","Globex","false","","2024-01-20T08:00:00.000Z"\n'
    )
    mock_bulk2(sf_mock, [page])
    tables, inputs = [], []
    for defer_table_reflect in (False, True):
        calls_count = sf_mock.call_count
        resource = salesforce_table(
            credentials=login_credentials(),
            sobject="Account",
            backend=backend,
            included_columns=[
                "Id",
                "Name",
                "IsDeleted",
                "AnnualRevenue",
                "SystemModstamp",
            ],
            defer_table_reflect=defer_table_reflect,
        )
        resource.apply_hints(columns={"AnnualRevenue": {"data_type": "double"}})
        # login and describe
        assert sf_mock.call_count - calls_count == (0 if defer_table_reflect else 2)

        pipeline = dlt.pipeline(
            pipeline_name=f"salesforce_defer_{defer_table_reflect}",
            destination="duckdb",
            dev_mode=True,
        )
        assert_load_info(pipeline.run(resource))
        assert load_table_counts(pipeline, "account") == {"account": 2}
        tables.append(pipeline.default_schema.get_table("account"))
        inputs.append(pipeline.last_trace.last_extract_info.asdict()["inputs"])

    assert token.call_count == 2
    # same columns with types, primary key and the hard delete hint. emitted hints are merged
    # into the user hints so the column order may differ
    assert tables[1]["columns"] == tables[0]["columns"]
    assert tables[1]["columns"]["annual_revenue"]["data_type"] == "double"
    assert tables[1]["write_disposition"] == tables[0]["write_disposition"] == "merge"
    # the location is recorded after login
    for extract_inputs in inputs:
        assert [(i["kind"], i["location"]) for i in extract_inputs] == [
            ("salesforce", "https://test.my.salesforce.com")
        ]


def test_salesforce_table_defer_table_reflect_no_system_modstamp(
    sf_mock: requests_mock.Mocker, credentials: InstanceAuth
) -> None:
    # the "auto" incremental of a deferred resource expects SystemModstamp
    fields = [
        {"name": name, "type": FIELD_TYPES[name], "nillable": name != "Id"}
        for name in ("Id", "CreatedDate")
    ]
    mock_bulk2(sf_mock, [""], fields=fields)
    resource = salesforce_table(
        credentials=credentials, sobject="Account", defer_table_reflect=True
    )
    with pytest.raises(ResourceExtractionError) as py_ex:
        list(resource)
    assert "no `SystemModstamp` field" in str(py_ex.value.__cause__)


def test_salesforce_source_defer_table_reflect(sf_mock: requests_mock.Mocker) -> None:
    # creating the source makes no API calls, all resources share a single login
    token = mock_login(sf_mock)
    page = (
        '"Id","Name","IsDeleted","SystemModstamp"\n'
        '"001A","Acme","false","2024-01-15T10:30:00.000Z"\n'
    )
    mock_bulk2(sf_mock, [page], fields=SOURCE_FIELDS, sobject=r"\w+")
    source = salesforce_source(
        credentials=login_credentials(), defer_table_reflect=True
    )
    assert sf_mock.call_count == 0

    pipeline = dlt.pipeline(
        pipeline_name="salesforce_source_deferred", destination="duckdb", dev_mode=True
    )
    assert_load_info(pipeline.run(source))
    assert token.call_count == 1
    assert load_table_counts(pipeline, *SOURCE_SOBJECTS) == {
        name: 1 for name in SOURCE_SOBJECTS
    }
    for name in SOURCE_SOBJECTS:
        table = pipeline.default_schema.get_table(name)
        assert table["write_disposition"] == "merge"
        assert table["columns"]["id"]["primary_key"] is True
        assert table["columns"]["is_deleted"]["hard_delete"] is True
        assert table["columns"]["system_modstamp"]["data_type"] == "timestamp"

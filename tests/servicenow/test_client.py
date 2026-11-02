import json
import threading
from datetime import datetime, time, timedelta, timezone  # noqa: I251
from email.utils import format_datetime
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any, List, Optional

import pytest
import requests_mock
from dlt.extract.exceptions import ResourceExtractionError
from dlt.sources.helpers.requests import Response, Timeout
from pytest_mock import MockerFixture

from sources.servicenow import servicenow_table
from sources.servicenow.helpers.client import (
    MAX_URL_LENGTH,
    ServiceNowClient,
    TransactionCancelled,
    _should_retry,
    csv_encoding,
    make_session,
    read_csv_table,
    read_records,
    read_string_table,
)
from sources.servicenow.helpers.credentials import ApiKeyCredentials, BasicCredentials
from sources.servicenow.helpers.query import TableQuery
from sources.servicenow.helpers.table import TReadApi, hidden_fields

from tests.servicenow.seed import data
from tests.servicenow.utils import (
    INCIDENT,
    INSTANCE,
    TRANSACTION_CANCELLED,
    FakeInstance,
    TRecord,
    add_task_tables,
    item_rows,
    make_records,
    updated_on,
)


def test_read_string_table() -> None:
    records = [
        {"sys_id": "1", "name": "multi\nline ✓", "unknown": "x"},
        {"sys_id": "2"},
    ]
    for content in (
        json.dumps({"result": records}).encode(),
        json.dumps({"result": records}, indent=2, ensure_ascii=False).encode(),
    ):
        # unknown fields are skipped, missing fields are nulls
        assert read_string_table(content, ["sys_id", "name"]).to_pylist() == [
            {"sys_id": "1", "name": "multi\nline ✓"},
            {"sys_id": "2", "name": None},
        ]
    assert read_string_table(b'{"result":[]}', ["sys_id"]).num_rows == 0


@pytest.mark.parametrize(
    "body,error",
    [
        # cancelled transactions send a part of the records with status 200
        (
            {
                "result": [{"sys_id": "1"}],
                "error": {"message": TRANSACTION_CANCELLED, "detail": None},
                "status": "failure",
            },
            TransactionCancelled,
        ),
        (b'{"result":[{"sys_id":"1"}],""}', TransactionCancelled),
        (b'{"result":[{"sys_id":"1"},{"sys_i', TransactionCancelled),
        (
            {
                "error": {"message": "Invalid table", "detail": None},
                "status": "failure",
            },
            ValueError,
        ),
        ({}, ValueError),
        ({"result": {}}, ValueError),
        (b"<html>login</html>", ValueError),
    ],
    ids=[
        "cancelled",
        "cut-off",
        "cut-off-record",
        "error",
        "no-result",
        "result-object",
        "html",
    ],
)
def test_read_invalid_responses(body: Any, error: type) -> None:
    content = body if isinstance(body, bytes) else json.dumps(body).encode()
    with pytest.raises(error):
        read_records(content)
    with pytest.raises(error):
        read_string_table(content, ["sys_id"])


def test_read_cancelled_csv_export() -> None:
    content = b'"sys_id","name"\n"1","a"\nTransaction cancelled - maximum execution time exceeded'
    for ending in (b"", b"\r\n", b"\n \n"):
        with pytest.raises(TransactionCancelled):
            read_csv_table(content + ending, "utf-8", ["sys_id", "name"])
    assert read_csv_table(b'"sys_id"\n', "utf-8", ["sys_id"]).num_rows == 0
    table = read_csv_table(
        b'"sys_id","name"\r\n"1","a"\r\n', "utf-8", ["sys_id", "name"]
    )
    assert table.to_pylist() == [{"sys_id": "1", "name": "a"}]


def test_captured_hidden_field() -> None:
    """Reads the Table API page and CSV export captured while a field ACL hides a field"""
    with open(data.CAPTURED_HIDDEN_PAGE, "rb") as f:
        page = f.read()
    with open(data.CAPTURED_HIDDEN_EXPORT, "rb") as f:
        export = f.read()
    fields = data.HIDDEN_CAPTURE_FIELDS
    pages = {
        "python": read_records(page),
        "pyarrow": read_string_table(page, fields),
        "csv": read_csv_table(export, csv_encoding("text/csv", export), fields),
    }
    for name, read in pages.items():
        assert hidden_fields(read, fields) == [data.HIDDEN_FIELD], name
    # exports quote empty values, values hidden by the ACL are not quoted
    assert pages["csv"].column("u_when").to_pylist()[:2] == ["", ""]


@pytest.mark.parametrize("backend", ["python", "pyarrow"])
def test_servicenow_table_pagination(
    instance: FakeInstance, credentials: BasicCredentials, backend: str
) -> None:
    records = make_records(25, updated_on)
    add_task_tables(instance, records)
    # hidden records shorten pages, the read continues until an empty page
    instance.hidden = {records[3]["sys_id"], records[20]["sys_id"]}
    resource = servicenow_table(
        credentials=credentials,
        table="incident",
        write_disposition="replace",
        page_size=10,
        backend=backend,
        allow_partitioned_load=False,
    )
    loaded = [row["sys_id"] for row in item_rows(list(resource))]
    assert loaded == sorted(
        r["sys_id"] for r in records if r["sys_id"] not in instance.hidden
    )
    # pages of 9, 10 and 4 records
    assert instance.queries("incident") == [
        "ORDERBYsys_id",
        f"sys_id>{loaded[8]}^ORDERBYsys_id",
        f"sys_id>{loaded[18]}^ORDERBYsys_id",
        f"sys_id>{loaded[22]}^ORDERBYsys_id",
    ]
    # the last page is empty, the remaining records are counted
    assert instance.count_queries("incident") == [f"sys_id>{loaded[22]}^ORDERBYsys_id"]
    params, headers = instance.page_requests("incident")[-1]
    assert params["sysparm_display_value"] == "false"
    assert params["sysparm_exclude_reference_link"] == "true"
    assert params["sysparm_no_count"] == "true"
    assert params["sysparm_suppress_pagination_header"] == "true"
    assert params["sysparm_limit"] == "10"
    assert "work_notes" not in params["sysparm_fields"].split(",")
    assert headers["Authorization"].startswith("Basic ")


def test_servicenow_table_ignored_keyset(
    instance: FakeInstance, credentials: BasicCredentials
) -> None:
    add_task_tables(instance, make_records(5, updated_on))
    instance.ignore_keyset = {"incident"}
    resource = servicenow_table(
        credentials=credentials,
        table="incident",
        write_disposition="replace",
        page_size=2,
    )
    with pytest.raises(ResourceExtractionError, match="query_range"):
        list(resource)


@pytest.mark.parametrize("api", ["table", "csv"])
def test_servicenow_table_hidden_pages(
    instance: FakeInstance, credentials: BasicCredentials, api: str
) -> None:
    records = make_records(30, updated_on)
    add_task_tables(instance, records)
    ordered = sorted(record["sys_id"] for record in records)
    # ACLs hide a full page and a record of the next one after the limit is applied
    instance.hidden = set(ordered[10:20]) | {ordered[25]}
    resource = servicenow_table(
        credentials=credentials,
        table="incident",
        write_disposition="replace",
        page_size=10,
        api=api,
        allow_partitioned_load=False,
    )
    rows = item_rows(list(resource))
    assert sorted(row["sys_id"] for row in rows) == [
        sys_id for sys_id in ordered if sys_id not in instance.hidden
    ]
    # an empty page is skipped while records are counted after it
    assert instance.count_queries("incident") == [
        f"sys_id>{ordered[9]}^ORDERBYsys_id",
        f"sys_id>{ordered[29]}^ORDERBYsys_id",
    ]
    offsets = [
        params.get("sysparm_offset") for params, _ in instance.page_requests("incident")
    ]
    assert offsets == [None, None, "10", None]

    instance.requests.clear()
    instance.hidden = set(ordered)
    assert item_rows(list(resource)) == []
    offsets = [
        params.get("sysparm_offset") for params, _ in instance.page_requests("incident")
    ]
    assert offsets == [None, "10", "20"]


def test_servicenow_table_collation(
    instance: FakeInstance, credentials: BasicCredentials
) -> None:
    # sys_ids can be mixed case or not hex, and the database can compare them case-insensitively
    records = make_records(6, updated_on)
    for record, sys_id in zip(
        records, ["global", "Alpha", "beta", "Charlie", "delta", "Echo"]
    ):
        record["sys_id"] = sys_id
    add_task_tables(instance, records)
    instance.collation = str.lower
    resource = servicenow_table(
        credentials=credentials,
        table="incident",
        write_disposition="replace",
        page_size=2,
    )
    assert [row["sys_id"] for row in item_rows(list(resource))] == sorted(
        (record["sys_id"] for record in records), key=str.lower
    )


def test_servicenow_table_page_shrink(
    instance: FakeInstance, credentials: BasicCredentials, mocker: MockerFixture
) -> None:
    sleep = mocker.patch("time.sleep")
    add_task_tables(instance, make_records(450, updated_on))
    instance.failures["incident"] = [(500, TRANSACTION_CANCELLED, {})] * 2
    rows = item_rows(
        list(
            servicenow_table(
                credentials=credentials,
                table="incident",
                write_disposition="replace",
                page_size=400,
            )
        )
    )
    assert len(rows) == 450
    # the same page is read again with half of the records, cancellations are not retried
    limits = [
        int(params["sysparm_limit"]) for params, _ in instance.page_requests("incident")
    ]
    assert limits == [400, 200] + [100] * 6
    assert instance.queries("incident")[:3] == ["ORDERBYsys_id"] * 3
    sleep.assert_not_called()

    # a page of 100 records is not split
    instance.failures["incident"] = [(500, TRANSACTION_CANCELLED, {})] * 2
    resource = servicenow_table(
        credentials=credentials,
        table="incident",
        write_disposition="replace",
        page_size=200,
    )
    with pytest.raises(
        ResourceExtractionError, match="HTTP 500 .* Transaction cancelled"
    ):
        list(resource)


@pytest.mark.parametrize(
    "api,backend", [("table", "python"), ("table", "pyarrow"), ("csv", "pyarrow")]
)
def test_servicenow_table_cancelled_in_response(
    instance: FakeInstance, credentials: BasicCredentials, api: str, backend: str
) -> None:
    add_task_tables(instance, make_records(30, updated_on))
    # cancelled transactions send a part of the page with status 200
    instance.failures["incident"] = [(200, TRANSACTION_CANCELLED, {})]
    resource = servicenow_table(
        credentials=credentials,
        table="incident",
        write_disposition="replace",
        page_size=200,
        api=api,
        backend=backend,
    )
    assert len(item_rows(list(resource))) == 30
    limits = [
        params.get("sysparm_limit", params.get("sysparm_record_count"))
        for params, _ in instance.page_requests("incident")
    ]
    assert limits[:2] == ["200", "100"]


def test_keyset_pages_timeout(
    credentials: BasicCredentials, mocker: MockerFixture
) -> None:
    client = ServiceNowClient(credentials)
    mocker.patch.object(client, "count", return_value=0)
    limits: List[int] = []

    def fetch_page(query: str, limit: int, offset: int) -> List[TRecord]:
        limits.append(limit)
        if len(limits) == 1:
            raise Timeout("read timed out")
        return [{"sys_id": "1"}] if len(limits) == 2 else []

    pages = list(client._keyset_pages("incident", TableQuery(), 400, fetch_page))
    # a page that times out is read again with half of the records
    assert pages == [[{"sys_id": "1"}]] and limits == [400, 200, 200]


def test_servicenow_table_empty_response(
    instance: FakeInstance, credentials: BasicCredentials, mocker: MockerFixture
) -> None:
    mocker.patch("time.sleep")
    add_task_tables(instance, make_records(3, updated_on))
    # under load, the instance can answer with an empty 202, which the client retries
    instance.failures["incident"] = [(202, "", {})]
    resource = servicenow_table(
        credentials=credentials, table="incident", write_disposition="replace"
    )
    assert len(item_rows(list(resource))) == 3
    assert len(instance.page_requests("incident")) == 3


def test_servicenow_table_retries(
    instance: FakeInstance, credentials: BasicCredentials, mocker: MockerFixture
) -> None:
    sleep = mocker.patch("time.sleep")
    add_task_tables(instance, make_records(3, updated_on))
    retry_date = format_datetime(
        datetime.now(timezone.utc) + timedelta(seconds=30), usegmt=True
    )
    instance.failures["incident"] = [
        (429, "Too many requests", {"Retry-After": "7"}),
        (429, "Too many requests", {"Retry-After": retry_date}),
        (503, "Service unavailable", {}),
    ]
    rows = item_rows(
        list(
            servicenow_table(
                credentials=credentials, table="incident", write_disposition="replace"
            )
        )
    )
    assert len(rows) == 3
    waits = [call.args[0] for call in sleep.call_args_list]
    assert len(waits) == 3 and waits[0] == 7 and 20 < waits[1] <= 30


def test_servicenow_table_html_response(credentials: BasicCredentials) -> None:
    with requests_mock.Mocker() as m:
        m.get(
            requests_mock.ANY,
            text="<!DOCTYPE html><html>login</html>",
            headers={"Content-Type": "text/html;charset=UTF-8"},
        )
        with pytest.raises(ValueError, match="HTML page"):
            servicenow_table(credentials=credentials, table="incident")


@pytest.mark.parametrize(
    "encoding,content_type",
    [
        ("utf-8", "text/csv;charset=UTF-8"),
        ("cp1252", "text/csv;charset=windows-1252"),
        ("cp1252", "text/csv"),
        ("utf-8", "text/csv"),
        ("utf-8-sig", "application/octet-stream"),
    ],
    ids=["utf-8", "windows-1252", "default-encoding", "utf-8-detected", "utf-8-bom"],
)
def test_servicenow_table_csv_export(
    instance: FakeInstance,
    credentials: BasicCredentials,
    mocker: MockerFixture,
    encoding: str,
    content_type: str,
) -> None:
    empty = {"sys_id": "2", **{name: "" for name in INCIDENT if name != "sys_id"}}
    empty["sys_updated_on"] = "2024-01-01 00:00:00"
    text = {**INCIDENT, "sys_id": "3", "sys_class_name": 'multi\r\nline, "quoted" café'}
    # exports put an apostrophe before text that starts with a formula character, not numbers
    formulas = [
        {**INCIDENT, "sys_id": f"4{i}", "sys_class_name": value, "priority": "-5"}
        for i, value in enumerate(
            ["=SUM(A1:A2)", "+1 555", "-dash", "@user", "'quoted"]
        )
    ]
    add_task_tables(instance, [INCIDENT, empty, text, *formulas])
    instance.csv_encoding = encoding
    instance.csv_content_type = content_type
    warning = mocker.patch("sources.servicenow.helpers.client.logger.warning")

    rows = {
        api: item_rows(
            list(
                servicenow_table(
                    credentials=credentials, table="incident", api=api, page_size=2
                )
            )
        )
        for api in ("table", "csv")
    }
    assert rows["csv"] == rows["table"]
    assert len(rows["csv"]) == 8
    exports = [params for _, params, _ in instance.requests if "CSV" in params]
    window = "sys_updated_on<2024-03-01 00:00:00^ORsys_updated_onISEMPTY"
    assert [params["sysparm_query"] for params in exports[:2]] == [
        f"{window}^ORDERBYsys_id",
        f"{window}^sys_id>3^ORDERBYsys_id",
    ]
    assert len(exports) == 5
    assert exports[0]["sysparm_record_count"] == "2"
    assert exports[0]["sysparm_display_value"] == "false"
    assert exports[0]["sysparm_export_column_header_label"] == "false"
    assert "work_notes" not in exports[0]["sysparm_fields"].split(",")
    # characters without a code in the encoding are lost on the instance
    warned = any(
        "glide.export.csv.charset" in call.args[0] for call in warning.call_args_list
    )
    assert warned == (encoding == "cp1252")


def test_servicenow_table_csv_export_errors(
    instance: FakeInstance, credentials: BasicCredentials
) -> None:
    add_task_tables(instance, [INCIDENT])
    with pytest.raises(ValueError, match="pyarrow"):
        servicenow_table(
            credentials=credentials, table="incident", api="csv", backend="python"
        )
    # the login page is returned for credentials not accepted for exports
    instance.csv_content_type = "text/html;charset=UTF-8"
    with pytest.raises(ResourceExtractionError, match="HTML page"):
        list(servicenow_table(credentials=credentials, table="incident", api="csv"))
    # labels instead of field names in the header
    instance.csv_content_type = "text/csv;charset=UTF-8"
    instance.csv_header_labels = True
    with pytest.raises(ResourceExtractionError, match="has no \\['sys_id'\\] columns"):
        list(servicenow_table(credentials=credentials, table="incident", api="csv"))
    instance.csv_header_labels = False
    # exports do not accept API keys and can reject basic authentication
    api_key = ApiKeyCredentials(instance=INSTANCE, api_key="key")
    api_key.resolve()
    with pytest.raises(ValueError, match="API keys"):
        servicenow_table(credentials=api_key, table="incident", api="csv")
    instance.failures["incident"] = [(401, "User Not Authenticated", {})]
    with pytest.raises(ResourceExtractionError, match="snc_basic_auth_api_access"):
        list(
            servicenow_table(
                credentials=credentials,
                table="incident",
                api="csv",
                write_disposition="replace",
            )
        )


@pytest.mark.parametrize("api", ["table", "csv"])
def test_servicenow_table_long_fields(
    instance: FakeInstance, credentials: BasicCredentials, api: str
) -> None:
    fields = {
        "sys_id": "GUID",
        **{f"u_field_with_a_long_name_{i}": "string" for i in range(300)},
    }
    instance.add_table(
        "u_wide",
        fields,
        records=[{"sys_id": "1", "u_field_with_a_long_name_1": "a", "u_unknown": "b"}],
    )
    rows = item_rows(
        list(servicenow_table(credentials=credentials, table="u_wide", api=api))
    )
    # all fields are returned and fields not in the dictionary are dropped
    params = instance.page_requests("u_wide")[-1][0]
    assert "sysparm_fields" not in params
    if api == "csv":
        assert params["sysparm_default_export_fields"] == "all"
    assert rows[0]["u_field_with_a_long_name_1"] == "a" and "u_unknown" not in rows[0]


def test_servicenow_table_unsafe_keys(
    instance: FakeInstance, credentials: BasicCredentials
) -> None:
    # sys_ids of records imported from other systems can be invalid in an encoded query
    records = make_records(5, updated_on)
    records[0]["sys_id"] = "x^y"
    records[1]["sys_id"] = "javascript:gs.getUserID()"
    add_task_tables(instance, records)
    resource = servicenow_table(
        credentials=credentials,
        table="incident",
        write_disposition="replace",
        page_size=2,
        allow_partitioned_load=False,
    )
    assert sorted(row["sys_id"] for row in item_rows(list(resource))) == sorted(
        record["sys_id"] for record in records
    )
    # pages after these records skip the records read after the last valid sys_id
    queries = instance.queries("incident")
    assert not any("javascript" in query or "x^y" in query for query in queries)
    offsets = [
        params.get("sysparm_offset") for params, _ in instance.page_requests("incident")
    ]
    assert offsets == [None, None, "2", "4"]


@pytest.mark.parametrize("api", ["table", "csv"])
def test_servicenow_table_denied_query(
    instance: FakeInstance, credentials: BasicCredentials, api: TReadApi
) -> None:
    add_task_tables(instance, make_records(3, updated_on))
    instance.tables["sys_audit_delete"] = []
    # query ACLs deny conditions on the cursor, exports return no records instead of an error
    instance.denied_fields = {"sys_updated_on"}
    resource = servicenow_table(credentials=credentials, table="incident", api=api)
    with pytest.raises(ResourceExtractionError, match="query_match.*sys_updated_on"):
        list(resource)


def test_servicenow_table_uncounted(
    instance: FakeInstance, credentials: BasicCredentials, mocker: MockerFixture
) -> None:
    records = make_records(6, updated_on)
    for i, record in enumerate(records):
        record["sys_id"] = f"i{i}"
    add_task_tables(instance, records)
    # remote tables are not counted, records after a page hidden by ACLs cannot be found
    instance.uncounted = {"incident"}
    instance.hidden = {"i0", "i1"}
    warning = mocker.patch("sources.servicenow.helpers.client.logger.warning")
    resource = servicenow_table(
        credentials=credentials,
        table="incident",
        write_disposition="replace",
        page_size=2,
    )
    assert item_rows(list(resource)) == []
    assert any("does not count" in call.args[0] for call in warning.call_args_list)


@pytest.mark.parametrize("api", ["table", "csv"])
def test_servicenow_table_long_query(
    instance: FakeInstance, credentials: BasicCredentials, api: str
) -> None:
    fields = {
        "sys_id": "GUID",
        **{f"u_field_with_a_long_name_{i}": "string" for i in range(150)},
    }
    instance.add_table(
        "u_wide", fields, records=[{"sys_id": "1", "u_field_with_a_long_name_1": "a"}]
    )

    def resource(conditions: int) -> Any:
        # a condition takes 15 characters in the url
        return servicenow_table(
            credentials=credentials,
            table="u_wide",
            api=api,
            write_disposition="replace",
            encoded_query="^OR".join(["sys_id=1"] * conditions),
        )

    # the fields and the query each fit in the URL but not together, so all fields are returned
    rows = item_rows(list(resource(300)))
    params = instance.page_requests("u_wide")[-1][0]
    assert "sysparm_fields" not in params
    assert rows[0]["u_field_with_a_long_name_1"] == "a"
    with pytest.raises(ResourceExtractionError, match="Shorten the encoded query"):
        list(resource(MAX_URL_LENGTH // 15 + 1))


@pytest.mark.parametrize(
    "status,body,retried",
    [
        (429, b"", True),
        (503, b"", True),
        (500, b"Transaction cancelled: maximum execution time exceeded", False),
        (202, b"", True),
        (200, b"null", True),
        (200, b" [] ", True),
        (200, b'{"result":[]}', False),
    ],
    ids=[
        "throttled",
        "unavailable",
        "cancelled",
        "empty-202",
        "null",
        "array",
        "empty",
    ],
)
def test_should_retry(status: int, body: bytes, retried: bool) -> None:
    response = Response()
    response.status_code = status
    response._content = body
    assert _should_retry(response, None) is retried


def test_session_without_cookies() -> None:
    # the instance runs the requests of a session one at a time, tables read in parallel must
    # not send the session cookie back
    cookies: List[Optional[str]] = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            cookies.append(self.headers.get("Cookie"))
            self.send_response(200)
            self.send_header("Set-Cookie", "JSESSIONID=session; Path=/")
            self.send_header("Content-Length", "0")
            self.end_headers()

        def log_message(self, *args: Any) -> None:
            pass

    server = HTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        session = make_session()
        for _ in range(2):
            session.get(f"http://127.0.0.1:{server.server_port}/")
    finally:
        server.shutdown()
    assert cookies == [None, None]

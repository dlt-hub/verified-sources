"""Offline tests of issues, changelogs and worklogs with the Jira REST API mocked"""

import glob
import json
import os
import subprocess
import sys
import textwrap
import time
from datetime import date, datetime, timedelta, timezone
from threading import Lock
from typing import Any, Dict, Iterator, List, Optional, Tuple

import dlt
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import requests_mock
from pytest_mock import MockerFixture
from dlt.common.libs.pyarrow import deserialize_type
from dlt.extract.exceptions import ResourceExtractionError
from dlt.sources import DltResource
from dlt.sources.helpers.requests import HTTPError

from sources.jira import issue_changelogs, issue_comments, jira, jira_issues, worklogs
from sources.jira.helpers.client import JiraClient
from sources.jira.helpers.issues import IssuesReader, changelog_rows
from sources.jira.helpers.schema_types import (
    field_to_column_schema,
    get_field_type,
    shape_to_arrow,
    trim_to_shape,
)

from tests.jira.utils import (
    BASE_URL,
    CREDENTIALS,
    CUSTOM,
    DEFAULT_FIELD_IDS,
    USER,
    JiraMock,
    custom_field,
    load_captured,
    make_comment,
    make_history,
    make_issue,
    make_worklog,
    system_field,
)
from tests.utils import (
    TEST_STORAGE_ROOT,
    assert_load_info,
    load_table_counts,
    load_tables_to_dicts,
)

UPDATED_1 = "2024-01-15T10:30:00.123+0100"
UPDATED_2 = "2024-01-16T08:00:00.000-0800"
START = datetime(2024, 1, 15, 9, 30, 15, tzinfo=timezone.utc)
# 2024-01-01T10:00:00Z
T1 = 1704103200000


@pytest.fixture
def http_mock() -> Iterator[requests_mock.Mocker]:
    with requests_mock.Mocker() as m:
        yield m


@pytest.fixture
def jira_mock(http_mock: requests_mock.Mocker) -> JiraMock:
    return JiraMock(http_mock)


@pytest.fixture
def captured_mock(http_mock: requests_mock.Mocker) -> Dict[str, Any]:
    """Serves field metadata and issues captured from the test site"""
    captured = load_captured()
    jira_mock = JiraMock(http_mock, fields=captured["fields"])
    jira_mock.add_issues(*captured["issues"])
    return captured


def run_to_parquet(
    name: str, data: Any, table_name: str = "issues"
) -> Tuple[dlt.Pipeline, pa.Table]:
    """Loads `data` into parquet files on the local filesystem and reads `table_name`"""
    bucket_path = os.path.abspath(os.path.join(TEST_STORAGE_ROOT, name))
    pipeline = dlt.pipeline(
        name, destination=dlt.destinations.filesystem(bucket_path), dev_mode=True
    )
    assert_load_info(pipeline.run(data, loader_file_format="parquet"))
    files = glob.glob(f"{bucket_path}/{pipeline.dataset_name}/{table_name}/*.parquet")
    return pipeline, pa.concat_tables(pq.read_table(file) for file in files)


def to_rows(items: List[Any]) -> List[Dict[str, Any]]:
    """Converts items yielded by a resource into rows"""
    rows: List[Dict[str, Any]] = []
    for item in items:
        if isinstance(item, pa.Table):
            rows.extend(item.to_pylist())
        else:
            rows.append(item)
    return rows


@pytest.mark.parametrize(
    "field,expected",
    [
        (system_field("summary", "string"), {"data_type": "text"}),
        (system_field("updated", "datetime"), {"data_type": "timestamp"}),
        (system_field("duedate", "date"), {"data_type": "date"}),
        (system_field("timespent", "number"), {"data_type": "bigint"}),
        (custom_field(10016, "number", CUSTOM + "float"), {"data_type": "double"}),
        (custom_field(10032, "date", CUSTOM + "datepicker"), {"data_type": "date"}),
        (
            custom_field(10060, "number", "com.example.app:rating"),
            {"data_type": "double"},
        ),
        (custom_field(10050, "any", "com.example.app:widget"), {"data_type": "text"}),
        (system_field("thumbnail", None), {"data_type": "text"}),
        (system_field("status", "status"), None),
        (system_field("labels", "array", "string"), None),
        (custom_field(10030, "option", CUSTOM + "select"), None),
        (custom_field(10031, "array", CUSTOM + "multiuserpicker", "user"), None),
        (custom_field(10061, "option", "com.example.app:choice"), None),
    ],
    ids=[
        "string",
        "datetime",
        "date",
        "seconds",
        "custom_float",
        "custom_date",
        "unknown_custom_number",
        "unknown_custom_any",
        "no_schema",
        "status",
        "labels",
        "select",
        "multiuserpicker",
        "unknown_custom_option",
    ],
)
def test_field_to_column_schema(
    field: Dict[str, Any], expected: Optional[Dict[str, Any]]
) -> None:
    # scalars and values of unknown structure are typed in both backends
    if expected:
        for backend in ("python", "pyarrow"):
            assert field_to_column_schema(field, backend, nested_types=True) == {
                "name": field["id"],
                **expected,
            }
        return
    # values of known structure are flattened and unnested by the normalizer
    assert field_to_column_schema(field, "python", nested_types=True) is None
    # or loaded as json
    assert field_to_column_schema(field, "pyarrow", nested_types=False) == {
        "name": field["id"],
        "data_type": "json",
    }
    nested_col = field_to_column_schema(field, "pyarrow", nested_types=True)
    assert nested_col["data_type"] == "json"
    assert pa.types.is_nested(deserialize_type(nested_col["x-nested-type"]))  # type: ignore[typeddict-item]


def test_nested_type_of_field() -> None:
    col = field_to_column_schema(
        system_field("status", "status"), "pyarrow", nested_types=True
    )
    assert str(deserialize_type(col["x-nested-type"])) == (  # type: ignore[index]
        "struct<id: string, name: string, statusCategory: struct<id: int64, key: string,"
        " name: string>>"
    )


@pytest.mark.parametrize(
    "value,shape,expected",
    [
        (None, {"id": "text"}, None),
        (
            USER,
            {"accountId": "text", "active": "bool"},
            {"accountId": "a1", "active": True},
        ),
        ({"id": "1"}, {"id": "text", "value": "text"}, {"id": "1"}),
        (
            [{"id": "1", "value": "Red", "self": "https://o"}],
            [{"id": "text", "value": "text"}],
            [{"id": "1", "value": "Red"}],
        ),
        ({"any": ["thing"]}, None, {"any": ["thing"]}),
        (3.5, "double", 3.5),
    ],
    ids=["none", "object", "missing_key", "list_of_objects", "unknown_shape", "scalar"],
)
def test_trim_to_shape(value: Any, shape: Any, expected: Any) -> None:
    assert trim_to_shape(value, shape) == expected


@pytest.mark.parametrize(
    "value,shape",
    [
        ("legacy", {"id": "text"}),
        ({"id": "1"}, [{"id": "text"}]),
        ({"weird": 1}, "double"),
        ({"child": "legacy"}, {"child": {"id": "text"}}),
    ],
    ids=["string_for_object", "object_for_list", "object_for_scalar", "nested"],
)
def test_trim_to_shape_unexpected_kind(value: Any, shape: Any) -> None:
    with pytest.raises(ValueError):
        trim_to_shape(value, shape)


def test_issues_python_backend(jira_mock: JiraMock) -> None:
    jira_mock.add_issues(make_issue(1, UPDATED_1), make_issue(2, UPDATED_2, labels=[]))
    pipeline = dlt.pipeline("jira_issues_python", destination="duckdb", dev_mode=True)
    assert_load_info(pipeline.run(jira_issues(**CREDENTIALS)))

    assert load_table_counts(
        pipeline,
        "issues",
        "issues__labels",
        "issues__fix_versions",
        "issues__customfield_10031",
    ) == {
        "issues": 2,
        "issues__labels": 2,
        "issues__fix_versions": 2,
        "issues__customfield_10031": 2,
    }
    columns = pipeline.default_schema.get_table_columns("issues")
    assert {
        name: columns[name]["data_type"] for name in columns if not name.startswith("_")
    } == {
        "id": "text",
        "key": "text",
        "summary": "text",
        "description": "text",
        "created": "timestamp",
        "updated": "timestamp",
        "duedate": "date",
        "timespent": "bigint",
        "customfield_10016": "double",
        "customfield_10032": "date",
        "customfield_10033": "timestamp",
        # values of unknown structure are text
        "customfield_10050": "text",
        "thumbnail": "text",
        # objects are flattened and trimmed to known keys
        "status__id": "text",
        "status__name": "text",
        "status__status_category__id": "bigint",
        "status__status_category__key": "text",
        "status__status_category__name": "text",
        "project__id": "text",
        "project__key": "text",
        "project__name": "text",
        "assignee__account_id": "text",
        "assignee__display_name": "text",
        "assignee__active": "bool",
        "assignee__account_type": "text",
        "assignee__time_zone": "text",
        "customfield_10030__id": "text",
        "customfield_10030__value": "text",
    }
    rows = sorted(
        load_tables_to_dicts(pipeline, "issues")["issues"], key=lambda r: r["id"]
    )
    assert [row["updated"] for row in rows] == [
        datetime(2024, 1, 15, 9, 30, 0, 123000, tzinfo=timezone.utc),
        datetime(2024, 1, 16, 16, 0, tzinfo=timezone.utc),
    ]
    assert rows[0]["key"] == "KAN-1"
    assert rows[0]["description"] == "h1. Title\n*bold*"
    assert rows[0]["duedate"] == date(2024, 2, 1)
    assert rows[0]["status__status_category__key"] == "indeterminate"


def test_issues_pyarrow_backend(jira_mock: JiraMock) -> None:
    jira_mock.add_issues(make_issue(1, UPDATED_1), make_issue(2, UPDATED_2))
    pipeline = dlt.pipeline("jira_issues_pyarrow", destination="duckdb", dev_mode=True)
    assert_load_info(pipeline.run(jira_issues(**CREDENTIALS, backend="pyarrow")))

    # nested values are loaded as json, there are no nested tables
    assert [table["name"] for table in pipeline.default_schema.data_tables()] == [
        "issues"
    ]
    columns = pipeline.default_schema.get_table_columns("issues")
    assert {
        name: columns[name]["data_type"] for name in columns if not name.startswith("_")
    } == {
        "id": "text",
        "key": "text",
        "summary": "text",
        "description": "text",
        "created": "timestamp",
        "updated": "timestamp",
        "duedate": "date",
        "status": "json",
        "project": "json",
        "assignee": "json",
        "labels": "json",
        "fix_versions": "json",
        "timespent": "bigint",
        "attachment": "json",
        "customfield_10016": "double",
        "customfield_10030": "json",
        "customfield_10031": "json",
        "customfield_10032": "date",
        "customfield_10033": "timestamp",
        "customfield_10050": "text",
        "thumbnail": "text",
        "parent": "json",
    }
    with pipeline.sql_client() as client:
        rows = client.execute_sql(
            "SELECT updated, status, labels, customfield_10050 FROM issues ORDER BY id"
        )
    assert rows[0][0] == datetime(2024, 1, 15, 9, 30, 0, 123000, tzinfo=timezone.utc)
    assert rows[1][0] == datetime(2024, 1, 16, 16, 0, tzinfo=timezone.utc)
    # trimmed to known keys, nested keys are not normalized
    assert rows[0][1] == (
        '{"id":"3","name":"In Progress","statusCategory":{"id":4,"key":"indeterminate",'
        '"name":"In Progress"}}'
    )
    assert rows[0][2] == '["backend","urgent"]'
    assert rows[0][3] == '{"anything":["goes",1]}'


def test_issues_pyarrow_nested_types(jira_mock: JiraMock) -> None:
    jira_mock.add_issues(make_issue(1, UPDATED_1))
    _, table = run_to_parquet(
        "jira_issues_nested",
        jira_issues(**CREDENTIALS, backend="pyarrow", nested_types=True),
    )

    schema = table.schema
    assert str(schema.field("status").type) == (
        "struct<id: string, name: string, statusCategory: struct<id: int64, key: string,"
        " name: string>>"
    )
    assert str(schema.field("labels").type) == "list<element: string>"
    assert str(schema.field("customfield_10031").type) == (
        "list<element: struct<accountId: string, displayName: string, emailAddress: string,"
        " active: bool, accountType: string, timeZone: string>>"
    )
    # values of unknown structure are text
    assert schema.field("customfield_10050").type == pa.string()
    row = table.to_pylist()[0]
    assert row["fix_versions"] == [
        {
            "id": "10",
            "name": "1.0",
            "archived": None,
            "released": True,
            "releaseDate": None,
        }
    ]


@pytest.mark.parametrize("destination", ["filesystem", "duckdb"])
def test_issues_nested_types_support(jira_mock: JiraMock, destination: str) -> None:
    # a string instead of an option and an object instead of a list of users
    jira_mock.add_issues(
        make_issue(1, UPDATED_1),
        make_issue(2, UPDATED_1, customfield_10030="legacy", customfield_10031=USER),
    )
    issues = jira_issues(**CREDENTIALS, backend="pyarrow", nested_types=True)
    if destination == "filesystem":
        _, table = run_to_parquet("jira_nested_support", issues)
        assert pa.types.is_struct(table.schema.field("customfield_10030").type)
        assert pa.types.is_list(table.schema.field("customfield_10031").type)
        rows = {row["key"]: row for row in table.to_pylist()}
    else:
        pipeline = dlt.pipeline(
            "jira_nested_support", destination="duckdb", dev_mode=True
        )
        assert_load_info(pipeline.run(issues))
        # duckdb does not support nested types, values are json strings
        with pipeline.sql_client() as client:
            types = dict(
                client.execute_sql(
                    "SELECT column_name, data_type FROM information_schema.columns"
                    " WHERE table_name = 'issues'"
                )
            )
            records = client.execute_sql(
                "SELECT key, customfield_10030, customfield_10031 FROM issues"
            )
        assert types["customfield_10030"] == types["customfield_10031"] == "JSON"
        rows = {
            key: {
                "customfield_10030": option and json.loads(option),
                "customfield_10031": users and json.loads(users),
            }
            for key, option, users in records
        }
    assert rows["KAN-1"]["customfield_10030"]["value"] == "Red"
    assert [user["accountId"] for user in rows["KAN-1"]["customfield_10031"]] == ["a1"]
    # values of unexpected structure are loaded as null
    assert rows["KAN-2"]["customfield_10030"] is None
    assert rows["KAN-2"]["customfield_10031"] is None


def test_issues_captured_nested_types(captured_mock: Dict[str, Any]) -> None:
    pipeline, table = run_to_parquet(
        "jira_captured_nested",
        jira_issues(**CREDENTIALS, backend="pyarrow", nested_types=True),
    )

    # fields of known shape have nested types
    naming = pipeline.default_schema.naming
    shapes = {field["id"]: get_field_type(field) for field in captured_mock["fields"]}
    nested_shapes = {k: v for k, v in shapes.items() if isinstance(v, (dict, list))}
    assert sorted(nested_shapes) == [
        "aggregateprogress",
        "assignee",
        "attachment",
        "components",
        "creator",
        "customfield_10020",
        "customfield_10021",
        "customfield_10036",
        "customfield_10044",
        "customfield_10045",
        "customfield_10046",
        "customfield_10051",
        "customfield_10052",
        "fixVersions",
        "issuelinks",
        "issuetype",
        "labels",
        "parent",
        "priority",
        "progress",
        "project",
        "reporter",
        "status",
        "statusCategory",
        "subtasks",
        "timetracking",
        "versions",
        "votes",
        "watches",
    ]
    for field_id, shape in nested_shapes.items():
        column = table.schema.field(naming.normalize_identifier(field_id))
        assert column.type == shape_to_arrow(shape), field_id

    rows = {row["key"]: row for row in table.to_pylist()}
    assert len(rows) == 8
    assert rows["TST-13"]["customfield_10020"] == [
        {
            "id": 4,
            "name": "TST Sprint 3",
            "state": "future",
            "boardId": 2,
            "goal": "[dummy] schema contracts",
            "startDate": None,
            "endDate": None,
            "completeDate": None,
        }
    ]
    all_types = rows["TST-24"]
    assert all_types["customfield_10046"] == {
        "id": "10027",
        "value": "[dummy] sources",
        "child": {"id": "10029", "value": "[dummy] rest_api"},
    }
    assert [option["value"] for option in all_types["customfield_10045"]] == [
        "[dummy] duckdb",
        "[dummy] bigquery",
    ]
    assert all_types["timetracking"] == {
        "originalEstimateSeconds": 7200,
        "remainingEstimateSeconds": 5400,
        "timeSpentSeconds": None,
    }
    (attachment,) = rows["TST-17"]["attachment"]
    assert attachment["filename"] == "dlt-seed.csv"
    assert attachment["mimeType"] == "text/csv"
    assert attachment["author"] == {
        "accountId": "account-1",
        "displayName": "User account-1",
    }
    assert [subtask["key"] for subtask in rows["TST-18"]["subtasks"]] == [
        "TST-19",
        "TST-20",
    ]
    assert rows["TST-19"]["parent"] == {"id": "10034", "key": "TST-18"}
    assert sorted(
        (link["type"]["name"], link["outwardIssue"]["key"], link["inwardIssue"])
        for link in rows["TST-18"]["issuelinks"]
    ) == [("Blocks", "TST-21", None), ("Relates", "TST-21", None)]
    # rank is text, values of unknown structure are serialized as json
    assert all_types["customfield_10019"] == "0|i0008n:"
    assert all_types["customfield_10002"] == "[]"
    # restrictions have dynamic keys and are not loaded by default
    assert "issuerestriction" not in table.column_names


def test_issues_nested_types_snowflake(captured_mock: Dict[str, Any]) -> None:
    pytest.importorskip("snowflake.connector")
    if dlt.secrets.get("destination.snowflake.credentials") is None:
        pytest.skip("Snowflake credentials are not configured")
    pipeline = dlt.pipeline(
        "jira_nested_snowflake",
        destination=dlt.destinations.snowflake(use_nested_types=True),
        dev_mode=True,
    )
    for _ in range(2):
        # the second run merges issues updated within lag into the structured columns
        issues = jira_issues(**CREDENTIALS, backend="pyarrow", nested_types=True)
        assert_load_info(pipeline.run(issues, loader_file_format="parquet"))

    text, number = "VARCHAR(16777216)", "NUMBER(19,0)"
    issue_ref = f"OBJECT(id {text}, key {text})"
    with pipeline.sql_client() as client:
        table = client.make_qualified_table_name("issues")
        types = {
            row[0]: row[1] for row in client.execute_sql(f"DESCRIBE TABLE {table}")
        }
        rows = client.execute_sql(
            f"""
            SELECT
                key,
                customfield_10020[0]:"name"::string,
                customfield_10020[0]:"boardId"::int,
                customfield_10046:"child":"value"::string,
                attachment[0]:"author":"accountId"::string,
                issuelinks[0]:"outwardIssue":"key"::string,
                subtasks[1]:"key"::string,
                parent:"key"::string,
                status:"statusCategory":"key"::string,
                ARRAY_SIZE(labels),
                customfield_10019
            FROM {table}
            """
        )
    # nested types are created as structured types with Jira keys
    assert types["CUSTOMFIELD_10020"] == (
        f"ARRAY(OBJECT(id {number}, name {text}, state {text}, boardId {number}, goal {text},"
        f" startDate {text}, endDate {text}, completeDate {text}))"
    )
    assert types["ISSUELINKS"] == (
        f"ARRAY(OBJECT(id {text}, type OBJECT(id {text}, name {text}, inward {text}, outward"
        f" {text}), inwardIssue {issue_ref}, outwardIssue {issue_ref}))"
    )
    assert types["ATTACHMENT"] == (
        f"ARRAY(OBJECT(id {text}, filename {text}, mimeType {text}, size {number}, created"
        f" {text}, author OBJECT(accountId {text}, displayName {text})))"
    )
    assert types["CUSTOMFIELD_10046"] == (
        f"OBJECT(id {text}, value {text}, child OBJECT(id {text}, value {text}))"
    )
    assert types["STATUS"] == (
        f"OBJECT(id {text}, name {text}, statusCategory OBJECT(id {number}, key {text}, name"
        f" {text}))"
    )
    assert types["PARENT"] == issue_ref
    assert types["LABELS"] == f"ARRAY({text})"
    assert types["CUSTOMFIELD_10019"] == text

    values = {row[0]: row[1:] for row in rows}
    assert len(values) == 8
    assert values["TST-13"][:2] == ("TST Sprint 3", 2)
    assert values["TST-24"][2] == "[dummy] rest_api"
    assert values["TST-17"][3] == "account-1"
    assert values["TST-18"][4:6] == ("TST-21", "TST-20")
    assert values["TST-19"][6] == "TST-18"
    labels = next(i for i in captured_mock["issues"] if i["key"] == "TST-24")["fields"][
        "labels"
    ]
    assert values["TST-24"][7:] == ("new", len(labels), "0|i0008n:")


def test_issues_captured_parallel_pages(
    captured_mock: Dict[str, Any], mocker: MockerFixture
) -> None:
    # requests_mock serializes requests, the delay is added around them to overlap the fetches
    fetch_issues = IssuesReader._fetch_issues
    lock, fetches = Lock(), {"in_flight": 0, "max_parallel": 0}

    def slow_fetch_issues(
        reader: IssuesReader, ids: List[str], fields: List[str]
    ) -> Any:
        with lock:
            fetches["in_flight"] += 1
            fetches["max_parallel"] = max(fetches["max_parallel"], fetches["in_flight"])
        time.sleep(0.1)
        try:
            return fetch_issues(reader, ids, fields)
        finally:
            with lock:
                fetches["in_flight"] -= 1

    mocker.patch.object(IssuesReader, "_fetch_issues", slow_fetch_issues)
    _, single_page = run_to_parquet(
        "jira_single_page",
        jira_issues(**CREDENTIALS, backend="pyarrow", nested_types=True),
    )
    assert fetches["max_parallel"] == 1

    # a page per issue, pages are fetched in parallel by dlt extract workers
    _, parallel_pages = run_to_parquet(
        "jira_parallel_pages",
        jira_issues(**CREDENTIALS, backend="pyarrow", nested_types=True, chunk_size=1),
    )
    assert fetches["max_parallel"] > 1

    # column hints reach the extractor before any of the pages
    def comparable(table: pa.Table) -> pa.Table:
        return table.drop_columns(
            [name for name in table.column_names if name.startswith("_dlt")]
        ).sort_by("key")

    single_page, parallel_pages = comparable(single_page), comparable(parallel_pages)
    assert parallel_pages.schema == single_page.schema
    assert parallel_pages.to_pylist() == single_page.to_pylist()


def test_issues_captured_python_backend(captured_mock: Dict[str, Any]) -> None:
    pipeline = dlt.pipeline("jira_captured_python", destination="duckdb", dev_mode=True)
    assert_load_info(pipeline.run(jira_issues(**CREDENTIALS)))

    issues = {issue["key"]: issue["fields"] for issue in captured_mock["issues"]}
    assert load_table_counts(
        pipeline,
        "issues",
        "issues__subtasks",
        "issues__issuelinks",
        "issues__customfield_10020",
    ) == {
        "issues": 8,
        "issues__subtasks": 2,
        "issues__issuelinks": sum(
            len(f.get("issuelinks", [])) for f in issues.values()
        ),
        "issues__customfield_10020": 1,
    }
    rows = {
        row["key"]: row for row in load_tables_to_dicts(pipeline, "issues")["issues"]
    }
    assert rows["TST-19"]["parent__key"] == "TST-18"
    assert rows["TST-24"]["customfield_10046__child__value"] == "[dummy] rest_api"
    assert rows["TST-24"]["timetracking__original_estimate_seconds"] == 7200
    assert rows["TST-24"]["customfield_10019"] == "0|i0008n:"


@pytest.mark.parametrize(
    "incremental,time_zone,jql,expected_jql",
    [
        ("auto", "UTC", None, 'created >= "1970-01-01" ORDER BY updated ASC'),
        (None, "UTC", None, 'created >= "1970-01-01"'),
        (
            dlt.sources.incremental("updated", initial_value=START),
            "UTC",
            None,
            'updated >= "2024-01-15 09:30" ORDER BY updated ASC',
        ),
        (
            dlt.sources.incremental("updated", initial_value=START),
            "America/Los_Angeles",
            None,
            'updated >= "2024-01-15 01:30" ORDER BY updated ASC',
        ),
        (
            dlt.sources.incremental("updated", initial_value=START),
            "Asia/Kolkata",
            None,
            'updated >= "2024-01-15 15:00" ORDER BY updated ASC',
        ),
        (
            dlt.sources.incremental("updated", initial_value=START),
            None,
            None,
            'updated >= "2024-01-15 09:30" ORDER BY updated ASC',
        ),
        (
            dlt.sources.incremental(
                "updated",
                initial_value=datetime(
                    2024, 1, 15, 10, 30, 15, tzinfo=timezone(timedelta(hours=1))
                ),
            ),
            "UTC",
            None,
            'updated >= "2024-01-15 09:30" ORDER BY updated ASC',
        ),
        (
            dlt.sources.incremental(
                "updated",
                initial_value=START,
                end_value=datetime(2024, 1, 16, 12, 0, 30, tzinfo=timezone.utc),
            ),
            "UTC",
            None,
            'updated >= "2024-01-15 09:30" AND updated < "2024-01-16 12:01" ORDER BY updated ASC',
        ),
        (
            dlt.sources.incremental(
                "updated",
                initial_value=START,
                end_value=datetime(2024, 1, 16, 12, 0, tzinfo=timezone.utc),
                range_start="open",
                range_end="closed",
            ),
            "UTC",
            None,
            'updated >= "2024-01-15 09:30" AND updated < "2024-01-16 12:01" ORDER BY updated ASC',
        ),
        (
            dlt.sources.incremental(
                "updated", initial_value=START, last_value_func=min
            ),
            "UTC",
            None,
            'updated < "2024-01-15 09:31" ORDER BY updated DESC',
        ),
        (
            dlt.sources.incremental("updated", initial_value=START, row_order="desc"),
            "UTC",
            None,
            'updated >= "2024-01-15 09:30" ORDER BY updated DESC',
        ),
        (
            dlt.sources.incremental(
                "updated", initial_value=START, on_cursor_value_missing="include"
            ),
            "UTC",
            None,
            '(updated >= "2024-01-15 09:30" OR updated is EMPTY) ORDER BY updated ASC',
        ),
        (
            dlt.sources.incremental(
                "updated", initial_value=START, on_cursor_value_missing="exclude"
            ),
            "UTC",
            None,
            'updated >= "2024-01-15 09:30" AND updated is not EMPTY ORDER BY updated ASC',
        ),
        (
            dlt.sources.incremental("customfield_10033", initial_value=START),
            "UTC",
            None,
            'cf[10033] >= "2024-01-15 09:30" ORDER BY cf[10033] ASC',
        ),
        (
            dlt.sources.incremental(
                "duedate", initial_value=date(2024, 2, 1), end_value=date(2024, 3, 1)
            ),
            "UTC",
            None,
            'duedate >= "2024-02-01" AND duedate <= "2024-03-01" ORDER BY duedate ASC',
        ),
        (
            dlt.sources.incremental(
                "updated", last_value_func=lambda values: values[-1]
            ),
            "UTC",
            None,
            'created >= "1970-01-01"',
        ),
        ("auto", "UTC", "project = KAN", "(project = KAN) ORDER BY updated ASC"),
        (
            dlt.sources.incremental("updated", initial_value=START),
            "UTC",
            "project = KAN OR labels = x",
            '(project = KAN OR labels = x) AND updated >= "2024-01-15 09:30"'
            " ORDER BY updated ASC",
        ),
    ],
    ids=[
        "auto_first_run",
        "no_incremental",
        "start",
        "time_zone_negative",
        "time_zone_half_hour",
        "no_user_time_zone",
        "start_with_offset",
        "end_open",
        "end_closed",
        "min",
        "row_order_desc",
        "include_missing",
        "exclude_missing",
        "custom_field_cursor",
        "date_cursor",
        "custom_last_value_func",
        "user_jql",
        "user_jql_with_start",
    ],
)
def test_issues_jql(
    http_mock: requests_mock.Mocker,
    incremental: Any,
    time_zone: Optional[str],
    jql: Optional[str],
    expected_jql: str,
) -> None:
    jira_mock = JiraMock(http_mock, time_zone=time_zone)
    assert (
        to_rows(list(jira_issues(**CREDENTIALS, incremental=incremental, jql=jql)))
        == []
    )
    assert jira_mock.jqls == [expected_jql]
    # search returns values of the cursor field for the incremental
    cursor = (
        "updated"
        if incremental == "auto"
        else getattr(incremental, "cursor_path", "id")
    )
    assert jira_mock.searches[0]["fields"] == cursor
    assert jira_mock.searches[0]["maxResults"] == "5000"
    # time zone is needed to render datetimes only
    assert jira_mock.myself_route.called == (incremental is not None)


@pytest.mark.parametrize(
    "cursor_path,exception",
    [
        ("summary", ValueError),
        ("resolutiondate", KeyError),
        ("fields.updated", ValueError),
    ],
    ids=["not_a_datetime", "unknown_field", "not_a_field_id"],
)
def test_issues_invalid_cursor(
    jira_mock: JiraMock, cursor_path: str, exception: type
) -> None:
    resource = jira_issues(
        **CREDENTIALS, incremental=dlt.sources.incremental(cursor_path)
    )
    with pytest.raises(ResourceExtractionError) as py_ex:
        list(resource)
    assert isinstance(py_ex.value.__cause__, exception)
    assert not jira_mock.search_route.called


def test_issues_jql_with_order_by() -> None:
    with pytest.raises(ValueError):
        jira_issues(**CREDENTIALS, jql="project = KAN order  by created")


@pytest.mark.parametrize("last_page_token", [False, True], ids=["no_token", "is_last"])
def test_issues_pagination(jira_mock: JiraMock, last_page_token: bool) -> None:
    jira_mock.add_issues(*(make_issue(issue_id, UPDATED_1) for issue_id in range(1, 6)))
    jira_mock.search_pages = [["1", "2"], ["3", "4"], ["5"]]
    jira_mock.last_page_token = last_page_token

    issues = jira_issues(
        **CREDENTIALS,
        backend="pyarrow",
        chunk_size=3,
        bulk_fetch_size=2,
        search_page_size=2,
    )
    pages = list(issues)

    # ids found by search are fetched in pages of `chunk_size`, pages are fetched in parallel
    assert sorted(page.num_rows for page in pages) == [2, 3]
    assert sorted(row["id"] for row in to_rows(pages)) == ["1", "2", "3", "4", "5"]
    assert [search.get("nextPageToken") for search in jira_mock.searches] == [
        None,
        "1",
        "2",
    ]
    assert len({search["jql"] for search in jira_mock.searches}) == 1
    assert {search["maxResults"] for search in jira_mock.searches} == {"2"}
    # each page is fetched in batches of `bulk_fetch_size`
    assert sorted(fetch["issueIdsOrKeys"] for fetch in jira_mock.bulk_fetches) == [
        ["1", "2"],
        ["3"],
        ["4", "5"],
    ]


@pytest.mark.parametrize(
    "included_columns,max_bulk_issues,expected_batches,expected_pages",
    [
        (["summary"], None, [50, 1000], [50, 1000]),
        (None, None, [50] + [100] * 10, [50] + [200] * 5),
        (["summary"], 100, [50] + [100] * 10 + [1000], [50, 1000]),
    ],
    ids=["single_value_fields", "multi_value_fields", "large_batches_rejected"],
)
def test_issues_bulk_fetch_batches(
    jira_mock: JiraMock,
    included_columns: Optional[List[str]],
    max_bulk_issues: Optional[int],
    expected_batches: List[int],
    expected_pages: List[int],
) -> None:
    jira_mock.add_issues(
        *(make_issue(issue_id, UPDATED_1) for issue_id in range(1, 1051))
    )
    jira_mock.max_bulk_issues = max_bulk_issues

    pages = list(
        jira_issues(**CREDENTIALS, backend="pyarrow", included_columns=included_columns)
    )

    # a page is fetched with a single request when possible, at least 200 issues per page
    assert sorted(page.num_rows for page in pages) == expected_pages
    # pages are fetched in parallel, each in batches
    assert (
        sorted(len(fetch["issueIdsOrKeys"]) for fetch in jira_mock.bulk_fetches)
        == expected_batches
    )


def test_issues_limit_backfill(jira_mock: JiraMock) -> None:
    # issues updated an hour apart are read in pages of 2
    jira_mock.add_issues(
        *(make_issue(n, f"2024-01-15T1{n}:00:00.000+0000") for n in range(1, 6))
    )
    pipeline = dlt.pipeline("jira_issues_backfill", destination="duckdb", dev_mode=True)
    loaded = []
    for _ in range(3):
        issues = jira_issues(**CREDENTIALS, chunk_size=2, lag=0)
        # transformers process all pages they receive, the limit is set on the resource with ids
        issues._parent.add_limit(1)
        assert_load_info(pipeline.run(issues))
        rows = load_tables_to_dicts(pipeline, "issues")["issues"]
        loaded.append(sorted(row["id"] for row in rows))

    # each run loads the next page of issues and continues from its last update
    assert loaded == [["1", "2"], ["1", "2", "3", "4"], ["1", "2", "3", "4", "5"]]
    assert jira_mock.jqls == [
        'created >= "1970-01-01" ORDER BY updated ASC',
        'updated >= "2024-01-15 12:00" ORDER BY updated ASC',
        'updated >= "2024-01-15 14:00" ORDER BY updated ASC',
    ]


def test_issues_empty(jira_mock: JiraMock) -> None:
    issues = jira_issues(**CREDENTIALS)
    assert to_rows(list(issues | issue_changelogs(**CREDENTIALS))) == []
    assert to_rows(list(issues | issue_comments)) == []
    assert len(jira_mock.searches) == 2
    assert jira_mock.bulk_fetches == []
    assert jira_mock.changelog_fetches == []
    assert jira_mock.comment_fetches == []


def test_issues_deleted_after_search(jira_mock: JiraMock) -> None:
    jira_mock.add_issues(make_issue(1, UPDATED_1), make_issue(3, UPDATED_1))
    jira_mock.search_pages = [["1", "2", "3"]]

    # the deleted issue has no cursor value in the mocked search results
    rows = to_rows(list(jira_issues(**CREDENTIALS, incremental=None)))

    assert [row["id"] for row in rows] == ["1", "3"]


@pytest.mark.parametrize("status_code", [400, 401, 403, 404])
def test_issues_api_errors(http_mock: requests_mock.Mocker, status_code: int) -> None:
    JiraMock(http_mock)
    http_mock.get(
        BASE_URL + "rest/api/2/search/jql",
        status_code=status_code,
        json={"errorMessages": ["Field 'colour' does not exist"], "errors": {}},
    )
    with pytest.raises(ResourceExtractionError) as py_ex:
        list(jira_issues(**CREDENTIALS))
    http_ex = py_ex.value.__cause__
    assert isinstance(http_ex, HTTPError)
    assert http_ex.response.status_code == status_code
    assert "Field 'colour' does not exist" in str(http_ex)


def test_issues_retries_throttling(http_mock: requests_mock.Mocker) -> None:
    jira_mock = JiraMock(http_mock)
    jira_mock.add_issues(make_issue(1, UPDATED_1))
    http_mock.post(
        BASE_URL + "rest/api/2/issue/bulkfetch",
        [
            {"status_code": 429, "headers": {"Retry-After": "0"}},
            {"status_code": 503, "headers": {"Retry-After": "0"}},
            {"json": {"issues": [make_issue(1, UPDATED_1)], "issueErrors": []}},
        ],
    )
    rows = to_rows(list(jira_issues(**CREDENTIALS)))
    assert [row["id"] for row in rows] == ["1"]


@pytest.mark.parametrize("backend", ["python", "pyarrow"])
def test_issues_unexpected_values(jira_mock: JiraMock, backend: str) -> None:
    issue_without_project = make_issue(1, UPDATED_1)
    del issue_without_project["fields"]["project"]
    jira_mock.add_issues(
        issue_without_project,
        make_issue(2, UPDATED_1, project=None),
        make_issue(
            3, UPDATED_1, customfield_10030="legacy", customfield_10016={"value": 1}
        ),
    )

    rows = to_rows(list(jira_issues(**CREDENTIALS, backend=backend)))

    # the python backend skips empty fields
    assert [row.get("project") for row in rows] == [None, None, rows[2]["project"]]
    assert rows[2]["project"] is not None
    # values of unexpected structure are loaded as null
    assert rows[2].get("customfield_10030") is None
    assert rows[2].get("customfield_10016") is None
    assert rows[0]["customfield_10030"] is not None


def test_issues_incremental_runs(jira_mock: JiraMock) -> None:
    jira_mock.add_issues(
        make_issue(1, "2024-01-15T10:30:00.000+0000"),
        make_issue(2, "2024-01-16T08:00:00.000+0000"),
    )
    pipeline = dlt.pipeline(
        "jira_issues_incremental", destination="duckdb", dev_mode=True
    )
    assert_load_info(pipeline.run(jira_issues(**CREDENTIALS)))
    table = pipeline.default_schema.get_table("issues")
    assert table["write_disposition"] == "merge"
    columns = pipeline.default_schema.get_table_columns("issues")
    assert columns["id"]["primary_key"] is True
    assert columns["updated"]["dedup_sort"] == "desc"

    # issue 2 was updated within lag and is read again, issue 1 was updated
    jira_mock.add_issues(
        make_issue(1, "2024-01-17T12:00:45.000+0000", summary="changed")
    )
    assert_load_info(pipeline.run(jira_issues(**CREDENTIALS)))

    assert jira_mock.jqls == [
        'created >= "1970-01-01" ORDER BY updated ASC',
        # the last value minus 10 minutes of lag
        'updated >= "2024-01-16 07:50" ORDER BY updated ASC',
    ]
    rows = sorted(
        load_tables_to_dicts(pipeline, "issues")["issues"], key=lambda r: r["id"]
    )
    assert [(row["id"], row["summary"]) for row in rows] == [
        ("1", "changed"),
        ("2", "Issue 2"),
    ]


@pytest.mark.parametrize(
    "incremental,expected_write_disposition",
    [
        ("auto", "merge"),
        (dlt.sources.incremental("created"), "merge"),
        (None, "replace"),
    ],
    ids=["auto", "explicit", "no_incremental"],
)
def test_issues_write_disposition(
    incremental: Any, expected_write_disposition: str
) -> None:
    resource = jira_issues(**CREDENTIALS, incremental=incremental)
    assert resource.write_disposition == expected_write_disposition


@pytest.mark.parametrize(
    "included_columns,excluded_columns,expected_fields",
    [
        (["summary", "status", "colour"], None, ["summary", "status", "updated"]),
        (["summary", "updated", "comment"], None, ["summary", "updated", "comment"]),
        (
            None,
            ["description", "customfield_10050"],
            [
                field_id
                for field_id in DEFAULT_FIELD_IDS
                if field_id not in ("description", "customfield_10050")
            ],
        ),
        (None, None, DEFAULT_FIELD_IDS),
    ],
    ids=["included", "included_comments", "excluded", "default"],
)
def test_issues_columns(
    jira_mock: JiraMock,
    included_columns: Optional[List[str]],
    excluded_columns: Optional[List[str]],
    expected_fields: List[str],
) -> None:
    jira_mock.add_issues(make_issue(1, UPDATED_1))
    rows = to_rows(
        list(
            jira_issues(
                **CREDENTIALS,
                included_columns=included_columns,
                excluded_columns=excluded_columns,
                # keeps empty fields
                backend="pyarrow",
            )
        )
    )
    assert jira_mock.bulk_fetches[0]["fields"] == expected_fields
    assert list(rows[0]) == ["id", "key", *expected_fields]


def test_issues_user_column_hints(jira_mock: JiraMock) -> None:
    jira_mock.add_issues(make_issue(1, UPDATED_1))
    issues = jira_issues(**CREDENTIALS)
    issues.apply_hints(columns={"customfield_10016": {"data_type": "text"}})
    pipeline = dlt.pipeline("jira_issues_hints", destination="duckdb", dev_mode=True)
    assert_load_info(pipeline.run(issues))
    columns = pipeline.default_schema.get_table_columns("issues")
    assert columns["customfield_10016"]["data_type"] == "text"
    assert columns["updated"]["data_type"] == "timestamp"


@pytest.mark.parametrize("backend", ["python", "pyarrow"])
def test_issue_changelogs(jira_mock: JiraMock, backend: str) -> None:
    jira_mock.add_issues(*(make_issue(issue_id, UPDATED_1) for issue_id in (1, 2, 3)))
    jira_mock.histories = {
        "1": [make_history(history_id, UPDATED_1) for history_id in (100, 101, 102)],
        "3": [
            make_history(300, UPDATED_2, field="assignee"),
            make_history(301, UPDATED_2),
        ],
    }
    jira_mock.changelog_page_size = 2
    issues = jira_issues(**CREDENTIALS, backend=backend)
    pipeline = dlt.pipeline(
        f"jira_changelogs_{backend}", destination="duckdb", dev_mode=True
    )
    assert_load_info(pipeline.run([issues, issues | issue_changelogs(**CREDENTIALS)]))

    assert [fetch.get("nextPageToken") for fetch in jira_mock.changelog_fetches] == [
        None,
        "2",
        "4",
    ]
    assert jira_mock.changelog_fetches[0]["issueIdsOrKeys"] == ["1", "2", "3"]
    assert load_table_counts(
        pipeline, "issue_changelogs", "issue_changelogs__items"
    ) == {
        "issue_changelogs": 5,
        "issue_changelogs__items": 5,
    }
    table = pipeline.default_schema.get_table("issue_changelogs")
    assert table["write_disposition"] == "merge"
    assert table["columns"]["created"]["data_type"] == "timestamp"
    rows = sorted(
        load_tables_to_dicts(pipeline, "issue_changelogs")["issue_changelogs"],
        key=lambda r: r["id"],
    )
    assert [(row["id"], row["issue_id"]) for row in rows] == [
        ("100", "1"),
        ("101", "1"),
        ("102", "1"),
        ("300", "3"),
        ("301", "3"),
    ]
    assert rows[3]["author__account_id"] == "a1"
    assert rows[3]["created"] == datetime(2024, 1, 16, 16, 0, tzinfo=timezone.utc)


def test_changelogs_of_many_issues(jira_mock: JiraMock) -> None:
    issues = [{"id": str(issue_id)} for issue_id in range(1001)]
    client = JiraClient(**CREDENTIALS)
    assert list(changelog_rows(client, issues)) == []
    assert [len(fetch["issueIdsOrKeys"]) for fetch in jira_mock.changelog_fetches] == [
        1000,
        1,
    ]


VISIBILITY = {"type": "role", "value": "Administrators", "identifier": "10002"}


@pytest.mark.parametrize("backend", ["python", "pyarrow"])
def test_issue_comments(jira_mock: JiraMock, backend: str) -> None:
    jira_mock.add_issues(*(make_issue(issue_id, UPDATED_1) for issue_id in (1, 2, 3)))
    jira_mock.comments = {
        "1": [
            make_comment(10, UPDATED_1),
            make_comment(11, UPDATED_2, visibility=VISIBILITY),
        ],
        "3": [make_comment(comment_id, UPDATED_1) for comment_id in range(30, 35)],
    }
    # issue 3 has more comments than embedded in the issue
    jira_mock.embedded_comments = 2
    jira_mock.comment_page_size = 2
    issues = jira_issues(**CREDENTIALS, backend=backend)
    pipeline = dlt.pipeline(
        f"jira_comments_{backend}", destination="duckdb", dev_mode=True
    )
    assert_load_info(pipeline.run([issues, issues | issue_comments]))

    # comments are fetched with the issues
    assert [fetch["fields"] for fetch in jira_mock.bulk_fetches] == [
        [*DEFAULT_FIELD_IDS, "comment"]
    ]
    assert "comment" not in pipeline.default_schema.get_table_columns("issues")
    # comments of issue 3 are paged
    assert [
        (fetch["issueId"], fetch["startAt"]) for fetch in jira_mock.comment_fetches
    ] == [("3", "0"), ("3", "2"), ("3", "4")]
    table = pipeline.default_schema.get_table("issue_comments")
    assert table["write_disposition"] == "merge"
    columns = table["columns"]
    assert columns["id"]["primary_key"] is True
    assert columns["issue_id"]["merge_key"] is True
    assert columns["created"]["data_type"] == "timestamp"
    assert columns["updated"]["dedup_sort"] == "desc"
    assert columns["deleted"]["hard_delete"] is True
    rows = sorted(
        load_tables_to_dicts(pipeline, "issue_comments")["issue_comments"],
        key=lambda r: int(r["id"]),
    )
    # the first run does not delete comments of issues without comments
    assert [(row["id"], row["issue_id"]) for row in rows] == [
        ("10", "1"),
        ("11", "1"),
        *((str(comment_id), "3") for comment_id in range(30, 35)),
    ]
    assert rows[1]["updated"] == datetime(2024, 1, 16, 16, 0, tzinfo=timezone.utc)
    assert rows[1]["body"] == "comment 11 with *wiki markup*"
    assert rows[1]["jsd_public"] is True
    if backend == "python":
        assert rows[1]["author__account_id"] == "a1"
        assert rows[1]["visibility__value"] == "Administrators"
        assert rows[0]["visibility__value"] is None
    else:
        assert columns["visibility"]["data_type"] == "json"
        with pipeline.sql_client() as client:
            visibility = client.execute_sql(
                "SELECT visibility FROM issue_comments WHERE id = '11'"
            )[0][0]
        assert json.loads(visibility) == VISIBILITY


def test_issue_comments_nested_types(jira_mock: JiraMock) -> None:
    jira_mock.add_issues(make_issue(1, UPDATED_1))
    jira_mock.comments = {"1": [make_comment(10, UPDATED_1, visibility=VISIBILITY)]}
    issues = jira_issues(**CREDENTIALS, backend="pyarrow", nested_types=True)
    _, table = run_to_parquet(
        "jira_comments_nested", issues | issue_comments, "issue_comments"
    )

    assert str(table.schema.field("visibility").type) == (
        "struct<type: string, value: string, identifier: string>"
    )
    assert table.schema.field("author").type.get_field_index("accountId") == 0
    assert table.to_pylist()[0]["visibility"] == VISIBILITY


@pytest.mark.parametrize("backend", ["python", "pyarrow"])
def test_issue_comments_captured(http_mock: requests_mock.Mocker, backend: str) -> None:
    captured = load_captured()
    jira_mock = JiraMock(http_mock)
    jira_mock.add_issues(
        *(make_issue(int(issue_id), UPDATED_1) for issue_id in captured["comments"])
    )
    jira_mock.comments = captured["comments"]
    pipeline = dlt.pipeline(
        f"jira_comments_captured_{backend}", destination="duckdb", dev_mode=True
    )
    issues = jira_issues(**CREDENTIALS, backend=backend)
    assert_load_info(pipeline.run(issues | issue_comments))

    rows = sorted(
        load_tables_to_dicts(pipeline, "issue_comments")["issue_comments"],
        key=lambda r: r["id"],
    )
    assert [(row["id"], row["issue_id"]) for row in rows] == [
        ("10009", "10293"),
        ("10010", "10293"),
    ]
    # wiki markup body of api v2
    assert rows[0]["body"] == (
        "h2. Seeded\n*bold* comment with emoji 🚀, ünïcode and a\nnew line"
    )
    # the edited comment
    assert rows[1]["body"] == "after edit"
    assert rows[1]["updated"] > rows[1]["created"]
    assert rows[1]["created"] == datetime(
        2026, 10, 27, 1, 38, 39, 945000, tzinfo=timezone.utc
    )
    if backend == "python":
        assert rows[0]["author__account_id"] == "account-1"
        assert rows[0]["update_author__display_name"] == "User account-1"
    else:
        assert json.loads(rows[0]["author"])["accountId"] == "account-1"


@pytest.mark.parametrize(
    "resources,expected_fields",
    [
        (["issues"], [DEFAULT_FIELD_IDS]),
        (["issue_comments"], [["comment"]]),
        (["issues", "issue_comments"], [[*DEFAULT_FIELD_IDS, "comment"]]),
        (["issue_changelogs"], []),
        (["issue_comments", "issue_changelogs"], [["comment"]]),
    ],
    ids=[
        "issues",
        "comments",
        "issues_and_comments",
        "changelogs",
        "comments_changelogs",
    ],
)
def test_issues_fetch_selected_resources(
    jira_mock: JiraMock, resources: List[str], expected_fields: List[List[str]]
) -> None:
    jira_mock.add_issues(*(make_issue(issue_id, UPDATED_1) for issue_id in (1, 2)))
    jira_mock.comments = {"1": [make_comment(10, UPDATED_1)]}
    jira_mock.histories = {"2": [make_history(200, UPDATED_1)]}
    pipeline = dlt.pipeline(
        "jira_selected_resources", destination="duckdb", dev_mode=True
    )
    assert_load_info(pipeline.run(jira(**CREDENTIALS).with_resources(*resources)))

    # selected resources are loaded with a single request per batch of issues
    assert [fetch["fields"] for fetch in jira_mock.bulk_fetches] == expected_fields
    row_counts = pipeline.last_trace.last_normalize_info.row_counts
    assert {name for name in row_counts if "__" not in name} == {
        "_dlt_pipeline_state",
        *resources,
    }


def test_issue_comments_renamed(jira_mock: JiraMock) -> None:
    jira_mock.add_issues(make_issue(1, UPDATED_1))
    jira_mock.comments = {"1": [make_comment(10, UPDATED_1)]}
    issues = jira_issues(**CREDENTIALS)
    comments = issues | issue_comments.with_name("comments")

    rows = to_rows(list(comments))

    # comments are fetched separately when the transformer is renamed
    assert [fetch["fields"] for fetch in jira_mock.bulk_fetches] == [
        DEFAULT_FIELD_IDS,
        ["comment"],
    ]
    assert [row["id"] for row in rows] == ["10"]


def test_issue_comments_not_from_issues() -> None:
    with pytest.raises(ResourceExtractionError) as py_ex:
        list(dlt.resource([{"id": "1"}], name="other") | issue_comments)
    assert isinstance(py_ex.value.__cause__, ValueError)


@pytest.mark.parametrize("backend", ["python", "pyarrow"])
def test_issue_comments_changed(jira_mock: JiraMock, backend: str) -> None:
    jira_mock.add_issues(*(make_issue(issue_id, UPDATED_1) for issue_id in (1, 2, 3)))
    jira_mock.comments = {
        "1": [make_comment(10, UPDATED_1), make_comment(11, UPDATED_1)],
        "2": [make_comment(20, UPDATED_1)],
    }
    pipeline = dlt.pipeline(
        f"jira_comments_changed_{backend}", destination="duckdb", dev_mode=True
    )

    def load() -> Dict[str, Optional[str]]:
        issues = jira_issues(**CREDENTIALS, backend=backend)
        assert_load_info(pipeline.run(issues | issue_comments))
        rows = load_tables_to_dicts(pipeline, "issue_comments")["issue_comments"]
        return {row["id"]: row["body"] for row in rows}

    assert load() == {
        "10": "comment 10 with *wiki markup*",
        "11": "comment 11 with *wiki markup*",
        "20": "comment 20 with *wiki markup*",
    }
    assert pipeline.last_trace.last_normalize_info.row_counts["issue_comments"] == 3

    # comment 10 is edited, comment 11 is deleted, issue 2 has no comments left and issue 3 gets
    # a comment, issues are updated within lag and read again
    jira_mock.comments = {
        "1": [make_comment(10, UPDATED_2, body="edited")],
        "3": [make_comment(30, UPDATED_2)],
    }
    assert load() == {"10": "edited", "30": "comment 30 with *wiki markup*"}
    # issue 2 without comments deletes its comments
    assert pipeline.last_trace.last_normalize_info.row_counts["issue_comments"] == 3


def test_worklogs(jira_mock: JiraMock) -> None:
    jira_mock.worklogs = {
        str(worklog_id): make_worklog(worklog_id, 1, T1 + worklog_id * 60000)
        for worklog_id in (1, 2, 3)
    }
    jira_mock.worklog_page_size = 2
    pipeline = dlt.pipeline("jira_worklogs", destination="duckdb", dev_mode=True)
    assert_load_info(pipeline.run(worklogs(**CREDENTIALS)))

    # the next page starts at the youngest worklog of the previous page
    assert jira_mock.worklog_feeds == [
        {"feed": "updated", "since": 0},
        {"feed": "updated", "since": T1 + 2 * 60000},
        {"feed": "deleted", "since": 0},
    ]
    assert jira_mock.worklog_lists == [[1, 2], [2, 3]]
    assert load_table_counts(pipeline, "worklogs") == {"worklogs": 3}
    columns = pipeline.default_schema.get_table_columns("worklogs")
    assert columns["updated"]["data_type"] == "timestamp"
    assert columns["updated"]["dedup_sort"] == "desc"
    assert columns["time_spent_seconds"]["data_type"] == "bigint"
    assert columns["deleted"]["hard_delete"] is True

    # worklog 2 is deleted, the next run starts at the last update minus 10 minutes of lag
    jira_mock.worklog_feeds.clear()
    del jira_mock.worklogs["2"]
    jira_mock.deleted_worklogs = [{"worklogId": 2, "updatedTime": T1 + 4 * 60000}]
    assert_load_info(pipeline.run(worklogs(**CREDENTIALS)))
    since = T1 + 3 * 60000 - 600000
    assert jira_mock.worklog_feeds == [
        {"feed": "updated", "since": since},
        {"feed": "deleted", "since": since},
    ]
    rows = load_tables_to_dicts(pipeline, "worklogs")["worklogs"]
    assert sorted(row["id"] for row in rows) == ["1", "3"]


def test_worklogs_many_ids(jira_mock: JiraMock) -> None:
    jira_mock.worklogs = {
        str(worklog_id): make_worklog(worklog_id, 1, T1 + worklog_id)
        for worklog_id in range(1, 1002)
    }
    jira_mock.worklog_page_size = 2000
    assert len(to_rows(list(worklogs(**CREDENTIALS)))) == 1001
    assert [len(ids) for ids in jira_mock.worklog_lists] == [1000, 1]


def test_worklogs_page_does_not_advance(jira_mock: JiraMock) -> None:
    jira_mock.worklogs = {
        str(worklog_id): make_worklog(worklog_id, 1, T1) for worklog_id in (1, 2, 3)
    }
    jira_mock.worklog_page_size = 2
    with pytest.raises(ResourceExtractionError) as py_ex:
        list(worklogs(**CREDENTIALS))
    assert isinstance(py_ex.value.__cause__, RuntimeError)


def test_worklogs_empty(jira_mock: JiraMock) -> None:
    assert list(worklogs(**CREDENTIALS)) == []
    assert jira_mock.worklog_lists == []


# runs in a fresh interpreter in which pyarrow cannot be imported
NO_PYARROW_SCRIPT = textwrap.dedent(
    """
    import sys

    sys.modules["pyarrow"] = None

    import dlt
    import requests_mock
    from dlt.common.exceptions import MissingDependencyException

    from sources.jira import issue_changelogs, issue_comments, jira_issues, worklogs
    from tests.jira.utils import (
        CREDENTIALS,
        JiraMock,
        make_comment,
        make_history,
        make_issue,
        make_worklog,
    )

    work_dir = sys.argv[1]
    with requests_mock.Mocker() as m:
        jira_mock = JiraMock(m)
        jira_mock.add_issues(make_issue(1, "2024-01-15T10:30:00.123+0100"))
        jira_mock.histories["1"] = [make_history(100, "2024-01-15T10:30:00.000+0100")]
        jira_mock.comments["1"] = [make_comment(10, "2024-01-15T10:30:00.000+0100")]
        jira_mock.worklogs = {"7": make_worklog(7, 1, 1704103200000)}
        pipeline = dlt.pipeline(
            "jira_no_pyarrow",
            pipelines_dir=work_dir,
            destination=dlt.destinations.duckdb(f"{work_dir}/jira.duckdb"),
        )
        # nested types are ignored by the python backend
        issues = jira_issues(**CREDENTIALS, nested_types=True)
        info = pipeline.run(
            [
                issues,
                issues | issue_comments,
                issues | issue_changelogs(**CREDENTIALS),
                worklogs(**CREDENTIALS),
            ]
        )
        assert not info.has_failed_jobs
        print(sorted(pipeline.last_trace.last_normalize_info.row_counts.items()))
        try:
            jira_issues(**CREDENTIALS, backend="pyarrow")
        except MissingDependencyException:
            print("pyarrow backend requires pyarrow")
"""
)


def test_python_backend_without_pyarrow(tmp_path: Any) -> None:
    root_dir = os.path.dirname(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    )
    result = subprocess.run(
        [sys.executable, "-c", NO_PYARROW_SCRIPT, str(tmp_path)],
        cwd=root_dir,
        env={**os.environ, "PYTHONPATH": root_dir},
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.splitlines() == [
        "[('_dlt_pipeline_state', 1), ('issue_changelogs', 1),"
        " ('issue_changelogs__items', 1), ('issue_comments', 1), ('issues', 1),"
        " ('issues__customfield_10031', 1), ('issues__fix_versions', 1), ('issues__labels', 2),"
        " ('worklogs', 1)]",
        "pyarrow backend requires pyarrow",
    ]


def test_jira_source(http_mock: requests_mock.Mocker) -> None:
    # creating the source does not call the API: no routes are mocked
    source = jira(**CREDENTIALS)
    assert list(source.resources) == [
        "issues",
        "issue_comments",
        "issue_changelogs",
        "worklogs",
        "users",
        "workflows",
        "workflow_statuses",
        "projects",
    ]
    write_dispositions = {
        name: resource.write_disposition for name, resource in source.resources.items()
    }
    assert write_dispositions == {
        "issues": "merge",
        "issue_comments": "merge",
        "issue_changelogs": "merge",
        "worklogs": "merge",
        "users": "replace",
        "workflows": "replace",
        "workflow_statuses": "replace",
        "projects": "replace",
    }

    http_mock.get(
        BASE_URL + "rest/api/3/workflows/search",
        [
            {
                "json": {
                    "values": [{"id": {"entityId": "w1"}, "name": "Flow"}],
                    "isLast": True,
                }
            },
            {"json": {"values": [], "isLast": True}},
        ],
    )
    rows = to_rows(list(source.with_resources("workflows")))
    assert [row["name"] for row in rows] == ["Flow"]

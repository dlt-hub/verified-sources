from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, Optional

import dlt
import pytest
from pytest_mock import MockerFixture

from sources.servicenow import servicenow_table
from sources.servicenow.helpers.client import ServiceNowClient
from sources.servicenow.helpers.credentials import BasicCredentials
from sources.servicenow.helpers.schema import (
    LazyDictionary,
    descendant_tables,
    dictionary_to_columns,
    internal_type_to_column,
    table_chain,
)

from tests.servicenow.utils import (
    INCIDENT_FIELDS,
    TASK_FIELDS,
    FakeInstance,
    add_task_tables,
    make_records,
    updated_on,
)


TYPE_CASES = [
    ("string", {"data_type": "text"}),
    ("GUID", {"data_type": "text"}),
    ("reference", {"data_type": "text"}),
    ("choice", {"data_type": "text"}),
    ("html", {"data_type": "text"}),
    ("sys_class_name", {"data_type": "text"}),
    ("boolean", {"data_type": "bool"}),
    ("integer", {"data_type": "bigint"}),
    ("longint", {"data_type": "bigint"}),
    ("decimal", {"data_type": "decimal"}),
    ("currency", {"data_type": "decimal"}),
    ("price", {"data_type": "decimal"}),
    ("float", {"data_type": "double"}),
    ("glide_date_time", {"data_type": "timestamp", "timezone": True}),
    ("due_date", {"data_type": "timestamp", "timezone": True}),
    ("glide_date", {"data_type": "date"}),
    ("glide_time", {"data_type": "time", "precision": 0}),
    ("glide_duration", {"data_type": "bigint"}),
    ("timer", {"data_type": "bigint"}),
    # types added in the future are loaded as text
    ("new_type", {"data_type": "text"}),
    ("journal", None),
    ("journal_input", None),
    ("password", None),
    ("password2", None),
]


@pytest.mark.parametrize(
    "internal_type,expected", TYPE_CASES, ids=[case[0] for case in TYPE_CASES]
)
def test_internal_type_to_column(
    internal_type: str, expected: Optional[Dict[str, Any]]
) -> None:
    column = internal_type_to_column("field", internal_type)
    assert column == (None if expected is None else {"name": "field", **expected})


def test_dictionary_to_columns() -> None:
    tables = {
        "task": "",
        "incident": "task",
        "u_incident": "incident",
        "change_request": "task",
    }
    dictionary = [
        {"name": name, "element": element, "internal_type": type_}
        for name, fields in (("task", TASK_FIELDS), ("incident", INCIDENT_FIELDS))
        for element, type_ in fields.items()
    ]
    chain = table_chain(tables, "incident")
    assert chain == ["incident", "task"]
    columns, durations = dictionary_to_columns(dictionary, chain)
    assert list(columns) == [
        "sys_id",
        "number",
        "active",
        "priority",
        "sys_updated_on",
        "sys_created_on",
        "sys_class_name",
        "business_duration",
        "caller_id",
        "u_cost",
        "u_score",
        "u_due",
        "u_start",
    ]
    assert columns["number"]["data_type"] == "bigint"
    assert durations == {"business_duration"}
    # parent fields only
    columns, _ = dictionary_to_columns(dictionary, table_chain(tables, "task"))
    assert columns["number"]["data_type"] == "text" and "caller_id" not in columns
    columns, durations = dictionary_to_columns(
        dictionary, chain, included_columns=["sys_id", "u_cost"]
    )
    assert list(columns) == ["sys_id", "u_cost"] and durations == set()
    columns, _ = dictionary_to_columns(
        dictionary, chain, excluded_columns=["caller_id"]
    )
    assert "caller_id" not in columns

    assert sorted(descendant_tables(tables, "task")) == [
        "change_request",
        "incident",
        "u_incident",
    ]
    assert descendant_tables(tables, "u_incident") == []
    # cycles in `super_class` do not loop
    assert table_chain({"a": "b", "b": "a"}, "a") == ["a", "b"]
    with pytest.raises(ValueError, match="not in `sys_db_object`"):
        table_chain(tables, "incident_view")


def test_servicenow_table_dispositions(
    instance: FakeInstance, credentials: BasicCredentials
) -> None:
    instance.add_table(
        "sys_journal_field",
        {"sys_id": "GUID", "value": "string", "sys_created_on": "glide_date_time"},
    )
    instance.add_table("cmn_schedule", {"sys_id": "GUID", "name": "string"})
    instance.tables["sys_journal_field"] = make_records(2, updated_on)
    pipeline = dlt.pipeline(pipeline_name="servicenow_dispositions", dev_mode=True)
    for _ in range(2):
        journal = servicenow_table(credentials=credentials, table="sys_journal_field")
        pipeline.extract(journal)
    # insert-only tables are appended without a lag and without deletes
    assert journal.write_disposition == "append"
    assert instance.queries("sys_journal_field")[-2].startswith(
        "sys_created_on>=2024-01-01 00:00:00^sys_created_on<2024-03-01 00:00:00^"
    )
    assert "is_deleted" not in journal.compute_table_schema()["columns"]
    assert (
        servicenow_table(
            credentials=credentials, table="cmn_schedule"
        ).write_disposition
        == "replace"
    )
    add_task_tables(instance)
    replace = servicenow_table(
        credentials=credentials, table="incident", write_disposition="replace"
    )
    pipeline.extract(replace)
    # the full table is read, without a window
    assert replace.write_disposition == "replace"
    assert instance.queries("incident") == ["ORDERBYsys_id"]


def test_lazy_dictionary(
    instance: FakeInstance, credentials: BasicCredentials, mocker: MockerFixture
) -> None:
    add_task_tables(instance)
    client = ServiceNowClient(credentials)
    dictionary = mocker.spy(client, "dictionary")
    lazy = LazyDictionary(client, ["incident", "u_missing"])
    assert instance.requests == []
    # resources of a source call it from the extract worker threads, one of them reads
    with ThreadPoolExecutor(8) as pool:
        results = list(pool.map(lambda _: lazy(), range(8)))
    assert dictionary.call_count == 1
    # unknown tables are skipped, the tables that `incident` extends are read
    assert dictionary.call_args.args[0] == ["incident", "task"]
    assert all(result is results[0] for result in results)
    assert {entry["name"] for entry in results[0]} == {"incident", "task"}

    # a failed read raises in every resource and is not repeated
    failing = mocker.patch.object(
        client, "dictionary", side_effect=ValueError("no access")
    )
    lazy = LazyDictionary(client, ["incident"])
    for _ in range(3):
        with pytest.raises(ValueError, match="no access"):
            lazy()
    assert failing.call_count == 1

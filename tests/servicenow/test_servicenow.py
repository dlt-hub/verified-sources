import os
from typing import Any, Dict, List, Tuple

import dlt
import pytest
from dlt.pipeline.exceptions import PipelineStepFailed

from sources.servicenow import DEFAULT_TABLES, servicenow_source, servicenow_table
from sources.servicenow.helpers.credentials import ApiKeyCredentials, BasicCredentials

from tests.servicenow.utils import (
    INSTANCE,
    FakeInstance,
    add_task_tables,
    make_records,
    updated_on,
)
from tests.utils import assert_load_info, load_table_counts


def test_servicenow_source(
    instance: FakeInstance, credentials: BasicCredentials
) -> None:
    for table in DEFAULT_TABLES:
        instance.add_table(
            table,
            {"sys_id": "GUID", "name": "string", "sys_updated_on": "glide_date_time"},
            records=make_records(2, updated_on, name=table),
        )
    instance.tables["sys_audit_delete"] = []
    source = servicenow_source(
        credentials=credentials,
        encoded_queries={"incident": "name=incident"},
    )
    assert list(source.resources) == list(DEFAULT_TABLES)
    # tables and fields of all tables are read with a page each, the empty page that follows is
    # confirmed with a count
    assert [name for name, _, _ in instance.requests] == [
        "sys_db_object",
        "sys_db_object",
        "sys_db_object",
        "sys_dictionary",
        "sys_dictionary",
        "sys_dictionary",
    ]
    for resource in source.resources.values():
        assert resource.write_disposition == "merge"
        assert (
            resource.compute_table_schema()["columns"]["sys_id"]["primary_key"] is True
        )

    pipeline = dlt.pipeline(
        pipeline_name="servicenow_source", destination="duckdb", dev_mode=True
    )
    assert_load_info(pipeline.run(source))
    assert load_table_counts(pipeline, *DEFAULT_TABLES) == {
        name: 2 for name in DEFAULT_TABLES
    }
    # the user query is added to the incident query only
    filtered = [
        name
        for name, params, _ in instance.requests
        if "name=incident" in params.get("sysparm_query", "")
    ]
    assert filtered == ["incident"] * 3


def test_servicenow_source_deferred(
    instance: FakeInstance, credentials: BasicCredentials
) -> None:
    for table in DEFAULT_TABLES:
        instance.add_table(
            table,
            {"sys_id": "GUID", "name": "string", "sys_updated_on": "glide_date_time"},
            records=make_records(2, updated_on, name=table),
        )
    instance.tables["sys_audit_delete"] = []
    source = servicenow_source(credentials=credentials, defer_table_reflect=True)
    # creating the source makes no requests
    assert instance.requests == []
    pipeline = dlt.pipeline(
        pipeline_name="servicenow_source_deferred", destination="duckdb", dev_mode=True
    )
    assert_load_info(pipeline.run(source))
    assert load_table_counts(pipeline, *DEFAULT_TABLES) == {
        name: 2 for name in DEFAULT_TABLES
    }
    # the resources share one read of the fields of all tables
    dictionary = [name for name, _, _ in instance.requests if name == "sys_dictionary"]
    assert len(dictionary) == 3
    columns = pipeline.default_schema.get_table_columns("incident")
    assert columns["sys_updated_on"]["data_type"] == "timestamp"
    assert columns["is_deleted"]["hard_delete"] is True

    # an unknown table fails in its own resource
    source = servicenow_source(
        credentials=credentials,
        tables=["incident", "u_missing"],
        defer_table_reflect=True,
    )
    with pytest.raises(PipelineStepFailed, match="not in `sys_db_object`"):
        pipeline.run(source)


def test_servicenow_source_start_date(
    instance: FakeInstance, credentials: BasicCredentials
) -> None:
    os.environ["SOURCES__SERVICENOW__START_DATE"] = "2024-02-02"
    tables = ["incident", "sys_user"]
    for table in tables:
        instance.add_table(
            table,
            {"sys_id": "GUID", "sys_updated_on": "glide_date_time"},
            records=make_records(30, updated_on),
        )
    instance.tables["sys_audit_delete"] = []
    source = servicenow_source(credentials=credentials, tables=tables)
    assert {resource.section for resource in source.resources.values()} == {
        "servicenow"
    }
    pipeline = dlt.pipeline(
        pipeline_name="servicenow_source_start", destination="duckdb", dev_mode=True
    )
    assert_load_info(pipeline.run(source))
    assert load_table_counts(pipeline, *tables) == {table: 6 for table in tables}


def test_servicenow_invalid_arguments(
    instance: FakeInstance, credentials: BasicCredentials
) -> None:
    add_task_tables(instance)
    instance.add_table("cmn_schedule", {"sys_id": "GUID", "name": "string"})
    instance.add_table("u_without_key", {"name": "string"})
    api_key = ApiKeyCredentials(instance=INSTANCE, api_key="key")
    api_key.resolve()
    cases: List[Tuple[Dict[str, Any], str]] = [
        (
            {
                "start_date": "2024-02-02",
                "incremental": dlt.sources.incremental("sys_created_on"),
            },
            "not both",
        ),
        # the table has no field that the default incremental can start from
        ({"table": "cmn_schedule", "start_date": "2024-02-02"}, "start_date"),
        # the incremental has no cursor without its field
        ({"excluded_columns": ["sys_updated_on"]}, "not a field"),
        ({"table": "u_without_key"}, "no `sys_id`"),
        ({"api": "csv", "backend": "python"}, "pyarrow"),
        ({"api": "csv", "credentials": api_key}, "API keys"),
    ]
    for kwargs, match in cases:
        with pytest.raises(ValueError, match=match):
            servicenow_table(
                **{"credentials": credentials, "table": "incident", **kwargs}
            )
    with pytest.raises(ValueError, match="unknown tables"):
        servicenow_source(
            credentials=credentials, encoded_queries={"incidents": "active=true"}
        )

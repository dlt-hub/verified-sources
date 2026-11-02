import os
from typing import Optional

import dlt
import pytest

from sources.servicenow import DEFAULT_TABLES, servicenow_source
from sources.servicenow.helpers.credentials import BasicCredentials

from tests.servicenow.utils import FakeInstance, make_records, updated_on
from tests.utils import assert_load_info, load_table_counts


@pytest.mark.parametrize("backend", ["python", "pyarrow"])
def test_servicenow_source(
    instance: FakeInstance, credentials: BasicCredentials, backend: str
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
        backend=backend,
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

    with pytest.raises(ValueError, match="unknown tables"):
        servicenow_source(
            credentials=credentials, encoded_queries={"incidents": "active=true"}
        )


@pytest.mark.parametrize(
    "write_disposition", [None, "replace"], ids=["merge", "replace"]
)
def test_servicenow_source_start_date(
    instance: FakeInstance,
    credentials: BasicCredentials,
    write_disposition: Optional[str],
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
    source = servicenow_source(
        credentials=credentials, tables=tables, write_disposition=write_disposition
    )
    assert {resource.section for resource in source.resources.values()} == {
        "servicenow"
    }
    pipeline = dlt.pipeline(
        pipeline_name="servicenow_source_start", destination="duckdb", dev_mode=True
    )
    assert_load_info(pipeline.run(source))
    assert load_table_counts(pipeline, *tables) == {table: 6 for table in tables}

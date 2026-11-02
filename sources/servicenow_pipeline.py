#!/usr/bin/env python3
"""Pipeline to load ServiceNow data."""
import dlt
from servicenow import servicenow_source, servicenow_table


def load() -> None:
    """Loads the most common tables and merges the records changed since the last run"""
    pipeline = dlt.pipeline(
        pipeline_name="servicenow", destination="duckdb", dataset_name="servicenow_data"
    )
    load_info = pipeline.run(servicenow_source())
    print(load_info)


def load_selected() -> None:
    """Loads active incidents and changes, and all users"""
    pipeline = dlt.pipeline(
        pipeline_name="servicenow", destination="duckdb", dataset_name="servicenow_data"
    )
    source = servicenow_source(
        tables=["incident", "change_request", "sys_user"],
        encoded_queries={"incident": "active=true", "change_request": "active=true"},
    )
    load_info = pipeline.run(source)
    print(load_info)


def load_table() -> None:
    """Loads a single table, for example a custom table, with selected fields"""
    pipeline = dlt.pipeline(
        pipeline_name="servicenow", destination="duckdb", dataset_name="servicenow_data"
    )
    # the first run loads the records changed since `start_date`. Next runs merge the changes.
    # for a custom table, pass its name, for example `u_custom_table`. `sys_id` and the cursor
    # `sys_updated_on` are always loaded
    incident = servicenow_table(
        table="incident",
        start_date="2024-01-01",
        included_columns=[
            "number",
            "short_description",
            "state",
            "priority",
        ],
    )
    load_info = pipeline.run(incident)
    print(load_info)


def reload_table() -> None:
    """Reloads all records of a table, for example after its fields changed"""
    pipeline = dlt.pipeline(
        pipeline_name="servicenow", destination="duckdb", dataset_name="servicenow_data"
    )
    load_info = pipeline.run(
        servicenow_table(table="cmdb_ci", write_disposition="replace")
    )
    print(load_info)


if __name__ == "__main__":
    load()
    # load_selected()
    # load_table()
    # reload_table()

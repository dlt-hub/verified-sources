#!/usr/bin/env python3
"""Pipeline to load Salesforce data."""
import dlt
from salesforce import salesforce_source, salesforce_table


def load() -> None:
    """Load the most common sObjects, records changed since the last run are merged"""
    pipeline = dlt.pipeline(
        pipeline_name="salesforce", destination="duckdb", dataset_name="salesforce_data"
    )
    load_info = pipeline.run(salesforce_source())
    print(load_info)


def load_selected() -> None:
    """Reload all open opportunities and their accounts on every run"""
    pipeline = dlt.pipeline(
        pipeline_name="salesforce", destination="duckdb", dataset_name="salesforce_data"
    )
    source = salesforce_source(
        write_disposition="replace", soql_filters={"opportunity": "IsClosed = false"}
    ).with_resources("account", "opportunity")
    load_info = pipeline.run(source)
    print(load_info)


def load_table() -> None:
    """Load a single sObject, e.g. a custom object, with selected fields"""
    pipeline = dlt.pipeline(
        pipeline_name="salesforce", destination="duckdb", dataset_name="salesforce_data"
    )
    # merges records changed since the last run on `Id`, the first run loads records changed
    # since `start_date`. use `Invoice__c` for a custom object
    account = salesforce_table(
        sobject="Account",
        start_date="2024-01-01",
        included_columns=["Id", "Name", "Industry", "AnnualRevenue", "SystemModstamp"],
    )
    load_info = pipeline.run(account)
    print(load_info)


if __name__ == "__main__":
    load()
    # load_selected()
    # load_table()

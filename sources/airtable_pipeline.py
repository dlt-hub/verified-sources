import dlt
from airtable import airtable_source


def load_entire_base() -> None:
    """
    Loads all tables from the specified Airtable base.

    Note:
    - Locate the base ID starting with "app". For guidance, refer to https://support.airtable.com/docs/finding-airtable-ids.
    - The base_id can either be passed directly or set up in ".dlt/config.toml".
    """
    # configure the pipeline with your destination details
    pipeline = dlt.pipeline(
        pipeline_name="airtable", destination="duckdb", dataset_name="airtable_data"
    )

    # Retrieve data from Airtable using airtable_source.
    airtables = airtable_source(base_id="Please set me up!")

    # Explicitly define data types for columns to prevent type inference warnings.
    # Adjust as per your data.
    airtables.resources["Table1"].apply_hints(
        columns={"Field1": {"name": "Field1", "data_type": "text"}}
    )

    load_info = pipeline.run(airtables, write_disposition="replace")
    print(load_info)


def load_select_tables_from_base_by_id() -> None:
    """
    Load specific table IDs from Airtable to a data pipeline.

    Note:
    - Table IDs should start with "tbl".
    - Please refer to Airtable documentation for finding IDs:
      https://support.airtable.com/docs/finding-airtable-ids
    - Example in this Airtable URL: https://airtable.com/app7RlqvdoOmJm9XR/tblKHM5s3AujfSbAH
    - Table ID: "tblKHM5s3AujfSbAH"
    """

    # configure the pipeline with your destination details
    pipeline = dlt.pipeline(
        pipeline_name="airtable", destination="duckdb", dataset_name="airtable_data"
    )

    airtables = airtable_source(
        base_id="Please set me up!",
        table_names=["table1", "table2"],
    )

    load_info = pipeline.run(airtables, write_disposition="replace")
    print(load_info)


def load_select_tables_from_base_by_name() -> None:
    """
    Loads specific table names from an Airtable base.

    Note:
        Filtering by table names is less reliable than by IDs since names can be changed by Airtable users.
        Example Airtable: https://airtable.com/app7RlqvdoOmJm9XR/tblJCTXfjwOETmvy2/
    """
    pipeline = dlt.pipeline(
        pipeline_name="airtable", destination="duckdb", dataset_name="airtable_data"
    )

    airtables = airtable_source(
        base_id="Please set me up!",
        table_names=["table_name"],
    )

    airtables.resources["table_name"].apply_hints(
        primary_key="Field1",
        columns={"Field1": {"name": "Field1", "data_type": "integer"}},
    )
    load_info = pipeline.run(airtables, write_disposition="replace")
    print(load_info)


def load_and_customize_write_disposition() -> None:
    """
    Loads data from a specific Airtable base's table with customized write disposition("merge") using Field1.

    Note:
        Ensure the 'base_id' value in 'airtable_source' is properly set up before execution.

    """
    pipeline = dlt.pipeline(
        pipeline_name="airtable", destination="duckdb", dataset_name="airtable_data"
    )

    airtables = airtable_source(base_id="Please set me up!", table_names=["table_name"])
    airtables.resources["Sheet1"].apply_hints(
        primary_key="Field1",
        columns={"Field1": {"name": "Field1", "data_type": "text"}},
        write_disposition="merge",
    )
    load_info = pipeline.run(airtables)
    print(load_info)


if __name__ == "__main__":
    load_entire_base()
    # load_select_tables_from_base_by_id()
    # load_select_tables_from_base_by_name()
    # load_and_customize_write_disposition()

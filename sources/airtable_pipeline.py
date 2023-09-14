from typing import List

import dlt
from airtable import airtable_source


def load_entire_base(base_id: str) -> None:
    """
    Loads all tables from the specified Airtable base.

    Args:
        base_id (str): The id of the base. Obtain it, e.g. from the URL in your web browser.
            It starts with "app". See https://support.airtable.com/docs/finding-airtable-ids

    Note:
        - The base_id can either be passed directly or set up in ".dlt/config.toml".
    """
    # configure the pipeline with your destination details
    pipeline = dlt.pipeline(
        pipeline_name="airtable", destination="duckdb", dataset_name="airtable_data"
    )

    # Retrieve data from Airtable using airtable_source.
    airtables = airtable_source(base_id=base_id)

    load_info = pipeline.run(airtables, write_disposition="replace")
    print(load_info)


def load_select_tables_from_base_by_id(base_id: str, table_names: List[str]) -> None:
    """
    Load specific table IDs from Airtable to a data pipeline.

    Args:
        base_id (str): The id of the base. Obtain it, e.g. from the URL in your web browser.
            It starts with "app". See https://support.airtable.com/docs/finding-airtable-ids
        table_names (List[str]): A list of table IDs or table names to load. Unless specified otherwise,
            all tables in the schema are loaded.  Names are freely user-defined. IDs start with "tbl".
            See https://support.airtable.com/docs/finding-airtable-ids

    Note:
        - Filtering by names is less reliable than filtering on IDs because names can be changed by Airtable users.
        - Example in this Airtable URL: https://airtable.com/app7RlqvdoOmJm9XR/tblKHM5s3AujfSbAH
        - Table ID: "tblKHM5s3AujfSbAH"
        - The base_id and table_names can either be passed directly or set up in ".dlt/config.toml".
    """

    # configure the pipeline with your destination details
    pipeline = dlt.pipeline(
        pipeline_name="airtable", destination="duckdb", dataset_name="airtable_data"
    )

    airtables = airtable_source(
        base_id=base_id,
        table_names=table_names,
    )

    load_info = pipeline.run(airtables, write_disposition="replace")
    print(load_info)


def load_select_tables_from_base_by_name(
    base_id: str, table_names: List[str], resource_name: str, field_name: str
) -> None:
    """
    Loads specific table names from an Airtable base.

    Args:
        base_id (str): The id of the base. Obtain it, e.g. from the URL in your web browser.
            It starts with "app". See https://support.airtable.com/docs/finding-airtable-ids
        table_names (List[str]): A list of table IDs or table names to load. Unless specified otherwise,
            all tables in the schema are loaded.  Names are freely user-defined. IDs start with "tbl".
            See https://support.airtable.com/docs/finding-airtable-idss
        field_name (str): The name of the table field for which we want to apply hints.

    Note:
        - Filtering by names is less reliable than filtering on IDs because names can be changed by Airtable users.
        - Example in this Airtable URL: https://airtable.com/app7RlqvdoOmJm9XR/tblKHM5s3AujfSbAH
        - Table ID: "tblKHM5s3AujfSbAH"
        - The base_id and table_names can either be passed directly or set up in ".dlt/config.toml".
    """
    pipeline = dlt.pipeline(
        pipeline_name="airtable", destination="duckdb", dataset_name="airtable_data"
    )

    airtables = airtable_source(
        base_id=base_id,
        table_names=table_names,
    )

    airtables.resources[resource_name].apply_hints(
        primary_key=field_name,
        columns={field_name: {"data_type": "text"}},
    )
    load_info = pipeline.run(airtables, write_disposition="replace")
    print(load_info)


def load_and_customize_write_disposition(
    base_id: str, table_names: List[str], resource_name: str, field_name: str
) -> None:
    """
    Loads data from a specific Airtable base's table with customized write disposition("merge") using field_name.

    Args:
        base_id (str): The id of the base. Obtain it, e.g. from the URL in your web browser.
            It starts with "app". See https://support.airtable.com/docs/finding-airtable-ids
        table_names (List[str]): A list of table IDs or table names to load. Unless specified otherwise,
            all tables in the schema are loaded.  Names are freely user-defined. IDs start with "tbl".
            See https://support.airtable.com/docs/finding-airtable-ids
        field_name (str): The name of the table field for which we want to apply hints.

    Note:
        - Filtering by names is less reliable than filtering on IDs because names can be changed by Airtable users.
        - Example in this Airtable URL: https://airtable.com/app7RlqvdoOmJm9XR/tblKHM5s3AujfSbAH
        - Table ID: "tblKHM5s3AujfSbAH"
        - The base_id and table_names can either be passed directly or set up in ".dlt/config.toml".

    """
    pipeline = dlt.pipeline(
        pipeline_name="airtable", destination="duckdb", dataset_name="airtable_data"
    )

    airtables = airtable_source(
        base_id=base_id,
        table_names=table_names,
    )
    airtables.resources[resource_name].apply_hints(
        primary_key=field_name,
        columns={field_name: {"data_type": "text"}},
    )
    load_info = pipeline.run(airtables)
    print(load_info)


if __name__ == "__main__":
    base_id_example = "Please set me up!"
    table_names_example = ["Please set me up!"]
    resource_name_to_apply_hints = "Please set me up!"
    field_name_example = "Please set me up!"

    load_entire_base(base_id_example)
    load_select_tables_from_base_by_id(base_id_example, table_names_example)
    load_select_tables_from_base_by_name(
        base_id_example,
        table_names_example,
        resource_name_to_apply_hints,
        field_name_example,
    )
    load_and_customize_write_disposition(
        base_id_example,
        table_names_example,
        resource_name_to_apply_hints,
        field_name_example,
    )

import dlt
from airtable import airtable_source


def load_entire_base(pipeline: dlt.Pipeline) -> None:
    # Loads all tables inside a given base.
    # Find the base ID starting with "app". See https://support.airtable.com/docs/finding-airtable-ids
    all_event_planning_tables = airtable_source(base_id="app7RlqvdoOmJm9XR")

    # typing columns to silence warnings
    all_event_planning_tables.resources["📆 Schedule"].apply_hints(
        columns={"Activity": {"data_type": "text"}}
    )
    all_event_planning_tables.resources["🎤 Speakers"].apply_hints(
        columns={"Name": {"data_type": "text"}}
    )
    all_event_planning_tables.resources["🪑 Attendees"].apply_hints(
        columns={"Name": {"data_type": "text"}}
    )
    all_event_planning_tables.resources["💰 Budget"].apply_hints(
        columns={"Item": {"data_type": "text"}}
    )

    load_info = pipeline.run(all_event_planning_tables, write_disposition="replace")
    print(load_info)


def load_select_tables_from_base_by_id(pipeline: dlt.Pipeline) -> None:
    # Loads specific table IDs.
    # Starts with "tbl". See https://support.airtable.com/docs/finding-airtable-ids
    # See example: https://airtable.com/app7RlqvdoOmJm9XR/tblKHM5s3AujfSbAH
    airtables = airtable_source(
        base_id="app7RlqvdoOmJm9XR",
        table_names=["tblKHM5s3AujfSbAH", "tbloBrS8PnoO63aMP"],
    )

    load_info = pipeline.run(airtables, write_disposition="replace")
    print(load_info)


def load_select_tables_from_base_by_name(pipeline: dlt.Pipeline) -> None:
    # Loads specific table names.
    # Filtering by names is less reliable than filtering on IDs because names can be changed by Airtable users.
    # See example: https://airtable.com/app7RlqvdoOmJm9XR/tblJCTXfjwOETmvy2/
    event_base = airtable_source(
        base_id="app7RlqvdoOmJm9XR",
        table_names=["💰 Budget"],
    )
    event_base.resources["💰 Budget"].apply_hints(
        primary_key="Item", columns={"Item": {"data_type": "text"}}
    )
    load_info = pipeline.run(event_base, write_disposition="replace")
    print(load_info)


def load_and_customize_write_disposition(pipeline: dlt.Pipeline) -> None:
    questionnaire = airtable_source(
        base_id="appcChDyP0pZeC76v", table_names=["tbl1sN4CpPv8pBll4"]
    )
    questionnaire.resources["Sheet1"].apply_hints(
        primary_key="Name",
        columns={"Name": {"data_type": "text"}},
        write_disposition="merge",
    )
    load_info = pipeline.run(questionnaire)
    print(load_info)


if __name__ == "__main__":
    # configure the pipeline with your destination details
    pipeline = dlt.pipeline(
        pipeline_name="airtable", destination="duckdb", dataset_name="airtable_data"
    )

    load_entire_base(pipeline)
    load_select_tables_from_base_by_id(pipeline)
    load_select_tables_from_base_by_name(pipeline)
    load_and_customize_write_disposition(pipeline)
